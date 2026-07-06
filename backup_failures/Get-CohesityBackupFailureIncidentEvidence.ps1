<#
.SYNOPSIS
Cohesity backup failure incident evidence script.

.DESCRIPTION
Self-contained replacement workflow for backup_failures/Cohesity_Backup_Failures.
First run in the 18:00 ET compute window performs the full all-environment latest-uncleared-failure scan and stores that as the baseline in state.json.
Later runs in the same compute window check only the baseline failures for recovery.

Cohesity API calls are GET-only.
No Excel output.
No ServiceNow update.
Incident folder output files only:
- current_failures.csv
- recovered.csv
- new_failures.csv
- new_recoveries.csv
- worknotes.txt
- state.json
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
    [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = "",
    [switch]$ResetBaseline
)

$ErrorActionPreference = "Stop"

# -----------------------------
# Common helpers
# -----------------------------
function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Get-Value($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object -or [string]::IsNullOrWhiteSpace($Name)) { return $Default }
    $Property = $Object.PSObject.Properties[$Name]
    if ($Property) { return $Property.Value }
    return $Default
}

function As-List($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function Clean-Value($Value) {
    if ($null -eq $Value) { return "" }
    if ($Value -is [array]) { $Value = $Value -join " | " }
    return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim()
}

function Convert-ToUtcFromEpoch($Value) {
    if ($null -eq $Value -or [int64]$Value -eq 0) { return $null }
    try { return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Value / 1000)).UtcDateTime }
    catch { return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$Value).UtcDateTime }
}

function Convert-UsecsToEtText($Usecs) {
    $Utc = Convert-ToUtcFromEpoch $Usecs
    if ($null -eq $Utc) { return "" }
    return ([TimeZoneInfo]::ConvertTimeFromUtc($Utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Convert-EtTextToUsecs([string]$EtText) {
    if ([string]::IsNullOrWhiteSpace($EtText)) { return 0 }
    $EtDate = [datetime]$EtText
    $Utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtDate, [DateTimeKind]::Unspecified), $script:EtZone)
    return [int64](([DateTimeOffset]::new($Utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000)
}

function Read-JsonFile([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $Raw = Get-Content $Path -Raw
    if ([string]::IsNullOrWhiteSpace($Raw)) { return $null }
    return $Raw | ConvertFrom-Json
}

function Write-JsonFile($Object, [string]$Path) {
    $Folder = Split-Path $Path -Parent
    if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 100 | Set-Content $Path -Encoding UTF8
}

function Write-CsvFile($Rows, [string]$Path, [string[]]$Columns) {
    $Folder = Split-Path $Path -Parent
    if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }
    $List = @($Rows)
    if ($List.Count -eq 0) {
        ($Columns -join ",") | Set-Content $Path -Encoding UTF8
    } else {
        $List | Select-Object $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Query-String([hashtable]$Items) {
    return (($Items.GetEnumerator() | ForEach-Object {
        [uri]::EscapeDataString([string]$_.Key) + "=" + [uri]::EscapeDataString([string]$_.Value)
    }) -join "&")
}

function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $Response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
    } else {
        $Response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get
    }
    if (-not $Response -or [string]::IsNullOrWhiteSpace($Response.Content)) { return $null }
    return $Response.Content | ConvertFrom-Json
}

function Get-FirstLocalBackupInfo($Run) {
    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    return @(($Run.localBackupInfo))[0]
}

function Get-RunInfoByType($Run, [string]$RunType) {
    return As-List (Get-Value $Run "localBackupInfo" @()) |
        Where-Object { (Clean-Value (Get-Value $_ "runType" "")) -eq $RunType } |
        Select-Object -First 1
}

function Has-FailedAttempts($RunObject) {
    try {
        $Attempts = $RunObject.localSnapshotInfo.failedAttempts
        return ($Attempts -and $Attempts.Count -gt 0)
    } catch {
        return $false
    }
}

function Is-SuccessForClear($RunObject) {
    if ($null -eq $RunObject -or $null -eq $RunObject.localSnapshotInfo) { return $false }
    return (-not (Has-FailedAttempts $RunObject))
}

function Get-FailedAttempts($RunObject) {
    try { return @(Get-Value $RunObject.localSnapshotInfo "failedAttempts" @()) }
    catch { return @() }
}

function Combine-FailedAttempts($Attempts) {
    if (-not $Attempts) { return "" }
    $Messages = @()
    foreach ($Attempt in @($Attempts)) {
        $Message = Clean-Value (Get-Value $Attempt "message" "")
        if ($Message) { $Messages += $Message }
    }
    return ($Messages -join " | ")
}

function Normalize-Message($Message) {
    return Clean-Value $Message
}

function Is-FailedStatus([string]$Status) {
    return ($Status -in @("Failed", "kFailed"))
}

function Is-SuccessStatus([string]$Status) {
    return ($Status -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning"))
}

# -----------------------------
# Compute window / incident lock
# -----------------------------
function Get-WindowNow {
    $NowEt = [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
    if ($NowEt.Hour -lt 18) { $Start = $NowEt.Date.AddDays(-1).AddHours(18) } else { $Start = $NowEt.Date.AddHours(18) }
    $End = $Start.AddDays(1)
    $StartDate = $Start.ToString("yyyy-MM-dd")
    $EndDate = $End.ToString("yyyy-MM-dd")
    return [pscustomobject]@{
        Key = "${StartDate}_1800ET"
        Label = "$StartDate 18:00 ET -> $EndDate 18:00 ET"
        StartUsecs = Convert-EtTextToUsecs ($Start.ToString("yyyy-MM-dd HH:mm:ss"))
        EndUsecs = Convert-EtTextToUsecs ($End.ToString("yyyy-MM-dd HH:mm:ss"))
        GeneratedET = ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
    }
}

function Resolve-Incident($Window) {
    if (!(Test-Path $OutputRoot)) { New-Item $OutputRoot -ItemType Directory -Force | Out-Null }
    $RegistryPath = Join-Path $OutputRoot "BackupFailure_IncidentRegistry.json"
    $Registry = Read-JsonFile $RegistryPath
    if (!$Registry) { $Registry = [pscustomobject]@{ WindowSource = "backup_failures/compute_window.js"; Windows = [pscustomobject]@{} } }
    $Existing = $Registry.Windows.PSObject.Properties[$Window.Key]
    if ($Existing) { return $Existing.Value }

    $Incident = $IncidentNumber
    if (!$Incident) { $Incident = Read-Host "Enter incident number for this backup-failure window" }
    $Incident = $Incident.Trim().ToUpper()
    if ($Incident -notmatch '^INC[0-9A-Z]+$') { throw "Invalid incident number: $Incident" }

    $Entry = [pscustomobject]@{
        IncidentNumber = $Incident
        WindowKey = $Window.Key
        WindowLabel = $Window.Label
        FirstRunET = $Window.GeneratedET
        LastRunET = $Window.GeneratedET
        OutputFolder = (Join-Path $OutputRoot $Incident)
    }
    $Registry.Windows | Add-Member NoteProperty $Window.Key $Entry -Force
    Write-JsonFile $Registry $RegistryPath
    return $Entry
}

# -----------------------------
# Environment map and object helpers
# -----------------------------
function Get-EnvironmentMap {
    return @(
        [pscustomobject]@{ Label = "Oracle";        Filter = "kOracle";        TargetObjectType = "kDatabase";       ParentHostNeeded = $true;  RunLimit = $NumRuns },
        [pscustomobject]@{ Label = "SQL";           Filter = "kSQL";           TargetObjectType = "kDatabase";       ParentHostNeeded = $true;  RunLimit = $NumRuns },
        [pscustomobject]@{ Label = "Physical";      Filter = "kPhysical";      TargetObjectType = "kHost";           ParentHostNeeded = $false; RunLimit = $NumRuns },
        [pscustomobject]@{ Label = "GenericNas";    Filter = "kGenericNas";    TargetObjectType = "kHost";           ParentHostNeeded = $false; RunLimit = $NumRuns },
        [pscustomobject]@{ Label = "HyperV";        Filter = "kHyperV";        TargetObjectType = "kVirtualMachine"; ParentHostNeeded = $false; RunLimit = $NumRuns },
        [pscustomobject]@{ Label = "Acropolis";     Filter = "kAcropolis";     TargetObjectType = "kVirtualMachine"; ParentHostNeeded = $false; RunLimit = $NumRuns },
        [pscustomobject]@{ Label = "RemoteAdapter"; Filter = "kRemoteAdapter"; TargetObjectType = "kRemoteAdapter";  ParentHostNeeded = $false; RunLimit = 10 },
        [pscustomobject]@{ Label = "Isilon";        Filter = "kIsilon";        TargetObjectType = "kHost";           ParentHostNeeded = $false; RunLimit = $NumRuns }
    )
}

function Get-ClusterDisplayName($Cluster) {
    $Name = Clean-Value (Get-Value $Cluster "name" "")
    if (!$Name) { $Name = Clean-Value (Get-Value $Cluster "clusterName" "") }
    if (!$Name) { $Name = Clean-Value (Get-Value $Cluster "displayName" "") }
    if (!$Name) { $Name = "Unknown-$([string](Get-Value $Cluster 'clusterId' ''))" }
    return $Name
}

function Get-BaseObjectKey($RunObject) {
    if ($null -eq $RunObject -or $null -eq $RunObject.object) { return "" }
    $Object = $RunObject.object
    $ObjectId = ""
    if ($Object.id) { $ObjectId = [string]$Object.id }
    $SourceId = ""
    if ($Object.PSObject.Properties["sourceId"]) { $SourceId = [string]$Object.sourceId }
    return "$($Object.environment)|$($Object.objectType)|$($Object.name)|$ObjectId|$SourceId"
}

function Get-StateObjectKey($RunObject, [string]$ClusterId, [string]$ProtectionGroupId) {
    $BaseKey = Get-BaseObjectKey $RunObject
    if (!$BaseKey) { return "" }
    return "$ClusterId|$ProtectionGroupId|$BaseKey"
}

function Build-ObjectNameMaps($Runs, [bool]$ParentHostNeeded) {
    $IdToName = @{}
    $SourceHostById = @{}
    if (!$ParentHostNeeded) { return [pscustomobject]@{ IdToName = $IdToName; SourceHostById = $SourceHostById } }

    foreach ($Run in $Runs) {
        foreach ($RunObject in (As-List (Get-Value $Run "objects" @()))) {
            $Object = Get-Value $RunObject "object" $null
            if ($null -eq $Object -or -not $Object.id) { continue }
            $ObjectId = [string]$Object.id
            if (-not $IdToName.ContainsKey($ObjectId) -and $Object.name) { $IdToName[$ObjectId] = $Object.name }
            if (($Object.objectType -eq "kHost" -or $Object.environment -eq "kPhysical") -and $Object.name) { $SourceHostById[$ObjectId] = $Object.name }
        }
    }
    return [pscustomobject]@{ IdToName = $IdToName; SourceHostById = $SourceHostById }
}

function Resolve-ParentHostName($RunObject, [hashtable]$IdToName, [hashtable]$SourceHostById) {
    $Object = Get-Value $RunObject "object" $null
    if ($null -eq $Object) { return "" }
    $SourceId = ""
    if ($Object.PSObject.Properties["sourceId"]) { $SourceId = [string]$Object.sourceId }
    if ($SourceId -and $IdToName.ContainsKey($SourceId)) { return [string]$IdToName[$SourceId] }
    if ($SourceId -and $SourceHostById.ContainsKey($SourceId)) { return [string]$SourceHostById[$SourceId] }
    return ""
}

function New-IncidentFailureRow {
    param(
        [string]$Incident,
        $Window,
        $Cluster,
        $EnvironmentInfo,
        $ProtectionGroup,
        [string]$ObjectKey,
        [string]$SourceHostName,
        [string]$ObjectName,
        [string]$ObjectType,
        [string]$RunType,
        [int64]$StartUsecs,
        [int64]$EndUsecs,
        [string]$Message
    )

    $ClusterId = [string](Get-Value $Cluster "clusterId" "")
    $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
    $EffectiveUsecs = if ($EndUsecs -gt 0) { $EndUsecs } else { $StartUsecs }
    return [pscustomobject]@{
        IncidentNumber = $Incident
        WindowKey = $Window.Key
        Status = "StillFailing"
        Cluster = Get-ClusterDisplayName $Cluster
        Environment = $EnvironmentInfo.Label
        ProtectionGroup = Clean-Value (Get-Value $ProtectionGroup "name" "")
        SourceHostName = Clean-Value $SourceHostName
        ObjectName = Clean-Value $ObjectName
        ObjectType = Clean-Value $ObjectType
        RunType = Clean-Value $RunType
        FirstFailedET = Convert-UsecsToEtText $EffectiveUsecs
        LastFailedET = Convert-UsecsToEtText $EffectiveUsecs
        LastFailedUsecs = $EffectiveUsecs
        RecoveredET = ""
        ConsecutiveFailureCount = 1
        Message = Clean-Value $Message
        ObjectKey = $ObjectKey
        ClusterId = $ClusterId
        ProtectionGroupId = $ProtectionGroupId
        EnvironmentFilter = $EnvironmentInfo.Filter
    }
}

function New-RunLevelFailureRow($Incident, $Window, $Cluster, $EnvironmentInfo, $ProtectionGroup, $Info, [string]$Message) {
    $ClusterId = [string](Get-Value $Cluster "clusterId" "")
    $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
    $ProtectionGroupName = Clean-Value (Get-Value $ProtectionGroup "name" "")
    $RunType = Clean-Value (Get-Value $Info "runType" "")
    $ObjectKey = "$ClusterId|$ProtectionGroupId|RUNLEVEL|$RunType|$ProtectionGroupName"
    return New-IncidentFailureRow -Incident $Incident -Window $Window -Cluster $Cluster -EnvironmentInfo $EnvironmentInfo -ProtectionGroup $ProtectionGroup -ObjectKey $ObjectKey -SourceHostName "" -ObjectName $ProtectionGroupName -ObjectType "ProtectionGroup" -RunType $RunType -StartUsecs ([int64](Get-Value $Info "startTimeUsecs" 0)) -EndUsecs ([int64](Get-Value $Info "endTimeUsecs" 0)) -Message $Message
}

# -----------------------------
# Proven collector port: normal environments
# -----------------------------
function Collect-EnvironmentLatestUnclearedFailures {
    param(
        $Incident,
        $Window,
        $Clusters,
        [string]$ApiKey,
        $EnvironmentInfo
    )

    $GlobalRows = @()
    $FilterSet = $EnvironmentInfo.Filter.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $IsNasClass = ($EnvironmentInfo.Label -in @("GenericNas", "Isilon"))

    foreach ($Cluster in $Clusters) {
        $ClusterId = [string](Get-Value $Cluster "clusterId" "")
        $Headers = @{ apiKey = $ApiKey; accessClusterId = $ClusterId; accept = "application/json" }

        $ProtectionGroups = @()
        foreach ($FilterEnvironment in $FilterSet) {
            try {
                $PgUri = "$BaseUrl/v2/data-protect/protection-groups?environments=$FilterEnvironment&isDeleted=false&isPaused=false&isActive=true"
                $PgJson = Invoke-HeliosGetJson -Uri $PgUri -Headers $Headers
                if ($PgJson -and $PgJson.protectionGroups) { $ProtectionGroups += @($PgJson.protectionGroups) }
            } catch {
                continue
            }
        }

        $ProtectionGroups = @($ProtectionGroups | Sort-Object id -Unique)
        if ($ProtectionGroups.Count -eq 0) { continue }

        foreach ($ProtectionGroup in $ProtectionGroups) {
            $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
            try {
                $RunsUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=$($EnvironmentInfo.RunLimit)&excludeNonRestorableRuns=false&includeObjectDetails=true"
                $RunsJson = Invoke-HeliosGetJson -Uri $RunsUri -Headers $Headers
            } catch {
                continue
            }

            if (-not $RunsJson -or -not $RunsJson.runs) { continue }
            $Runs = @($RunsJson.runs)
            if ($Runs.Count -eq 0) { continue }

            $RunTypes = @(
                $Runs |
                    ForEach-Object {
                        $Info = Get-FirstLocalBackupInfo $_
                        if ($Info) { Clean-Value (Get-Value $Info "runType" "") }
                    } |
                    Where-Object { $_ } |
                    Select-Object -Unique
            )

            foreach ($RunType in $RunTypes) {
                $RunsForType = @(
                    $Runs |
                        Where-Object {
                            $Info = Get-FirstLocalBackupInfo $_
                            $Info -and (Clean-Value (Get-Value $Info "runType" "")) -eq $RunType
                        } |
                        Sort-Object {
                            $Info = Get-FirstLocalBackupInfo $_
                            [int64](Get-Value $Info "endTimeUsecs" 0)
                        } -Descending
                )

                if ($RunsForType.Count -eq 0) { continue }

                $NameMaps = Build-ObjectNameMaps $RunsForType $EnvironmentInfo.ParentHostNeeded
                $IdToName = $NameMaps.IdToName
                $SourceHostById = $NameMaps.SourceHostById
                $Cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                $LatestFailByKey = @{}
                $RunLevelCleared = $false

                foreach ($Run in $RunsForType) {
                    $Info = Get-FirstLocalBackupInfo $Run
                    if (!$Info) { continue }

                    $Status = [string](Get-Value $Info "status" "")
                    $StartUsecs = [int64](Get-Value $Info "startTimeUsecs" 0)
                    $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
                    $RunObjects = As-List (Get-Value $Run "objects" @())

                    if ($IsNasClass -and (Is-SuccessStatus $Status)) { $RunLevelCleared = $true }

                    if ($RunObjects.Count -eq 0) {
                        if ($IsNasClass -and (Is-FailedStatus $Status) -and -not $RunLevelCleared -and $LatestFailByKey.Count -eq 0) {
                            $Message = Normalize-Message (Get-Value $Info "messages" "")
                            if (!$Message) { $Message = "$($EnvironmentInfo.Label) run failed - no object-level details returned" }
                            $Row = New-RunLevelFailureRow $Incident $Window $Cluster $EnvironmentInfo $ProtectionGroup $Info $Message
                            if (-not $LatestFailByKey.ContainsKey($Row.ObjectKey)) { $LatestFailByKey[$Row.ObjectKey] = $Row }
                        }
                        continue
                    }

                    $ObjectsAll = @($RunObjects | Where-Object { $_ -and $_.object -and $_.localSnapshotInfo })

                    foreach ($RunObject in $ObjectsAll) {
                        if (Is-SuccessForClear $RunObject) {
                            $ClearKey = Get-StateObjectKey $RunObject $ClusterId $ProtectionGroupId
                            if ($ClearKey) { [void]$Cleared.Add($ClearKey) }
                        }
                    }

                    if (!(Is-FailedStatus $Status)) { continue }

                    if ($EnvironmentInfo.ParentHostNeeded) {
                        $HostObjects = @(
                            $ObjectsAll |
                                Where-Object {
                                    $_.object.objectType -eq "kHost" -or $_.object.environment -eq "kPhysical"
                                }
                        )

                        foreach ($HostObject in $HostObjects) {
                            $HostAttempts = Get-FailedAttempts $HostObject
                            if ($HostAttempts.Count -eq 0) { continue }
                            $HostKey = Get-StateObjectKey $HostObject $ClusterId $ProtectionGroupId
                            if (!$HostKey -or $Cleared.Contains($HostKey) -or $LatestFailByKey.ContainsKey($HostKey)) { continue }
                            $Message = Combine-FailedAttempts $HostAttempts
                            if (!$Message) { continue }

                            $Row = New-IncidentFailureRow -Incident $Incident -Window $Window -Cluster $Cluster -EnvironmentInfo $EnvironmentInfo -ProtectionGroup $ProtectionGroup -ObjectKey $HostKey -SourceHostName (Clean-Value $HostObject.object.name) -ObjectName "No DBs Found (Host-Level Failure)" -ObjectType "kHost" -RunType $RunType -StartUsecs $StartUsecs -EndUsecs $EndUsecs -Message $Message
                            $LatestFailByKey[$HostKey] = $Row
                        }
                    }

                    if ($IsNasClass) {
                        $TargetObjects = @(
                            $ObjectsAll |
                                Where-Object {
                                    $_.localSnapshotInfo.failedAttempts -and $_.localSnapshotInfo.failedAttempts.Count -gt 0
                                }
                        )
                    } else {
                        $TargetObjects = @(
                            $ObjectsAll |
                                Where-Object {
                                    $_.object.objectType -eq $EnvironmentInfo.TargetObjectType -and
                                    (
                                        -not $_.object.environment -or
                                        ($FilterSet -contains [string]$_.object.environment)
                                    )
                                }
                        )
                    }

                    foreach ($TargetObject in $TargetObjects) {
                        $ObjectKey = Get-StateObjectKey $TargetObject $ClusterId $ProtectionGroupId
                        if (!$ObjectKey -or $Cleared.Contains($ObjectKey) -or $LatestFailByKey.ContainsKey($ObjectKey)) { continue }

                        $Attempts = Get-FailedAttempts $TargetObject
                        if ($Attempts.Count -eq 0) {
                            if ($EnvironmentInfo.Label -eq "Physical" -and (Is-FailedStatus $Status)) {
                                $ObjectName = Clean-Value (Get-Value $TargetObject.object "name" "")
                                if (!$ObjectName) { $ObjectName = Clean-Value (Get-Value $ProtectionGroup "name" "") }
                                $Row = New-IncidentFailureRow -Incident $Incident -Window $Window -Cluster $Cluster -EnvironmentInfo $EnvironmentInfo -ProtectionGroup $ProtectionGroup -ObjectKey $ObjectKey -SourceHostName "" -ObjectName $ObjectName -ObjectType (Clean-Value (Get-Value $TargetObject.object "objectType" "")) -RunType $RunType -StartUsecs $StartUsecs -EndUsecs $EndUsecs -Message "No failedAttempts[] details found - Run marked Failed"
                                $LatestFailByKey[$ObjectKey] = $Row
                            }
                            continue
                        }

                        $Message = Combine-FailedAttempts $Attempts
                        if (!$Message) { continue }

                        if ($EnvironmentInfo.ParentHostNeeded) {
                            $SourceHostName = Resolve-ParentHostName $TargetObject $IdToName $SourceHostById
                            $ObjectName = Clean-Value (Get-Value $TargetObject.object "name" "")
                            $Row = New-IncidentFailureRow -Incident $Incident -Window $Window -Cluster $Cluster -EnvironmentInfo $EnvironmentInfo -ProtectionGroup $ProtectionGroup -ObjectKey $ObjectKey -SourceHostName $SourceHostName -ObjectName $ObjectName -ObjectType (Clean-Value (Get-Value $TargetObject.object "objectType" "")) -RunType $RunType -StartUsecs $StartUsecs -EndUsecs $EndUsecs -Message $Message
                            $LatestFailByKey[$ObjectKey] = $Row
                        } else {
                            $ObjectName = Clean-Value (Get-Value $TargetObject.object "name" "")
                            if (!$ObjectName) { $ObjectName = Clean-Value (Get-Value $ProtectionGroup "name" "") }
                            $Row = New-IncidentFailureRow -Incident $Incident -Window $Window -Cluster $Cluster -EnvironmentInfo $EnvironmentInfo -ProtectionGroup $ProtectionGroup -ObjectKey $ObjectKey -SourceHostName "" -ObjectName $ObjectName -ObjectType (Clean-Value (Get-Value $TargetObject.object "objectType" "")) -RunType $RunType -StartUsecs $StartUsecs -EndUsecs $EndUsecs -Message $Message
                            $LatestFailByKey[$ObjectKey] = $Row
                        }
                    }

                    if ($IsNasClass -and (Is-FailedStatus $Status) -and -not $RunLevelCleared -and $LatestFailByKey.Count -eq 0) {
                        $Message = Normalize-Message (Get-Value $Info "messages" "")
                        if (!$Message) { $Message = "$($EnvironmentInfo.Label) run failed - no object-level failedAttempts[] returned" }
                        $Row = New-RunLevelFailureRow $Incident $Window $Cluster $EnvironmentInfo $ProtectionGroup $Info $Message
                        if (-not $LatestFailByKey.ContainsKey($Row.ObjectKey)) { $LatestFailByKey[$Row.ObjectKey] = $Row }
                    }
                }

                foreach ($Key in $LatestFailByKey.Keys) { $GlobalRows += $LatestFailByKey[$Key] }
            }
        }
    }

    return @($GlobalRows)
}

# -----------------------------
# Proven collector port: RemoteAdapter
# -----------------------------
function Collect-RemoteAdapterLatestUnclearedFailures {
    param($Incident, $Window, $Clusters, [string]$ApiKey, $EnvironmentInfo)

    $Rows = @()
    foreach ($Cluster in $Clusters) {
        $ClusterId = [string](Get-Value $Cluster "clusterId" "")
        $Headers = @{ apiKey = $ApiKey; accessClusterId = $ClusterId; accept = "application/json" }
        try {
            $PgUri = "$BaseUrl/v2/data-protect/protection-groups?environments=kRemoteAdapter&isDeleted=false&isPaused=false&isActive=true"
            $PgJson = Invoke-HeliosGetJson -Uri $PgUri -Headers $Headers
            $ProtectionGroups = @($PgJson.protectionGroups)
        } catch { continue }
        if ($ProtectionGroups.Count -eq 0) { continue }

        foreach ($ProtectionGroup in $ProtectionGroups) {
            $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
            $ProtectionGroupName = Clean-Value (Get-Value $ProtectionGroup "name" "")
            $RaHostName = ""
            $RaDatabaseName = ""

            try {
                $RaHostName = Get-Value $ProtectionGroup.remoteAdapterParams.hosts "hostname" ""
                if ($RaHostName -is [array]) { $RaHostName = ($RaHostName -join ",") }
                $ScriptArgs = Get-Value $ProtectionGroup.remoteAdapterParams.hosts.incrementalBackupScript "params" ""
                if ($ScriptArgs -is [array]) { $ScriptArgs = ($ScriptArgs -join " ") }
                if ($ScriptArgs -match "-o\s+(\S+)") { $RaDatabaseName = $matches[1] }
            } catch {}

            try {
                $RunsUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=$($EnvironmentInfo.RunLimit)&excludeNonRestorableRuns=false&includeObjectDetails=true"
                $RunsJson = Invoke-HeliosGetJson -Uri $RunsUri -Headers $Headers
                $Runs = @($RunsJson.runs)
            } catch { continue }
            if ($Runs.Count -eq 0) { continue }

            $Flat = @()
            foreach ($Run in $Runs) {
                foreach ($Info in (As-List (Get-Value $Run "localBackupInfo" @()))) {
                    $Flat += [pscustomobject]@{
                        RunType = Clean-Value (Get-Value $Info "runType" "")
                        Status = [string](Get-Value $Info "status" "")
                        Message = Get-Value $Info "messages" ""
                        StartTimeUsecs = [int64](Get-Value $Info "startTimeUsecs" 0)
                        EndTimeUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
                    }
                }
            }
            if ($Flat.Count -eq 0) { continue }

            foreach ($Group in ($Flat | Where-Object { $_.RunType } | Group-Object RunType)) {
                $LatestFailed = $Group.Group |
                    Where-Object { Is-FailedStatus $_.Status } |
                    Sort-Object EndTimeUsecs -Descending |
                    Select-Object -First 1
                if ($null -eq $LatestFailed) { continue }

                $HasLaterSuccess = $Group.Group |
                    Where-Object { (Is-SuccessStatus $_.Status) -and $_.StartTimeUsecs -gt $LatestFailed.EndTimeUsecs } |
                    Select-Object -First 1
                if ($HasLaterSuccess) { continue }

                $ObjectName = if ($RaDatabaseName) { $RaDatabaseName } else { $RaHostName }
                if (!$ObjectName) { $ObjectName = $ProtectionGroupName }
                $ObjectKey = "$ClusterId|$ProtectionGroupId|RemoteAdapter|$($LatestFailed.RunType)|$ObjectName"
                $Row = New-IncidentFailureRow -Incident $Incident -Window $Window -Cluster $Cluster -EnvironmentInfo $EnvironmentInfo -ProtectionGroup $ProtectionGroup -ObjectKey $ObjectKey -SourceHostName "" -ObjectName $ObjectName -ObjectType "kRemoteAdapter" -RunType $LatestFailed.RunType -StartUsecs $LatestFailed.StartTimeUsecs -EndUsecs $LatestFailed.EndTimeUsecs -Message (Normalize-Message $LatestFailed.Message)
                $Rows += $Row
            }
        }
    }

    return @($Rows)
}

function Collect-FullBaseline {
    param($Incident, $Window, $Clusters, [string]$ApiKey)

    $AllRows = @()
    foreach ($EnvironmentInfo in (Get-EnvironmentMap)) {
        if ($EnvironmentInfo.Label -eq "RemoteAdapter") {
            $AllRows += Collect-RemoteAdapterLatestUnclearedFailures -Incident $Incident -Window $Window -Clusters $Clusters -ApiKey $ApiKey -EnvironmentInfo $EnvironmentInfo
        } else {
            $AllRows += Collect-EnvironmentLatestUnclearedFailures -Incident $Incident -Window $Window -Clusters $Clusters -ApiKey $ApiKey -EnvironmentInfo $EnvironmentInfo
        }
    }

    return @(
        $AllRows |
            Where-Object { $_ } |
            Group-Object ObjectKey |
            ForEach-Object { $_.Group | Sort-Object LastFailedUsecs -Descending | Select-Object -First 1 } |
            Sort-Object Cluster, ProtectionGroup, Environment, LastFailedUsecs -Descending
    )
}

# -----------------------------
# Targeted recovery checks
# -----------------------------
function Test-RemoteAdapterRecovered($BaselineRow, $Runs) {
    $Flat = @()
    foreach ($Run in $Runs) {
        foreach ($Info in (As-List (Get-Value $Run "localBackupInfo" @()))) {
            $Flat += [pscustomobject]@{
                RunType = Clean-Value (Get-Value $Info "runType" "")
                Status = [string](Get-Value $Info "status" "")
                StartTimeUsecs = [int64](Get-Value $Info "startTimeUsecs" 0)
                EndTimeUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
            }
        }
    }

    $Success = $Flat |
        Where-Object { $_.RunType -eq $BaselineRow.RunType -and (Is-SuccessStatus $_.Status) -and $_.StartTimeUsecs -gt [int64]$BaselineRow.LastFailedUsecs } |
        Sort-Object EndTimeUsecs -Descending |
        Select-Object -First 1
    if ($Success) { return Convert-UsecsToEtText $Success.EndTimeUsecs }
    return ""
}

function Test-BaselineRecovered($BaselineRow, $Runs) {
    if ($BaselineRow.Environment -eq "RemoteAdapter") { return Test-RemoteAdapterRecovered $BaselineRow $Runs }

    foreach ($Run in ($Runs | Sort-Object { $Info = Get-FirstLocalBackupInfo $_; [int64](Get-Value $Info "endTimeUsecs" 0) } -Descending)) {
        $Info = Get-RunInfoByType $Run $BaselineRow.RunType
        if (!$Info) { continue }
        if (!(Is-SuccessStatus ([string](Get-Value $Info "status" "")))) { continue }
        $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
        if ($EndUsecs -le [int64]$BaselineRow.LastFailedUsecs) { continue }

        $RunObjects = As-List (Get-Value $Run "objects" @())
        if ($BaselineRow.ObjectType -eq "ProtectionGroup" -and $RunObjects.Count -eq 0) { return Convert-UsecsToEtText $EndUsecs }
        if ($BaselineRow.ObjectType -eq "ProtectionGroup" -and $RunObjects.Count -gt 0) { return Convert-UsecsToEtText $EndUsecs }

        foreach ($RunObject in $RunObjects) {
            if (!(Is-SuccessForClear $RunObject)) { continue }
            $CandidateKey = Get-StateObjectKey $RunObject ([string]$BaselineRow.ClusterId) ([string]$BaselineRow.ProtectionGroupId)
            if ($CandidateKey -eq $BaselineRow.ObjectKey) { return Convert-UsecsToEtText $EndUsecs }
        }
    }
    return ""
}

function Collect-TargetedFollowUp($BaselineRows, $Clusters, [string]$ApiKey) {
    $CurrentRows = @()
    $RecoveredRows = @()
    $Warnings = @()

    $ClusterById = @{}
    foreach ($Cluster in $Clusters) {
        $ClusterId = [string](Get-Value $Cluster "clusterId" "")
        if ($ClusterId) { $ClusterById[$ClusterId] = $Cluster }
    }

    foreach ($Group in (@($BaselineRows) | Group-Object ClusterId, ProtectionGroupId)) {
        $Sample = $Group.Group[0]
        $ClusterId = [string]$Sample.ClusterId
        $ProtectionGroupId = [string]$Sample.ProtectionGroupId
        $Cluster = $ClusterById[$ClusterId]
        if (!$Cluster) {
            $Warnings += "Cluster not found: $($Sample.Cluster)"
            $CurrentRows += $Group.Group
            continue
        }

        $Headers = @{ apiKey = $ApiKey; accessClusterId = $ClusterId; accept = "application/json" }
        $RunLimit = if ($Sample.Environment -eq "RemoteAdapter") { 10 } else { $NumRuns }
        try {
            $RunsUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=$RunLimit&excludeNonRestorableRuns=false&includeObjectDetails=true"
            $RunsJson = Invoke-HeliosGetJson -Uri $RunsUri -Headers $Headers
            $Runs = @($RunsJson.runs)
        } catch {
            $Warnings += "Runs lookup failed for $($Sample.Cluster)/$($Sample.ProtectionGroup): $($_.Exception.Message)"
            $CurrentRows += $Group.Group
            continue
        }

        foreach ($BaselineRow in $Group.Group) {
            $RecoveredET = Test-BaselineRecovered $BaselineRow $Runs
            if ($RecoveredET) {
                $RecoveredRows += [pscustomobject]@{
                    IncidentNumber = $BaselineRow.IncidentNumber
                    WindowKey = $BaselineRow.WindowKey
                    Status = "Recovered"
                    Cluster = $BaselineRow.Cluster
                    Environment = $BaselineRow.Environment
                    ProtectionGroup = $BaselineRow.ProtectionGroup
                    SourceHostName = $BaselineRow.SourceHostName
                    ObjectName = $BaselineRow.ObjectName
                    ObjectType = $BaselineRow.ObjectType
                    RunType = $BaselineRow.RunType
                    FirstFailedET = $BaselineRow.FirstFailedET
                    LastFailedET = $BaselineRow.LastFailedET
                    LastFailedUsecs = $BaselineRow.LastFailedUsecs
                    RecoveredET = $RecoveredET
                    ConsecutiveFailureCount = 0
                    Message = $BaselineRow.Message
                    ObjectKey = $BaselineRow.ObjectKey
                    ClusterId = $BaselineRow.ClusterId
                    ProtectionGroupId = $BaselineRow.ProtectionGroupId
                    EnvironmentFilter = $BaselineRow.EnvironmentFilter
                }
            } else {
                $CurrentRows += $BaselineRow
            }
        }
    }

    return [pscustomobject]@{ Current = @($CurrentRows); Recovered = @($RecoveredRows); Warnings = @($Warnings) }
}

function Test-StateUsable($State) {
    if (!$State) { return $false }
    $Baseline = As-List (Get-Value $State "BaselineFailures" @())
    if ($Baseline.Count -eq 0) { return $false }
    $First = $Baseline[0]
    if (!(Get-Value $First "ObjectKey" "")) { return $false }
    if (!(Get-Value $First "ClusterId" "")) { return $false }
    if (!(Get-Value $First "ProtectionGroupId" "")) { return $false }
    return $true
}

# -----------------------------
# Main
# -----------------------------
if (!(Test-Path $HelperPath)) { throw "Missing API key helper: $HelperPath" }
if (!(Test-Path $EncryptedFile)) { throw "Missing encrypted key file: $EncryptedFile" }
. $HelperPath
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
if (!$ApiKey) { throw "API key is blank" }

$Window = Get-WindowNow
$IncidentLock = Resolve-Incident $Window
$Incident = $IncidentLock.IncidentNumber
$OutputFolder = $IncidentLock.OutputFolder
if (!(Test-Path $OutputFolder)) { New-Item $OutputFolder -ItemType Directory -Force | Out-Null }
$StatePath = Join-Path $OutputFolder "state.json"
$PreviousState = Read-JsonFile $StatePath

$CommonHeaders = @{ apiKey = $ApiKey; accept = "application/json" }
try {
    $ClusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers $CommonHeaders
    $Clusters = @($ClusterJson.cohesityClusters)
} catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}
if ($Clusters.Count -eq 0) { throw "No clusters returned from Helios." }

if ($ResetBaseline -or !(Test-StateUsable $PreviousState)) {
    $Mode = "FullBaseline"
    $BaselineFailures = @(Collect-FullBaseline -Incident $Incident -Window $Window -Clusters $Clusters -ApiKey $ApiKey)
    $CurrentFailures = @($BaselineFailures)
    $Recovered = @()
    $NewFailures = @($BaselineFailures)
    $NewRecoveries = @()
    $Warnings = @()
} else {
    $Mode = "TargetedFollowUp"
    $BaselineFailures = @(As-List (Get-Value $PreviousState "BaselineFailures" @()))
    $PreviouslyRecovered = @{}
    foreach ($Item in (As-List (Get-Value $PreviousState "Recovered" @()))) {
        $Key = [string](Get-Value $Item "ObjectKey" "")
        if ($Key) { $PreviouslyRecovered[$Key] = $true }
    }
    $FollowUp = Collect-TargetedFollowUp -BaselineRows $BaselineFailures -Clusters $Clusters -ApiKey $ApiKey
    $CurrentFailures = @($FollowUp.Current)
    $Recovered = @($FollowUp.Recovered)
    $Warnings = @($FollowUp.Warnings)
    $NewFailures = @()
    $NewRecoveries = @($Recovered | Where-Object { !$PreviouslyRecovered.ContainsKey($_.ObjectKey) })
}

$CsvColumns = "IncidentNumber", "WindowKey", "Status", "Cluster", "Environment", "ProtectionGroup", "SourceHostName", "ObjectName", "ObjectType", "RunType", "FirstFailedET", "LastFailedET", "RecoveredET", "ConsecutiveFailureCount", "Message", "ObjectKey"
Write-CsvFile $CurrentFailures (Join-Path $OutputFolder "current_failures.csv") $CsvColumns
Write-CsvFile $Recovered (Join-Path $OutputFolder "recovered.csv") $CsvColumns
Write-CsvFile $NewFailures (Join-Path $OutputFolder "new_failures.csv") $CsvColumns
Write-CsvFile $NewRecoveries (Join-Path $OutputFolder "new_recoveries.csv") $CsvColumns

$Worknotes = @"
Backup Failure Evidence

Incident: $Incident
Window: $($Window.Label)
Mode: $Mode
Generated At: $($Window.GeneratedET) ET

Summary:
- Baseline failures: $(@($BaselineFailures).Count)
- Current failures still failing: $(@($CurrentFailures).Count)
- Recovered baseline failures: $(@($Recovered).Count)
- New failures: $(@($NewFailures).Count)
- New recoveries since last run: $(@($NewRecoveries).Count)

Behavior:
- FullBaseline mode performs the all-environment latest uncleared failure scan.
- TargetedFollowUp mode checks only baseline failures stored in state.json.
- New failures are only evaluated during FullBaseline mode.
- Cohesity calls are GET-only.
- No Excel output is generated.
- No ServiceNow update is performed.
"@
if ($Warnings.Count -gt 0) { $Worknotes += "`nWarnings:`n" + (($Warnings | ForEach-Object { "- $_" }) -join "`n") }
$Worknotes | Set-Content (Join-Path $OutputFolder "worknotes.txt") -Encoding UTF8

$State = [pscustomobject]@{
    IncidentNumber = $Incident
    WindowKey = $Window.Key
    WindowLabel = $Window.Label
    Mode = $Mode
    LastRunET = $Window.GeneratedET
    BaselineFailures = @($BaselineFailures)
    CurrentFailures = @($CurrentFailures)
    Recovered = @($Recovered)
}
Write-JsonFile $State $StatePath

Write-Host "`nIncident: $Incident`nWindow  : $($Window.Label)`nMode    : $Mode`nOutput  : $OutputFolder`nCreated : current_failures.csv, recovered.csv, new_failures.csv, new_recoveries.csv, worknotes.txt, state.json"
