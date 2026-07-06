<#
.SYNOPSIS
One script for Cohesity backup failure incident evidence.

.DESCRIPTION
This replaces the operational need to run Cohesity_Backup_Failures first.
First run in the 18:00 ET compute window runs a full all-environment failure scan and writes the baseline into state.json.
Later runs in the same window check only that baseline for recovery.

GET-only against Cohesity. No Excel. No ServiceNow update.
Outputs only:
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

function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Get-Value($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
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

function Usecs-ToEt($Usecs) {
    if ($null -eq $Usecs -or [int64]$Usecs -le 0) { return "" }
    $Milliseconds = [int64]([double]$Usecs / 1000)
    $Utc = [DateTimeOffset]::FromUnixTimeMilliseconds($Milliseconds).UtcDateTime
    return ([TimeZoneInfo]::ConvertTimeFromUtc($Utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Et-ToUsecs([string]$EtText) {
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
    $Object | ConvertTo-Json -Depth 80 | Set-Content $Path -Encoding UTF8
}

function Write-CsvFile($Rows, [string]$Path, [string[]]$Columns) {
    $Folder = Split-Path $Path -Parent
    if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }
    $List = @($Rows)
    if ($List.Count -eq 0) {
        ($Columns -join ",") | Set-Content $Path -Encoding UTF8
    } else {
        $List | Select-Object $Columns | Export-Csv $Path -NoTypeInformation -Encoding UTF8
    }
}

function Query-String([hashtable]$Items) {
    return (($Items.GetEnumerator() | ForEach-Object {
        [uri]::EscapeDataString([string]$_.Key) + "=" + [uri]::EscapeDataString([string]$_.Value)
    }) -join "&")
}

function Invoke-GetJson([string]$Uri, [hashtable]$Headers) {
    $Response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
    if (!$Response.Content) { return $null }
    return $Response.Content | ConvertFrom-Json
}

function Get-WindowNow {
    $NowEt = [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
    if ($NowEt.Hour -lt 18) { $Start = $NowEt.Date.AddDays(-1).AddHours(18) } else { $Start = $NowEt.Date.AddHours(18) }
    $End = $Start.AddDays(1)
    $StartDate = $Start.ToString("yyyy-MM-dd")
    $EndDate = $End.ToString("yyyy-MM-dd")
    [pscustomobject]@{
        Key = "${StartDate}_1800ET"
        Label = "$StartDate 18:00 ET -> $EndDate 18:00 ET"
        StartUsecs = Et-ToUsecs ($Start.ToString("yyyy-MM-dd HH:mm:ss"))
        EndUsecs = Et-ToUsecs ($End.ToString("yyyy-MM-dd HH:mm:ss"))
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

function Get-EnvironmentMap {
    @(
        [pscustomobject]@{ Label = "Oracle";        Filter = "kOracle";        TargetType = "kDatabase";       ParentHostNeeded = $true  },
        [pscustomobject]@{ Label = "SQL";           Filter = "kSQL";           TargetType = "kDatabase";       ParentHostNeeded = $true  },
        [pscustomobject]@{ Label = "Physical";      Filter = "kPhysical";      TargetType = "kHost";           ParentHostNeeded = $false },
        [pscustomobject]@{ Label = "GenericNas";    Filter = "kGenericNas";    TargetType = "kHost";           ParentHostNeeded = $false },
        [pscustomobject]@{ Label = "Isilon";        Filter = "kIsilon";        TargetType = "kHost";           ParentHostNeeded = $false },
        [pscustomobject]@{ Label = "HyperV";        Filter = "kHyperV";        TargetType = "kVirtualMachine"; ParentHostNeeded = $false },
        [pscustomobject]@{ Label = "Acropolis";     Filter = "kAcropolis";     TargetType = "kVirtualMachine"; ParentHostNeeded = $false },
        [pscustomobject]@{ Label = "RemoteAdapter"; Filter = "kRemoteAdapter"; TargetType = "kRemoteAdapter";  ParentHostNeeded = $false }
    )
}

function Is-SuccessStatus([string]$Status) { return ($Status -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning")) }
function Is-FailedStatus([string]$Status) { return ($Status -in @("Failed", "kFailed")) }

function Get-ClusterDisplayName($Cluster) {
    $Name = Clean-Value (Get-Value $Cluster "name" "")
    if (!$Name) { $Name = Clean-Value (Get-Value $Cluster "clusterName" "") }
    if (!$Name) { $Name = Clean-Value (Get-Value $Cluster "displayName" "") }
    if (!$Name) { $Name = "Unknown-$(Get-Value $Cluster 'clusterId' '')" }
    return $Name
}

function Get-ObjectKeyFromRunObject($RunObject, [string]$ClusterId, [string]$Environment, [string]$ProtectionGroupId, [string]$ClusterName, [string]$ProtectionGroupName, [string]$SourceHostName, [string]$ObjectName) {
    $Object = Get-Value $RunObject "object" $null
    $ObjectId = [string](Get-Value $Object "id" "")
    if ($ObjectId) { return "$ClusterId|$Environment|$ProtectionGroupId|$ObjectId" }
    return "$ClusterName|$Environment|$ProtectionGroupName|$SourceHostName|$ObjectName"
}

function Get-FailedAttempts($RunObject) {
    try { return @(Get-Value $RunObject.localSnapshotInfo "failedAttempts" @()) } catch { return @() }
}

function Get-FailureMessage($RunObject, $Info) {
    $Attempts = Get-FailedAttempts $RunObject
    $Message = (($Attempts | ForEach-Object { Clean-Value (Get-Value $_ "message" "") } | Where-Object { $_ }) -join " | ")
    if (!$Message) { $Message = Clean-Value (Get-Value $Info "messages" "") }
    if (!$Message) { $Message = "Run marked Failed" }
    return $Message
}

function Resolve-ObjectNames($RunObject, [string]$Environment, [hashtable]$IdToName) {
    $Object = Get-Value $RunObject "object" $null
    $ObjectType = [string](Get-Value $Object "objectType" "")
    $ObjectName = Clean-Value (Get-Value $Object "name" "")
    $SourceHostName = ""

    if ($Environment -in @("Oracle", "SQL") -and $ObjectType -eq "kHost") {
        $SourceHostName = $ObjectName
        $ObjectName = "No DBs Found (Host-Level Failure)"
    }
    elseif ($Environment -in @("Oracle", "SQL") -and $ObjectType -eq "kDatabase") {
        $SourceId = [string](Get-Value $Object "sourceId" "")
        if ($SourceId -and $IdToName.ContainsKey($SourceId)) { $SourceHostName = $IdToName[$SourceId] }
    }

    [pscustomobject]@{ SourceHostName = $SourceHostName; ObjectName = $ObjectName; ObjectType = $ObjectType }
}

function New-FailureRow($Incident, $Window, $Cluster, $EnvironmentInfo, $ProtectionGroup, $RunObject, $Info, [hashtable]$IdToName) {
    $ClusterId = [string](Get-Value $Cluster "clusterId" "")
    $ClusterName = Get-ClusterDisplayName $Cluster
    $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
    $ProtectionGroupName = Clean-Value (Get-Value $ProtectionGroup "name" "")
    $Resolved = Resolve-ObjectNames $RunObject $EnvironmentInfo.Label $IdToName
    $RunType = Clean-Value (Get-Value $Info "runType" "")
    $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
    if ($EndUsecs -le 0) { $EndUsecs = [int64](Get-Value $Info "startTimeUsecs" 0) }
    $Key = Get-ObjectKeyFromRunObject $RunObject $ClusterId $EnvironmentInfo.Label $ProtectionGroupId $ClusterName $ProtectionGroupName $Resolved.SourceHostName $Resolved.ObjectName

    [pscustomobject]@{
        IncidentNumber = $Incident
        WindowKey = $Window.Key
        Status = "StillFailing"
        Cluster = $ClusterName
        Environment = $EnvironmentInfo.Label
        ProtectionGroup = $ProtectionGroupName
        SourceHostName = $Resolved.SourceHostName
        ObjectName = $Resolved.ObjectName
        ObjectType = $Resolved.ObjectType
        RunType = $RunType
        FirstFailedET = Usecs-ToEt $EndUsecs
        LastFailedET = Usecs-ToEt $EndUsecs
        LastFailedUsecs = $EndUsecs
        RecoveredET = ""
        ConsecutiveFailureCount = 1
        Message = Get-FailureMessage $RunObject $Info
        ObjectKey = $Key
        ClusterId = $ClusterId
        ProtectionGroupId = $ProtectionGroupId
        EnvironmentFilter = $EnvironmentInfo.Filter
    }
}

function New-RunLevelFailureRow($Incident, $Window, $Cluster, $EnvironmentInfo, $ProtectionGroup, $Info) {
    $ClusterId = [string](Get-Value $Cluster "clusterId" "")
    $ClusterName = Get-ClusterDisplayName $Cluster
    $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
    $ProtectionGroupName = Clean-Value (Get-Value $ProtectionGroup "name" "")
    $RunType = Clean-Value (Get-Value $Info "runType" "")
    $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
    if ($EndUsecs -le 0) { $EndUsecs = [int64](Get-Value $Info "startTimeUsecs" 0) }
    $Key = "$ClusterName|$($EnvironmentInfo.Label)|$ProtectionGroupName||$ProtectionGroupName|$RunType"

    [pscustomobject]@{
        IncidentNumber = $Incident
        WindowKey = $Window.Key
        Status = "StillFailing"
        Cluster = $ClusterName
        Environment = $EnvironmentInfo.Label
        ProtectionGroup = $ProtectionGroupName
        SourceHostName = ""
        ObjectName = $ProtectionGroupName
        ObjectType = "ProtectionGroup"
        RunType = $RunType
        FirstFailedET = Usecs-ToEt $EndUsecs
        LastFailedET = Usecs-ToEt $EndUsecs
        LastFailedUsecs = $EndUsecs
        RecoveredET = ""
        ConsecutiveFailureCount = 1
        Message = Clean-Value (Get-Value $Info "messages" "Run marked Failed")
        ObjectKey = $Key
        ClusterId = $ClusterId
        ProtectionGroupId = $ProtectionGroupId
        EnvironmentFilter = $EnvironmentInfo.Filter
    }
}

function Build-IdToName($Runs) {
    $Map = @{}
    foreach ($Run in $Runs) {
        foreach ($RunObject in (As-List (Get-Value $Run "objects" @()))) {
            $Object = Get-Value $RunObject "object" $null
            $ObjectId = [string](Get-Value $Object "id" "")
            $ObjectName = Clean-Value (Get-Value $Object "name" "")
            if ($ObjectId -and $ObjectName -and !$Map.ContainsKey($ObjectId)) { $Map[$ObjectId] = $ObjectName }
        }
    }
    return $Map
}

function Collect-FullBaseline($Incident, $Window, $Clusters, $ApiKey) {
    $Rows = @()
    foreach ($EnvironmentInfo in (Get-EnvironmentMap)) {
        foreach ($Cluster in $Clusters) {
            $ClusterId = [string](Get-Value $Cluster "clusterId" "")
            $Headers = @{ apiKey = $ApiKey; accept = "application/json"; accessClusterId = $ClusterId }
            try {
                $PgUri = "$BaseUrl/v2/data-protect/protection-groups?$(Query-String @{ environments=$EnvironmentInfo.Filter; isDeleted='false'; isPaused='false'; isActive='true' })"
                $ProtectionGroups = @((Invoke-GetJson $PgUri $Headers).protectionGroups)
            } catch { continue }

            foreach ($ProtectionGroup in $ProtectionGroups) {
                $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
                try {
                    $RunUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?$(Query-String @{ numRuns=$NumRuns; excludeNonRestorableRuns='false'; includeObjectDetails='true' })"
                    $Runs = @((Invoke-GetJson $RunUri $Headers).runs)
                } catch { continue }
                if ($Runs.Count -eq 0) { continue }

                $IdToName = Build-IdToName $Runs
                $RunTypes = @($Runs | ForEach-Object { foreach ($Info in (As-List (Get-Value $_ "localBackupInfo" @()))) { Clean-Value (Get-Value $Info "runType" "") } } | Where-Object { $_ } | Select-Object -Unique)

                foreach ($RunType in $RunTypes) {
                    $Cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $LatestFailByKey = @{}
                    $RunsForType = @($Runs | Where-Object { (As-List (Get-Value $_ "localBackupInfo" @()) | Where-Object { (Clean-Value (Get-Value $_ "runType" "")) -eq $RunType }).Count -gt 0 })
                    $RunsForType = @($RunsForType | Sort-Object { $Info = (As-List (Get-Value $_ "localBackupInfo" @()) | Select-Object -First 1); [int64](Get-Value $Info "endTimeUsecs" 0) } -Descending)

                    foreach ($Run in $RunsForType) {
                        $Info = As-List (Get-Value $Run "localBackupInfo" @()) | Where-Object { (Clean-Value (Get-Value $_ "runType" "")) -eq $RunType } | Select-Object -First 1
                        if (!$Info) { continue }
                        $Status = [string](Get-Value $Info "status" "")
                        $RunObjects = As-List (Get-Value $Run "objects" @())

                        if (Is-SuccessStatus $Status) {
                            if ($RunObjects.Count -eq 0) {
                                $ProtectionGroupName = Clean-Value (Get-Value $ProtectionGroup "name" "")
                                [void]$Cleared.Add("$(Get-ClusterDisplayName $Cluster)|$($EnvironmentInfo.Label)|$ProtectionGroupName||$ProtectionGroupName|$RunType")
                            }
                            foreach ($RunObject in $RunObjects) {
                                $Resolved = Resolve-ObjectNames $RunObject $EnvironmentInfo.Label $IdToName
                                $Key = Get-ObjectKeyFromRunObject $RunObject $ClusterId $EnvironmentInfo.Label $ProtectionGroupId (Get-ClusterDisplayName $Cluster) (Clean-Value (Get-Value $ProtectionGroup "name" "")) $Resolved.SourceHostName $Resolved.ObjectName
                                if ($Key) { [void]$Cleared.Add($Key) }
                            }
                            continue
                        }

                        if (!(Is-FailedStatus $Status)) { continue }

                        if ($RunObjects.Count -eq 0) {
                            $Row = New-RunLevelFailureRow $Incident $Window $Cluster $EnvironmentInfo $ProtectionGroup $Info
                            if (!$Cleared.Contains($Row.ObjectKey) -and !$LatestFailByKey.ContainsKey($Row.ObjectKey)) { $LatestFailByKey[$Row.ObjectKey] = $Row }
                            continue
                        }

                        foreach ($RunObject in $RunObjects) {
                            $Object = Get-Value $RunObject "object" $null
                            if (!$Object) { continue }
                            $ObjectType = [string](Get-Value $Object "objectType" "")
                            $Attempts = Get-FailedAttempts $RunObject
                            if ($Attempts.Count -eq 0 -and $EnvironmentInfo.Label -ne "Physical") { continue }

                            $Relevant = $true
                            if ($EnvironmentInfo.Label -in @("Oracle", "SQL")) { $Relevant = ($ObjectType -in @("kDatabase", "kHost")) }
                            elseif ($EnvironmentInfo.Label -in @("HyperV", "Acropolis")) { $Relevant = ($ObjectType -eq "kVirtualMachine") }
                            elseif ($EnvironmentInfo.Label -in @("Physical", "GenericNas", "Isilon")) { $Relevant = ($ObjectType -eq "kHost" -or $Attempts.Count -gt 0) }
                            if (!$Relevant) { continue }

                            $Row = New-FailureRow $Incident $Window $Cluster $EnvironmentInfo $ProtectionGroup $RunObject $Info $IdToName
                            if (!$Cleared.Contains($Row.ObjectKey) -and !$LatestFailByKey.ContainsKey($Row.ObjectKey)) { $LatestFailByKey[$Row.ObjectKey] = $Row }
                        }
                    }
                    foreach ($Key in $LatestFailByKey.Keys) { $Rows += $LatestFailByKey[$Key] }
                }
            }
        }
    }

    return @($Rows | Group-Object ObjectKey | ForEach-Object { $_.Group | Sort-Object LastFailedUsecs -Descending | Select-Object -First 1 } | Sort-Object Cluster, ProtectionGroup, Environment, LastFailedUsecs -Descending)
}

function Test-BaselineRecovered($BaselineRow, $Runs) {
    $IdToName = Build-IdToName $Runs
    foreach ($Run in ($Runs | Sort-Object { $Info = (As-List (Get-Value $_ "localBackupInfo" @()) | Select-Object -First 1); [int64](Get-Value $Info "endTimeUsecs" 0) } -Descending)) {
        foreach ($Info in (As-List (Get-Value $Run "localBackupInfo" @()))) {
            if (!(Is-SuccessStatus ([string](Get-Value $Info "status" "")))) { continue }
            if ((Clean-Value (Get-Value $Info "runType" "")) -ne $BaselineRow.RunType) { continue }
            $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
            if ($EndUsecs -le [int64]$BaselineRow.LastFailedUsecs) { continue }

            $RunObjects = As-List (Get-Value $Run "objects" @())
            if ($RunObjects.Count -eq 0 -and $BaselineRow.ObjectType -eq "ProtectionGroup") { return (Usecs-ToEt $EndUsecs) }
            foreach ($RunObject in $RunObjects) {
                $Resolved = Resolve-ObjectNames $RunObject $BaselineRow.Environment $IdToName
                $Key = Get-ObjectKeyFromRunObject $RunObject $BaselineRow.ClusterId $BaselineRow.Environment $BaselineRow.ProtectionGroupId $BaselineRow.Cluster $BaselineRow.ProtectionGroup $Resolved.SourceHostName $Resolved.ObjectName
                if ($Key -eq $BaselineRow.ObjectKey) { return (Usecs-ToEt $EndUsecs) }
            }
        }
    }
    return ""
}

function Collect-TargetedFollowUp($BaselineRows, $Clusters, $ApiKey) {
    $Current = @()
    $Recovered = @()
    $Warnings = @()
    $ClusterById = @{}
    foreach ($Cluster in $Clusters) { $ClusterById[[string](Get-Value $Cluster "clusterId" "")] = $Cluster }

    foreach ($Group in (@($BaselineRows) | Group-Object ClusterId, ProtectionGroupId)) {
        $Sample = $Group.Group[0]
        $Cluster = $ClusterById[[string]$Sample.ClusterId]
        if (!$Cluster) { $Warnings += "Cluster not found: $($Sample.Cluster)"; $Current += $Group.Group; continue }
        $Headers = @{ apiKey = $ApiKey; accept = "application/json"; accessClusterId = [string]$Sample.ClusterId }
        try {
            $RunUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString([string]$Sample.ProtectionGroupId))/runs?$(Query-String @{ numRuns=$NumRuns; excludeNonRestorableRuns='false'; includeObjectDetails='true' })"
            $Runs = @((Invoke-GetJson $RunUri $Headers).runs)
        } catch {
            $Warnings += "Runs lookup failed for $($Sample.Cluster)/$($Sample.ProtectionGroup): $($_.Exception.Message)"
            $Current += $Group.Group
            continue
        }
        foreach ($BaselineRow in $Group.Group) {
            $RecoveredET = Test-BaselineRecovered $BaselineRow $Runs
            if ($RecoveredET) {
                $Recovered += [pscustomobject]@{
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
                $Current += $BaselineRow
            }
        }
    }

    return [pscustomobject]@{ Current = @($Current); Recovered = @($Recovered); Warnings = @($Warnings) }
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

$Headers = @{ apiKey = $ApiKey; accept = "application/json" }
$Clusters = @((Invoke-GetJson "$BaseUrl/v2/mcm/cluster-mgmt/info" $Headers).cohesityClusters)

if ($ResetBaseline -or !$PreviousState -or !(Get-Value $PreviousState "BaselineFailures" $null) -or (As-List (Get-Value $PreviousState "BaselineFailures" @())).Count -eq 0) {
    $Mode = "FullBaseline"
    $BaselineFailures = @(Collect-FullBaseline $Incident $Window $Clusters $ApiKey)
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
    $FollowUp = Collect-TargetedFollowUp $BaselineFailures $Clusters $ApiKey
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
- FullBaseline mode runs the full all-environment failure scan and creates state.json.
- TargetedFollowUp mode checks only the baseline failures stored in state.json.
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
