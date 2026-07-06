<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
Single-script incident evidence workflow.
Every run scans Cohesity again, finds latest uncleared failures, compares with prior state,
and writes current/new/recovered evidence plus paste-ready worknotes.

No Excel. No ServiceNow writes. Cohesity API calls are GET-only.
Authentication uses the existing AES helper/encrypted key method only.

Test one cluster:
  .\Get-CohesityBackupFailureWindowConsolidator.ps1 -ClusterName "CLUSTER_NAME"

All clusters:
  .\Get-CohesityBackupFailureWindowConsolidator.ps1
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
    [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
    [string]$ClusterName = "",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = ""
)

$ErrorActionPreference = "Stop"

function Get-EtZone {
    try { [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Get-Prop($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { return $p.Value }
    return $Default
}

function As-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function Clean($Value) {
    if ($null -eq $Value) { return "" }
    if ($Value -is [array]) { $Value = $Value -join " | " }
    return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim()
}

function Get-NowEtText {
    ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Convert-UsecsToEtText($Usecs) {
    if ($null -eq $Usecs -or [int64]$Usecs -eq 0) { return "" }
    try { $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000)).UtcDateTime }
    catch { return "" }
    ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Convert-EtToUsecs([datetime]$EtDate) {
    $utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtDate, [DateTimeKind]::Unspecified), $script:EtZone)
    [int64](([DateTimeOffset]::new($utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000)
}

function Read-Json([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $raw = Get-Content $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    $raw | ConvertFrom-Json
}

function Write-Json($Object, [string]$Path) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Write-Csv($Rows, [string]$Path, [string[]]$Columns) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $list = @($Rows)
    if ($list.Count -eq 0) {
        ($Columns -join ",") | Set-Content -Path $Path -Encoding UTF8
    } else {
        $list | Select-Object $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec 120
    } else {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    $r.Content | ConvertFrom-Json
}

function Get-CohesityApiKey {
    if (!(Test-Path $HelperPath)) { throw "Missing API key helper: $HelperPath" }
    if (!(Test-Path $EncryptedFile)) { throw "Missing encrypted key file: $EncryptedFile" }
    . $HelperPath
    $key = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key is blank from AES helper." }
    $key.Trim()
}

function Get-ComputeWindow {
    # Native implementation matching the existing 18:00 ET backup-failure compute window.
    # If compute_window.js exists and emits windowKey/windowLabel, use its labels for sync.
    $nowEt = [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
    if ($nowEt.Hour -lt 18) { $start = $nowEt.Date.AddDays(-1).AddHours(18) } else { $start = $nowEt.Date.AddHours(18) }
    $end = $start.AddDays(1)

    $window = [ordered]@{
        WindowKey = "$($start.ToString('yyyy-MM-dd'))_1800ET"
        WindowLabel = "$($start.ToString('yyyy-MM-dd')) 18:00 ET -> $($end.ToString('yyyy-MM-dd')) 18:00 ET"
        WindowStartET = $start.ToString("yyyy-MM-dd HH:mm:ss")
        WindowEndET = $end.ToString("yyyy-MM-dd HH:mm:ss")
        WindowStartUsecs = Convert-EtToUsecs $start
        WindowEndUsecs = Convert-EtToUsecs $end
        GeneratedET = Get-NowEtText
    }

    $js = Join-Path $PSScriptRoot "compute_window.js"
    $node = Get-Command node -ErrorAction SilentlyContinue
    if ((Test-Path $js) -and $node) {
        try {
            $raw = (& $node.Source $js 2>$null | Out-String).Trim()
            if ($raw) {
                $cw = $raw | ConvertFrom-Json
                $wk = Clean (Get-Prop $cw "windowKey" "")
                $wl = Clean (Get-Prop $cw "windowLabel" "")
                if ($wk) { $window.WindowKey = $wk }
                if ($wl) { $window.WindowLabel = $wl }
            }
        } catch {
            # Do not fail collection if local node/compute_window.js is not runnable.
        }
    }
    [pscustomobject]$window
}

function Resolve-IncidentLock($Window) {
    if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
    $registryPath = Join-Path $OutputRoot "BackupFailure_WindowRegistry.json"
    $registry = Read-Json $registryPath
    if (!$registry) {
        $registry = [pscustomobject]@{
            TimeZone = "America/New_York"
            WindowMode = "compute_window.js compatible"
            Windows = [pscustomobject]@{}
        }
    }

    $existing = $registry.Windows.PSObject.Properties[$Window.WindowKey]
    if ($existing) { return $existing.Value }

    $inc = $IncidentNumber
    if (!$inc) { $inc = Read-Host "Enter incident number for this backup-failure window" }
    $inc = $inc.Trim().ToUpper()
    if ($inc -notmatch '^INC[0-9A-Z]+$') { throw "Invalid incident number: $inc" }

    $entry = [pscustomobject]@{
        IncidentNumber = $inc
        WindowKey = $Window.WindowKey
        WindowLabel = $Window.WindowLabel
        WindowStartET = $Window.WindowStartET
        WindowEndET = $Window.WindowEndET
        FirstRunET = $Window.GeneratedET
        LastRunET = $Window.GeneratedET
        OutputFolder = (Join-Path $OutputRoot $inc)
    }
    $registry.Windows | Add-Member -MemberType NoteProperty -Name $Window.WindowKey -Value $entry -Force
    Write-Json $registry $registryPath
    $entry
}

function Get-ClusterName($Cluster) {
    $n = Clean (Get-Prop $Cluster "name" "")
    if (!$n) { $n = Clean (Get-Prop $Cluster "clusterName" "") }
    if (!$n) { $n = Clean (Get-Prop $Cluster "displayName" "") }
    if (!$n) { $n = "Unknown-$([string](Get-Prop $Cluster 'clusterId' ''))" }
    $n
}

function Get-EnvironmentMap {
    @(
        [pscustomobject]@{ Label="Oracle";        Filter="kOracle";        TargetObjectType="kDatabase";       ParentHostNeeded=$true;  RunLimit=$NumRuns },
        [pscustomobject]@{ Label="SQL";           Filter="kSQL";           TargetObjectType="kDatabase";       ParentHostNeeded=$true;  RunLimit=$NumRuns },
        [pscustomobject]@{ Label="Physical";      Filter="kPhysical";      TargetObjectType="kHost";           ParentHostNeeded=$false; RunLimit=$NumRuns },
        [pscustomobject]@{ Label="GenericNas";    Filter="kGenericNas";    TargetObjectType="kHost";           ParentHostNeeded=$false; RunLimit=$NumRuns },
        [pscustomobject]@{ Label="HyperV";        Filter="kHyperV";        TargetObjectType="kVirtualMachine"; ParentHostNeeded=$false; RunLimit=$NumRuns },
        [pscustomobject]@{ Label="Acropolis";     Filter="kAcropolis";     TargetObjectType="kVirtualMachine"; ParentHostNeeded=$false; RunLimit=$NumRuns },
        [pscustomobject]@{ Label="RemoteAdapter"; Filter="kRemoteAdapter"; TargetObjectType="kRemoteAdapter";  ParentHostNeeded=$false; RunLimit=10 },
        [pscustomobject]@{ Label="Isilon";        Filter="kIsilon";        TargetObjectType="kHost";           ParentHostNeeded=$false; RunLimit=$NumRuns }
    )
}

function Get-FirstLocalBackupInfo($Run) {
    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    @(($Run.localBackupInfo))[0]
}

function Get-RunEndUsecs($Run) {
    $i = Get-FirstLocalBackupInfo $Run
    if (!$i) { return 0 }
    [int64](Get-Prop $i "endTimeUsecs" 0)
}

function Is-FailedStatus([string]$Status) {
    $Status -in @("Failed", "kFailed")
}

function Is-SuccessStatus([string]$Status) {
    $Status -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning")
}

function Get-FailedAttempts($RunObject) {
    try { @(Get-Prop $RunObject.localSnapshotInfo "failedAttempts" @()) } catch { @() }
}

function Is-SuccessObject($RunObject) {
    if ($null -eq $RunObject -or $null -eq $RunObject.localSnapshotInfo) { return $false }
    $attempts = Get-FailedAttempts $RunObject
    $attempts.Count -eq 0
}

function Get-RunObjectKey($RunObject, [string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId, [string]$ProtectionGroupName) {
    if ($null -eq $RunObject -or $null -eq $RunObject.object) { return "" }
    $obj = $RunObject.object
    $objId = Clean (Get-Prop $obj "id" "")
    if ($objId) { return "$ClusterId|$EnvironmentLabel|$ProtectionGroupId|$objId" }
    $host = Clean (Get-Prop $obj "sourceId" "")
    $name = Clean (Get-Prop $obj "name" "")
    $type = Clean (Get-Prop $obj "objectType" "")
    "$ClusterId|$EnvironmentLabel|$ProtectionGroupName|$host|$type|$name"
}

function Get-ObjectNameMap($Runs) {
    $m = @{}
    foreach ($run in $Runs) {
        foreach ($ro in (As-Array (Get-Prop $run "objects" @()))) {
            $obj = Get-Prop $ro "object" $null
            if ($obj -and $obj.id -and $obj.name) { $m[[string]$obj.id] = Clean $obj.name }
        }
    }
    $m
}

function Resolve-HostName($RunObject, [hashtable]$ObjectNameById) {
    $obj = Get-Prop $RunObject "object" $null
    if (!$obj) { return "" }
    $sourceId = Clean (Get-Prop $obj "sourceId" "")
    if ($sourceId -and $ObjectNameById.ContainsKey($sourceId)) { return $ObjectNameById[$sourceId] }
    ""
}

function New-FailureRow {
    param(
        [string]$IncidentNumber,
        $Window,
        $Cluster,
        $Env,
        $ProtectionGroup,
        [string]$ObjectKey,
        [string]$SourceHostName,
        [string]$ObjectName,
        [string]$ObjectType,
        [string]$RunType,
        [int64]$StartUsecs,
        [int64]$EndUsecs,
        [string]$Message,
        [int]$ConsecutiveFailureCount = 1,
        [string]$LifecycleStatus = "StillFailing"
    )

    $effective = if ($EndUsecs -gt 0) { $EndUsecs } else { $StartUsecs }
    [pscustomobject]@{
        IncidentNumber = $IncidentNumber
        WindowKey = $Window.WindowKey
        LifecycleStatus = $LifecycleStatus
        Cluster = Get-ClusterName $Cluster
        Environment = $Env.Label
        ProtectionGroup = Clean (Get-Prop $ProtectionGroup "name" "")
        SourceHostName = Clean $SourceHostName
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = Convert-UsecsToEtText $effective
        LastFailedET = Convert-UsecsToEtText $effective
        LastFailedUsecs = $effective
        RecoveredET = ""
        ConsecutiveFailureCount = $ConsecutiveFailureCount
        Message = Clean $Message
        ObjectKey = $ObjectKey
        ClusterId = [string](Get-Prop $Cluster "clusterId" "")
        ProtectionGroupId = [string](Get-Prop $ProtectionGroup "id" "")
        EnvironmentFilter = $Env.Filter
    }
}

function New-RunLevelRow($IncidentNumber, $Window, $Cluster, $Env, $ProtectionGroup, $Info, [string]$Message) {
    $clusterId = [string](Get-Prop $Cluster "clusterId" "")
    $pgId = [string](Get-Prop $ProtectionGroup "id" "")
    $pgName = Clean (Get-Prop $ProtectionGroup "name" "")
    $runType = Clean (Get-Prop $Info "runType" "")
    $key = "$clusterId|$($Env.Label)|$pgId|RUNLEVEL|$runType|$pgName"
    New-FailureRow -IncidentNumber $IncidentNumber -Window $Window -Cluster $Cluster -Env $Env -ProtectionGroup $ProtectionGroup -ObjectKey $key -SourceHostName "" -ObjectName $pgName -ObjectType "ProtectionGroup" -RunType $runType -StartUsecs ([int64](Get-Prop $Info "startTimeUsecs" 0)) -EndUsecs ([int64](Get-Prop $Info "endTimeUsecs" 0)) -Message $Message
}

function Get-FailureMessage($Attempts) {
    $msgs = @()
    foreach ($a in @($Attempts)) {
        $m = Clean (Get-Prop $a "message" "")
        if ($m) { $msgs += $m }
    }
    ($msgs -join " | ")
}

function Get-ProtectionGroups($Cluster, $Env, [string]$ApiKey) {
    $clusterId = [string](Get-Prop $Cluster "clusterId" "")
    $headers = @{ accept = "application/json"; apiKey = $ApiKey; accessClusterId = $clusterId }
    $pgs = @()
    foreach ($filter in ($Env.Filter.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        try {
            $uri = "$BaseUrl/v2/data-protect/protection-groups?environments=$filter&isDeleted=false&isPaused=false&isActive=true"
            $json = Invoke-HeliosGetJson -Uri $uri -Headers $headers
            if ($json -and $json.protectionGroups) { $pgs += @($json.protectionGroups) }
        } catch {
            Write-Warning "Protection group lookup failed for $(Get-ClusterName $Cluster) / $filter : $($_.Exception.Message)"
        }
    }
    @($pgs | Sort-Object id -Unique)
}

function Get-ProtectionGroupRuns($Cluster, [string]$ProtectionGroupId, [int]$RunLimit, [string]$ApiKey) {
    $clusterId = [string](Get-Prop $Cluster "clusterId" "")
    $headers = @{ accept = "application/json"; apiKey = $ApiKey; accessClusterId = $clusterId }
    $uri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=$RunLimit&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json = Invoke-HeliosGetJson -Uri $uri -Headers $headers
    if ($json -and $json.runs) { return @($json.runs) }
    @()
}

function Collect-CurrentUnclearedFailures($IncidentNumber, $Window, $Clusters, [string]$ApiKey) {
    $rows = @()
    foreach ($env in (Get-EnvironmentMap)) {
        foreach ($cluster in $Clusters) {
            $pgs = Get-ProtectionGroups -Cluster $cluster -Env $env -ApiKey $ApiKey
            foreach ($pg in $pgs) {
                $pgId = [string](Get-Prop $pg "id" "")
                $pgName = Clean (Get-Prop $pg "name" "")
                try { $runs = Get-ProtectionGroupRuns -Cluster $cluster -ProtectionGroupId $pgId -RunLimit $env.RunLimit -ApiKey $ApiKey }
                catch {
                    Write-Warning "Runs lookup failed for $(Get-ClusterName $cluster) / $pgName : $($_.Exception.Message)"
                    continue
                }
                if ($runs.Count -eq 0) { continue }

                $runTypes = @($runs | ForEach-Object { $i = Get-FirstLocalBackupInfo $_; if ($i) { Clean (Get-Prop $i "runType" "") } } | Where-Object { $_ } | Select-Object -Unique)
                foreach ($runType in $runTypes) {
                    $runsForType = @($runs | Where-Object { $i = Get-FirstLocalBackupInfo $_; $i -and (Clean (Get-Prop $i "runType" "")) -eq $runType } | Sort-Object { Get-RunEndUsecs $_ } -Descending)
                    $cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $latestByKey = @{}
                    $objectNameById = Get-ObjectNameMap $runsForType
                    $isNas = $env.Label -in @("GenericNas", "Isilon")
                    $runLevelCleared = $false

                    foreach ($run in $runsForType) {
                        $info = Get-FirstLocalBackupInfo $run
                        if (!$info) { continue }
                        $status = Clean (Get-Prop $info "status" "")
                        $startUsecs = [int64](Get-Prop $info "startTimeUsecs" 0)
                        $endUsecs = [int64](Get-Prop $info "endTimeUsecs" 0)
                        $objects = As-Array (Get-Prop $run "objects" @())

                        if ($isNas -and (Is-SuccessStatus $status)) { $runLevelCleared = $true }

                        foreach ($ro in $objects) {
                            if (Is-SuccessObject $ro) {
                                $key = Get-RunObjectKey -RunObject $ro -ClusterId ([string](Get-Prop $cluster "clusterId" "")) -EnvironmentLabel $env.Label -ProtectionGroupId $pgId -ProtectionGroupName $pgName
                                if ($key) { [void]$cleared.Add($key) }
                            }
                        }

                        if (!(Is-FailedStatus $status)) { continue }

                        $foundObjectFailure = $false
                        foreach ($ro in $objects) {
                            $obj = Get-Prop $ro "object" $null
                            if (!$obj) { continue }
                            $attempts = Get-FailedAttempts $ro
                            if ($attempts.Count -eq 0) { continue }

                            $objType = Clean (Get-Prop $obj "objectType" "")
                            $objEnv = Clean (Get-Prop $obj "environment" "")
                            $isTarget = $false
                            if ($isNas) { $isTarget = $true }
                            elseif ($objType -eq $env.TargetObjectType) { $isTarget = $true }
                            elseif ($env.ParentHostNeeded -and ($objType -eq "kHost" -or $objEnv -eq "kPhysical")) { $isTarget = $true }
                            if (!$isTarget) { continue }

                            $key = Get-RunObjectKey -RunObject $ro -ClusterId ([string](Get-Prop $cluster "clusterId" "")) -EnvironmentLabel $env.Label -ProtectionGroupId $pgId -ProtectionGroupName $pgName
                            if (!$key -or $cleared.Contains($key) -or $latestByKey.ContainsKey($key)) { continue }

                            $message = Get-FailureMessage $attempts
                            if (!$message) { $message = "Object failed; failedAttempts did not include a message" }
                            $hostName = if ($env.ParentHostNeeded) { Resolve-HostName $ro $objectNameById } else { "" }
                            if (!$hostName -and $objType -eq "kHost") { $hostName = Clean (Get-Prop $obj "name" "") }
                            $objName = Clean (Get-Prop $obj "name" "")
                            if (!$objName) { $objName = $pgName }

                            $latestByKey[$key] = New-FailureRow -IncidentNumber $IncidentNumber -Window $Window -Cluster $cluster -Env $env -ProtectionGroup $pg -ObjectKey $key -SourceHostName $hostName -ObjectName $objName -ObjectType $objType -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $message
                            $foundObjectFailure = $true
                        }

                        if (!$foundObjectFailure -and !$runLevelCleared) {
                            $msg = Clean (Get-Prop $info "messages" "")
                            if (!$msg) { $msg = "Run marked failed; no object-level failedAttempts returned" }
                            $rl = New-RunLevelRow $IncidentNumber $Window $cluster $env $pg $info $msg
                            if (!$cleared.Contains($rl.ObjectKey) -and !$latestByKey.ContainsKey($rl.ObjectKey)) { $latestByKey[$rl.ObjectKey] = $rl }
                        }
                    }
                    foreach ($k in $latestByKey.Keys) { $rows += $latestByKey[$k] }
                }
            }
        }
    }
    @($rows | Group-Object ObjectKey | ForEach-Object { $_.Group | Sort-Object LastFailedUsecs -Descending | Select-Object -First 1 })
}

function Test-RecoveryForRows($Rows, $ClustersById, [string]$ApiKey) {
    $recovered = @()
    $unknown = @()
    foreach ($group in (@($Rows) | Group-Object ClusterId, ProtectionGroupId)) {
        $sample = $group.Group[0]
        $clusterId = [string]$sample.ClusterId
        $pgId = [string]$sample.ProtectionGroupId
        $cluster = $ClustersById[$clusterId]
        if (!$cluster -or !$pgId) { $unknown += $group.Group; continue }
        try { $runs = Get-ProtectionGroupRuns -Cluster $cluster -ProtectionGroupId $pgId -RunLimit $NumRuns -ApiKey $ApiKey }
        catch { $unknown += $group.Group; continue }

        foreach ($row in $group.Group) {
            $recoveredEt = ""
            $runsForType = @($runs | Where-Object { $i = Get-FirstLocalBackupInfo $_; $i -and (Clean (Get-Prop $i "runType" "")) -eq $row.RunType } | Sort-Object { Get-RunEndUsecs $_ } -Descending)
            foreach ($run in $runsForType) {
                $info = Get-FirstLocalBackupInfo $run
                if (!$info) { continue }
                $endUsecs = [int64](Get-Prop $info "endTimeUsecs" 0)
                if ($endUsecs -le [int64]$row.LastFailedUsecs) { continue }
                if ($row.ObjectType -eq "ProtectionGroup" -and (Is-SuccessStatus (Clean (Get-Prop $info "status" "")))) {
                    $recoveredEt = Convert-UsecsToEtText $endUsecs
                    break
                }
                foreach ($ro in (As-Array (Get-Prop $run "objects" @()))) {
                    if (!(Is-SuccessObject $ro)) { continue }
                    $candidateKey = Get-RunObjectKey -RunObject $ro -ClusterId $clusterId -EnvironmentLabel $row.Environment -ProtectionGroupId $pgId -ProtectionGroupName $row.ProtectionGroup
                    if ($candidateKey -eq $row.ObjectKey) {
                        $recoveredEt = Convert-UsecsToEtText $endUsecs
                        break
                    }
                }
                if ($recoveredEt) { break }
            }

            if ($recoveredEt) {
                $newRow = $row | Select-Object *
                $newRow.LifecycleStatus = "NewlyRecoveredThisCheck"
                $newRow.RecoveredET = $recoveredEt
                $recovered += $newRow
            } else {
                $newRow = $row | Select-Object *
                $newRow.LifecycleStatus = "UnknownNeedsReview"
                $unknown += $newRow
            }
        }
    }
    [pscustomobject]@{ Recovered = @($recovered); Unknown = @($unknown) }
}

function Index-ByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = [string](Get-Prop $r "ObjectKey" "")
        if ($k) { $h[$k] = $r }
    }
    $h
}

function Merge-Recovered($OldRecovered, $NewRecovered) {
    $h = @{}
    foreach ($r in @($OldRecovered)) { if ($r.ObjectKey) { $h[[string]$r.ObjectKey] = $r } }
    foreach ($r in @($NewRecovered)) { if ($r.ObjectKey) { $h[[string]$r.ObjectKey] = $r } }
    @($h.Values)
}

function Format-Section($Title, $Rows, [int]$Max = 40) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("")
    $lines.Add($Title)
    $list = @($Rows)
    if ($list.Count -eq 0) { $lines.Add("- None") ; return ($lines -join [Environment]::NewLine) }
    foreach ($r in ($list | Select-Object -First $Max)) {
        $rec = if ($r.RecoveredET) { " | Recovered: $($r.RecoveredET)" } else { "" }
        $lines.Add(("- {0} | {1} | {2} | {3} | {4} | LastFailed: {5}{6} | {7}" -f $r.Cluster, $r.Environment, $r.ProtectionGroup, $r.ObjectName, $r.RunType, $r.LastFailedET, $rec, $r.Message))
    }
    if ($list.Count -gt $Max) { $lines.Add("- ... $($list.Count - $Max) more rows in CSV") }
    ($lines -join [Environment]::NewLine)
}

function Write-WorkNotes($Path, $Incident, $Window, $Current, $Still, $New, $Recovered, $Refailed, $Unknown) {
    $clusters = @($Current | Select-Object -ExpandProperty Cluster -Unique | Where-Object { $_ })
    $envs = @($Current | Select-Object -ExpandProperty Environment -Unique | Where-Object { $_ })
    $txt = @"
Backup Failure Window Summary

Incident: $Incident
Locked Compute Window: $($Window.WindowLabel)
Generated At: $($Window.GeneratedET) ET
Source: Cohesity Helios API / PowerShell Window Consolidator

Summary:
- Current still failing now: $(@($Current).Count)
- Older failures still failing: $(@($Still).Count)
- New failures since previous check: $(@($New).Count)
- New recoveries since previous check: $(@($Recovered).Count)
- Re-failed after previous recovery: $(@($Refailed).Count)
- Unknown / needs review: $(@($Unknown).Count)
- Impacted clusters: $($clusters.Count)
- Impacted environments: $($envs -join ', ')

$(Format-Section "New Failures Since Previous Check:" $New)
$(Format-Section "Older Failures Still Failing:" $Still)
$(Format-Section "New Recoveries Since Previous Check:" $Recovered)
$(Format-Section "Re-Failed After Previous Recovery:" $Refailed)
$(Format-Section "Unknown / Needs Review:" $Unknown)

Evidence files in this folder:
- current_failures.csv
- recovered.csv
- new_failures.csv
- new_recoveries.csv
- worknotes.txt
- state.json

Note:
Script success means collection/consolidation succeeded. It does not mean backups are healthy.
"@
    $txt | Set-Content -Path $Path -Encoding UTF8
}

# ---------------- Main ----------------
$apiKey = Get-CohesityApiKey
$window = Get-ComputeWindow
$lock = Resolve-IncidentLock $window
$incident = $lock.IncidentNumber
$outputFolder = $lock.OutputFolder
if (!(Test-Path $outputFolder)) { New-Item -Path $outputFolder -ItemType Directory -Force | Out-Null }
$statePath = Join-Path $outputFolder "state.json"

$columns = "IncidentNumber","WindowKey","LifecycleStatus","Cluster","Environment","ProtectionGroup","SourceHostName","ObjectName","ObjectType","RunType","FirstFailedET","LastFailedET","RecoveredET","ConsecutiveFailureCount","Message","ObjectKey"
Write-Csv @() (Join-Path $outputFolder "current_failures.csv") $columns
Write-Csv @() (Join-Path $outputFolder "recovered.csv") $columns
Write-Csv @() (Join-Path $outputFolder "new_failures.csv") $columns
Write-Csv @() (Join-Path $outputFolder "new_recoveries.csv") $columns
"Backup failure consolidation started for $incident at $(Get-NowEtText) ET" | Set-Content (Join-Path $outputFolder "worknotes.txt") -Encoding UTF8

$clusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers @{ accept="application/json"; apiKey=$apiKey }
$clusters = @($clusterJson.cohesityClusters)
if ($ClusterName) {
    $clusters = @($clusters | Where-Object { (Get-ClusterName $_) -eq $ClusterName -or (Get-ClusterName $_) -like $ClusterName -or [string](Get-Prop $_ "clusterId" "") -eq $ClusterName })
    if ($clusters.Count -eq 0) { throw "Cluster not found: $ClusterName" }
}
if ($clusters.Count -eq 0) { throw "No Cohesity clusters returned from Helios." }

$currentRaw = @(Collect-CurrentUnclearedFailures -IncidentNumber $incident -Window $window -Clusters $clusters -ApiKey $apiKey)
$previousState = Read-Json $statePath
$previousOpen = @()
$previousRecovered = @()
if ($previousState) {
    $previousOpen = As-Array (Get-Prop $previousState "CurrentOpenFailures" @())
    $previousRecovered = As-Array (Get-Prop $previousState "AllRecovered" @())
}

$prevOpenByKey = Index-ByKey $previousOpen
$prevRecoveredByKey = Index-ByKey $previousRecovered
$currentByKey = Index-ByKey $currentRaw

$currentFinal = @()
$newFailures = @()
$stillFailing = @()
$refailed = @()

foreach ($row in $currentRaw) {
    $key = [string]$row.ObjectKey
    if ($prevOpenByKey.ContainsKey($key)) {
        $old = $prevOpenByKey[$key]
        $row.LifecycleStatus = "StillFailing"
        $row.FirstFailedET = $old.FirstFailedET
        $row.ConsecutiveFailureCount = [int](Get-Prop $old "ConsecutiveFailureCount" 1) + 1
        $stillFailing += $row
    } elseif ($prevRecoveredByKey.ContainsKey($key)) {
        $row.LifecycleStatus = "ReFailed"
        $row.ConsecutiveFailureCount = 1
        $refailed += $row
        $newFailures += $row
    } else {
        $row.LifecycleStatus = "NewlyFailedThisCheck"
        $row.ConsecutiveFailureCount = 1
        $newFailures += $row
    }
    $currentFinal += $row
}

$previousMissingNow = @($previousOpen | Where-Object { !$currentByKey.ContainsKey([string]$_.ObjectKey) })
$clusterById = @{}
foreach ($c in $clusters) { $clusterById[[string](Get-Prop $c "clusterId" "")] = $c }
$recoveryResult = Test-RecoveryForRows -Rows $previousMissingNow -ClustersById $clusterById -ApiKey $apiKey
$newRecoveries = @($recoveryResult.Recovered)
$unknown = @($recoveryResult.Unknown)
$allRecovered = @(Merge-Recovered $previousRecovered $newRecoveries)

Write-Csv $currentFinal (Join-Path $outputFolder "current_failures.csv") $columns
Write-Csv $allRecovered (Join-Path $outputFolder "recovered.csv") $columns
Write-Csv $newFailures (Join-Path $outputFolder "new_failures.csv") $columns
Write-Csv $newRecoveries (Join-Path $outputFolder "new_recoveries.csv") $columns
Write-WorkNotes -Path (Join-Path $outputFolder "worknotes.txt") -Incident $incident -Window $window -Current $currentFinal -Still $stillFailing -New $newFailures -Recovered $newRecoveries -Refailed $refailed -Unknown $unknown

$state = [pscustomobject]@{
    IncidentNumber = $incident
    WindowKey = $window.WindowKey
    WindowLabel = $window.WindowLabel
    WindowStartET = $window.WindowStartET
    WindowEndET = $window.WindowEndET
    LastRunET = Get-NowEtText
    ClusterFilter = $ClusterName
    CurrentOpenFailures = @($currentFinal)
    AllRecovered = @($allRecovered)
    LastRunNewFailures = @($newFailures)
    LastRunNewRecoveries = @($newRecoveries)
    LastRunUnknownNeedsReview = @($unknown)
}
Write-Json $state $statePath

Write-Host ""
Write-Host "Incident: $incident"
Write-Host "Window  : $($window.WindowLabel)"
Write-Host "Cluster : $(if ($ClusterName) { $ClusterName } else { 'All clusters' })"
Write-Host ""
Write-Host "Summary:"
Write-Host "Current Still Failing       : $(@($currentFinal).Count)"
Write-Host "Older Still Failing         : $(@($stillFailing).Count)"
Write-Host "New Failures This Check     : $(@($newFailures).Count)"
Write-Host "New Recoveries This Check   : $(@($newRecoveries).Count)"
Write-Host "Re-Failed This Check        : $(@($refailed).Count)"
Write-Host "Unknown / Needs Review      : $(@($unknown).Count)"
Write-Host ""
Write-Host "Files Created:"
Write-Host (Join-Path $outputFolder "current_failures.csv")
Write-Host (Join-Path $outputFolder "recovered.csv")
Write-Host (Join-Path $outputFolder "new_failures.csv")
Write-Host (Join-Path $outputFolder "new_recoveries.csv")
Write-Host (Join-Path $outputFolder "worknotes.txt")
Write-Host (Join-Path $outputFolder "state.json")
