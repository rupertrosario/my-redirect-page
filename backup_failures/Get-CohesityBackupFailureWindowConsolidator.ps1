<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
Single-script incident lifecycle workflow for backup failure windows.

The script keeps one incident folder per locked compute window. Every run captures the
current/latest uncleared backup failure position, compares it with state.json, tracks
old failures, marks failures cleared only by a later successful backup, and writes
paste-ready worknotes plus closure notes.

No Excel. No ServiceNow writes. Cohesity API calls are GET-only.
Authentication uses the existing AES helper/encrypted key method only.

Test one cluster:
  .\Get-CohesityBackupFailureWindowConsolidator.ps1 -ClusterName "CLUSTER_NAME"

All clusters:
  .\Get-CohesityBackupFailureWindowConsolidator.ps1

Optional first baseline from latest existing failure CSV:
  .\Get-CohesityBackupFailureWindowConsolidator.ps1 -UseLatestFailureCsv
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$LegacyFailureOutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailures",
    [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
    [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
    [string]$ClusterName = "",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = "",
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = "",
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35
)

$ErrorActionPreference = "Stop"
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:RetentionActions = New-Object System.Collections.Generic.List[string]

function Add-RunWarning([string]$Message) {
    $m = Clean $Message
    if ($m) {
        $script:Warnings.Add($m) | Out-Null
        Write-Warning $m
    }
}

function Add-RetentionAction([string]$Message) {
    $m = Clean $Message
    if ($m) { $script:RetentionActions.Add($m) | Out-Null }
}

function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Get-Prop($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
    if ($Object -is [hashtable]) {
        if ($Object.ContainsKey($Name)) { return $Object[$Name] }
        return $Default
    }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { return $p.Value }
    return $Default
}

function Set-ObjProp($Object, [string]$Name, $Value) {
    if ($null -eq $Object) { return }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { $Object.$Name = $Value }
    else { $Object | Add-Member -MemberType NoteProperty -Name $Name -Value $Value -Force }
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

function Get-NowEtDate {
    [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
}

function Get-NowEtText {
    (Get-NowEtDate).ToString("yyyy-MM-dd HH:mm:ss")
}

function Convert-UsecsToEtText($Usecs) {
    if ($null -eq $Usecs) { return "" }
    try {
        $u = [int64]$Usecs
        if ($u -le 0) { return "" }
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
    } catch {
        return ""
    }
}

function Convert-EtToUsecs([datetime]$EtDate) {
    $utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtDate, [DateTimeKind]::Unspecified), $script:EtZone)
    [int64](([DateTimeOffset]::new($utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000)
}

function Parse-EtTextToDate([string]$Text) {
    if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
    $formats = @("yyyy-MM-dd HH:mm:ss", "M/d/yyyy h:mm:ss tt", "M/d/yyyy H:mm:ss", "yyyy-MM-ddTHH:mm:ss")
    foreach ($fmt in $formats) {
        try { return [datetime]::ParseExact($Text.Trim(), $fmt, [Globalization.CultureInfo]::InvariantCulture) } catch {}
    }
    try { return [datetime]::Parse($Text) } catch { return $null }
}

function Convert-EtTextToUsecs([string]$Text) {
    $dt = Parse-EtTextToDate $Text
    if ($null -eq $dt) { return 0 }
    Convert-EtToUsecs $dt
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
    # Native 18:00 ET backup-failure window. If compute_window.js exists and emits
    # windowKey/windowLabel, use its labels for incident-window sync.
    $nowEt = Get-NowEtDate
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

function Get-RegistryPath {
    if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
    Join-Path $OutputRoot "BackupFailure_WindowRegistry.json"
}

function Get-WindowRegistry {
    $registryPath = Get-RegistryPath
    $registry = Read-Json $registryPath
    if (!$registry) {
        $registry = [pscustomobject]@{
            TimeZone = "America/New_York"
            WindowMode = "compute_window.js compatible"
            Windows = [pscustomobject]@{}
        }
    }
    if (!$registry.PSObject.Properties["Windows"]) {
        $registry | Add-Member -MemberType NoteProperty -Name "Windows" -Value ([pscustomobject]@{}) -Force
    }
    $registry
}

function Save-WindowRegistry($Registry) {
    Write-Json $Registry (Get-RegistryPath)
}

function Get-PreviousWindowEntry($Registry, [string]$CurrentWindowKey) {
    $entries = @()
    foreach ($p in $Registry.Windows.PSObject.Properties) {
        if ($p.Name -eq $CurrentWindowKey) { continue }
        $v = $p.Value
        $start = Clean (Get-Prop $v "WindowStartET" "")
        $entries += [pscustomobject]@{ Key=$p.Name; Entry=$v; SortKey=$start }
    }
    $latest = @($entries | Sort-Object SortKey -Descending | Select-Object -First 1)
    if ($latest.Count -eq 0) { return $null }
    $latest[0].Entry
}

function Resolve-IncidentLock($Window) {
    $registry = Get-WindowRegistry
    $existing = $registry.Windows.PSObject.Properties[$Window.WindowKey]
    if ($existing) {
        $entry = $existing.Value
        Set-ObjProp $entry "LastRunET" $Window.GeneratedET
        Save-WindowRegistry $registry
        return $entry
    }

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
    Save-WindowRegistry $registry
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
        [pscustomobject]@{ Label="RemoteAdapter"; Filter="kRemoteAdapter"; TargetObjectType="kRemoteAdapter";  ParentHostNeeded=$false; RunLimit=$NumRuns },
        [pscustomobject]@{ Label="Isilon";        Filter="kIsilon";        TargetObjectType="kHost";           ParentHostNeeded=$false; RunLimit=$NumRuns }
    )
}

function Get-FirstLocalBackupInfo($Run) {
    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    @(($Run.localBackupInfo))[0]
}

function Get-RunEffectiveUsecs($Run) {
    $i = Get-FirstLocalBackupInfo $Run
    if (!$i) { return 0 }
    $end = [int64](Get-Prop $i "endTimeUsecs" 0)
    if ($end -gt 0) { return $end }
    [int64](Get-Prop $i "startTimeUsecs" 0)
}

function Is-FailedStatus([string]$Status) {
    $Status -in @("Failed", "kFailed")
}

function Is-SuccessStatus([string]$Status) {
    $Status -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning")
}

function Is-RunningStatus([string]$Status) {
    $Status -in @("Running", "kRunning", "Accepted", "kAccepted", "Queued", "kQueued")
}

function Is-CancelledStatus([string]$Status) {
    $Status -in @("Canceled", "Cancelled", "kCanceled", "kCancelled", "Canceling", "kCanceling")
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

function Get-FailureMessage($Attempts) {
    $msgs = @()
    foreach ($a in @($Attempts)) {
        $m = Clean (Get-Prop $a "message" "")
        if ($m) { $msgs += $m }
    }
    ($msgs -join " | ")
}

function New-TrackingRow {
    param(
        [string]$IncidentNumber,
        $Window,
        $Cluster,
        $Env,
        $ProtectionGroup,
        [string]$ObjectKey,
        [string]$HostName,
        [string]$ObjectName,
        [string]$ObjectType,
        [string]$RunType,
        [int64]$StartUsecs,
        [int64]$EndUsecs,
        [string]$Message,
        [string]$Status = "NewlyFailedThisCheck",
        [string]$LatestRunStatus = "Failed",
        [int64]$LatestRunUsecs = 0,
        [string[]]$FailedRunKeys = @()
    )

    $effective = if ($EndUsecs -gt 0) { $EndUsecs } else { $StartUsecs }
    if ($LatestRunUsecs -le 0) { $LatestRunUsecs = $effective }

    [pscustomobject]@{
        IncidentNumber = $IncidentNumber
        WindowKey = $Window.WindowKey
        Status = $Status
        Cluster = Get-ClusterName $Cluster
        Environment = $Env.Label
        ProtectionGroup = Clean (Get-Prop $ProtectionGroup "name" "")
        Host = Clean $HostName
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = Convert-UsecsToEtText $effective
        LastFailedET = Convert-UsecsToEtText $effective
        LastFailedUsecs = $effective
        ClearedET = ""
        LastSeenET = Convert-UsecsToEtText $LatestRunUsecs
        LatestRunStatus = Clean $LatestRunStatus
        ConsecutiveFailureCount = 1
        Message = Clean $Message
        ObjectKey = $ObjectKey
        ClusterId = [string](Get-Prop $Cluster "clusterId" "")
        ProtectionGroupId = [string](Get-Prop $ProtectionGroup "id" "")
        EnvironmentFilter = $Env.Filter
        FailedRunKeys = @($FailedRunKeys)
    }
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
            Add-RunWarning "Protection group lookup failed for $(Get-ClusterName $Cluster) / $filter : $($_.Exception.Message)"
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

function Get-UsecsFromRunKey([string]$RunKey) {
    if ([string]::IsNullOrWhiteSpace($RunKey)) { return 0 }
    try {
        $parts = $RunKey -split "\|"
        return [int64]$parts[$parts.Count - 1]
    } catch {
        return 0
    }
}

function Update-RowConsecutiveFields($Row) {
    $keys = @(As-Array (Get-Prop $Row "FailedRunKeys" @()) | Where-Object { $_ } | Select-Object -Unique)
    Set-ObjProp $Row "FailedRunKeys" @($keys)
    Set-ObjProp $Row "ConsecutiveFailureCount" $keys.Count

    $times = @()
    foreach ($k in $keys) {
        $u = Get-UsecsFromRunKey $k
        if ($u -gt 0) { $times += $u }
    }
    if ($times.Count -gt 0) {
        $min = ($times | Measure-Object -Minimum).Minimum
        $max = ($times | Measure-Object -Maximum).Maximum
        Set-ObjProp $Row "FirstFailedET" (Convert-UsecsToEtText $min)
        Set-ObjProp $Row "LastFailedET" (Convert-UsecsToEtText $max)
        Set-ObjProp $Row "LastFailedUsecs" ([int64]$max)
    }
}

function New-RunLevelRow($IncidentNumber, $Window, $Cluster, $Env, $ProtectionGroup, $Info, [string]$Message, [string]$LatestRunStatus, [int64]$LatestRunUsecs) {
    $clusterId = [string](Get-Prop $Cluster "clusterId" "")
    $pgId = [string](Get-Prop $ProtectionGroup "id" "")
    $pgName = Clean (Get-Prop $ProtectionGroup "name" "")
    $runType = Clean (Get-Prop $Info "runType" "")
    $startUsecs = [int64](Get-Prop $Info "startTimeUsecs" 0)
    $endUsecs = [int64](Get-Prop $Info "endTimeUsecs" 0)
    $effective = if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs }
    $key = "$clusterId|$($Env.Label)|$pgId|RUNLEVEL|$runType|$pgName"
    $failedRunKey = "$clusterId|$pgId|$key|$runType|$effective"
    New-TrackingRow -IncidentNumber $IncidentNumber -Window $Window -Cluster $Cluster -Env $Env -ProtectionGroup $ProtectionGroup -ObjectKey $key -HostName "" -ObjectName $pgName -ObjectType "ProtectionGroup" -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $Message -LatestRunStatus $LatestRunStatus -LatestRunUsecs $LatestRunUsecs -FailedRunKeys @($failedRunKey)
}

function Collect-CurrentUnclearedFailures($IncidentNumber, $Window, $Clusters, [string]$ApiKey) {
    $rows = @()
    $clusterList = @($Clusters)
    $clusterTotal = $clusterList.Count
    $clusterIndex = 0

    foreach ($cluster in $clusterList) {
        $clusterIndex++
        $clusterName = Get-ClusterName $cluster
        Write-Host ("[{0}/{1}] Cluster: {2}" -f $clusterIndex, $clusterTotal, $clusterName)

        foreach ($env in (Get-EnvironmentMap)) {
            $before = $rows.Count
            $pgsChecked = 0
            $pgs = Get-ProtectionGroups -Cluster $cluster -Env $env -ApiKey $ApiKey

            foreach ($pg in $pgs) {
                $pgsChecked++
                $pgId = [string](Get-Prop $pg "id" "")
                $pgName = Clean (Get-Prop $pg "name" "")
                try {
                    $runs = Get-ProtectionGroupRuns -Cluster $cluster -ProtectionGroupId $pgId -RunLimit $env.RunLimit -ApiKey $ApiKey
                } catch {
                    Add-RunWarning "Runs lookup failed for $clusterName / $pgName : $($_.Exception.Message)"
                    continue
                }
                if ($runs.Count -eq 0) { continue }

                $runTypes = @($runs | ForEach-Object {
                    $i = Get-FirstLocalBackupInfo $_
                    if ($i) { Clean (Get-Prop $i "runType" "") }
                } | Where-Object { $_ } | Select-Object -Unique)

                foreach ($runType in $runTypes) {
                    $runsForType = @($runs | Where-Object {
                        $i = Get-FirstLocalBackupInfo $_
                        $i -and (Clean (Get-Prop $i "runType" "")) -eq $runType
                    } | Sort-Object { Get-RunEffectiveUsecs $_ } -Descending)

                    if ($runsForType.Count -eq 0) { continue }

                    $latestInfo = Get-FirstLocalBackupInfo $runsForType[0]
                    $latestRunStatus = Clean (Get-Prop $latestInfo "status" "")
                    $latestRunUsecs = Get-RunEffectiveUsecs $runsForType[0]

                    $cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $latestByKey = @{}
                    $failedKeysByKey = @{}
                    $objectNameById = Get-ObjectNameMap $runsForType
                    $isNas = $env.Label -in @("GenericNas", "Isilon")
                    $runLevelCleared = $false

                    foreach ($run in $runsForType) {
                        $info = Get-FirstLocalBackupInfo $run
                        if (!$info) { continue }
                        $status = Clean (Get-Prop $info "status" "")
                        $startUsecs = [int64](Get-Prop $info "startTimeUsecs" 0)
                        $endUsecs = [int64](Get-Prop $info "endTimeUsecs" 0)
                        $effectiveUsecs = if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs }
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
                            if (!$key -or $cleared.Contains($key)) { continue }

                            $runKey = "$([string](Get-Prop $cluster 'clusterId' ''))|$pgId|$key|$runType|$effectiveUsecs"
                            if (!$failedKeysByKey.ContainsKey($key)) { $failedKeysByKey[$key] = New-Object 'System.Collections.Generic.HashSet[string]' }
                            [void]$failedKeysByKey[$key].Add($runKey)

                            if (!$latestByKey.ContainsKey($key)) {
                                $message = Get-FailureMessage $attempts
                                if (!$message) { $message = "Object failed; failedAttempts did not include a message" }
                                $hostName = if ($env.ParentHostNeeded) { Resolve-HostName $ro $objectNameById } else { "" }
                                if (!$hostName -and $objType -eq "kHost") { $hostName = Clean (Get-Prop $obj "name" "") }
                                $objName = Clean (Get-Prop $obj "name" "")
                                if (!$objName) { $objName = $pgName }

                                $rowStatus = "NewlyFailedThisCheck"
                                if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "RunningAtLatestCheck" }
                                elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "CancelledAfterFailure" }

                                $latestByKey[$key] = New-TrackingRow -IncidentNumber $IncidentNumber -Window $Window -Cluster $cluster -Env $env -ProtectionGroup $pg -ObjectKey $key -HostName $hostName -ObjectName $objName -ObjectType $objType -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $message -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                            $foundObjectFailure = $true
                        }

                        if (!$foundObjectFailure -and !$runLevelCleared) {
                            $msg = Clean (Get-Prop $info "messages" "")
                            if (!$msg) { $msg = "Run marked failed; no object-level failedAttempts returned" }
                            $rl = New-RunLevelRow $IncidentNumber $Window $cluster $env $pg $info $msg $latestRunStatus $latestRunUsecs
                            if (!$cleared.Contains($rl.ObjectKey)) {
                                if (!$failedKeysByKey.ContainsKey($rl.ObjectKey)) { $failedKeysByKey[$rl.ObjectKey] = New-Object 'System.Collections.Generic.HashSet[string]' }
                                foreach ($rk in (As-Array $rl.FailedRunKeys)) { [void]$failedKeysByKey[$rl.ObjectKey].Add($rk) }
                                if (!$latestByKey.ContainsKey($rl.ObjectKey)) {
                                    if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt [int64]$rl.LastFailedUsecs) { $rl.Status = "RunningAtLatestCheck" }
                                    elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt [int64]$rl.LastFailedUsecs) { $rl.Status = "CancelledAfterFailure" }
                                    $latestByKey[$rl.ObjectKey] = $rl
                                }
                            }
                        }
                    }

                    foreach ($k in $latestByKey.Keys) {
                        if ($failedKeysByKey.ContainsKey($k)) { Set-ObjProp $latestByKey[$k] "FailedRunKeys" @($failedKeysByKey[$k]) }
                        Update-RowConsecutiveFields $latestByKey[$k]
                        $rows += $latestByKey[$k]
                    }
                }
            }

            $envFailures = $rows.Count - $before
            Write-Host ("  {0,-13}: PGs checked: {1} | failures: {2}" -f $env.Label, $pgsChecked, $envFailures)
        }
    }

    @($rows | Group-Object ObjectKey | ForEach-Object { $_.Group | Sort-Object LastFailedUsecs -Descending | Select-Object -First 1 })
}

function Clone-Row($Row) {
    if ($null -eq $Row) { return $null }
    $Row | Select-Object *
}

function Normalize-ExistingRows($Rows, [string]$Incident, $Window) {
    $out = @()
    foreach ($r in @($Rows)) {
        $n = Clone-Row $r
        if ($null -eq $n) { continue }
        $status = Clean (Get-Prop $n "Status" "")
        if (!$status) { $status = Clean (Get-Prop $n "LifecycleStatus" "") }
        if (!$status) { $status = "UnknownNeedsReview" }
        Set-ObjProp $n "Status" $status

        $clearedEt = Clean (Get-Prop $n "ClearedET" "")
        if (!$clearedEt) { $clearedEt = Clean (Get-Prop $n "RecoveredET" "") }
        Set-ObjProp $n "ClearedET" $clearedEt

        $hostName = Clean (Get-Prop $n "Host" "")
        if (!$hostName) { $hostName = Clean (Get-Prop $n "SourceHostName" "") }
        Set-ObjProp $n "Host" $hostName

        if (!(Get-Prop $n "IncidentNumber" "")) { Set-ObjProp $n "IncidentNumber" $Incident }
        if (!(Get-Prop $n "WindowKey" "")) { Set-ObjProp $n "WindowKey" $Window.WindowKey }
        if (!(Get-Prop $n "LatestRunStatus" "")) { Set-ObjProp $n "LatestRunStatus" "" }
        if (!(Get-Prop $n "LastSeenET" "")) { Set-ObjProp $n "LastSeenET" (Clean (Get-Prop $n "LastFailedET" "")) }
        if (!(Get-Prop $n "FailedRunKeys" $null)) { Set-ObjProp $n "FailedRunKeys" @() }
        if (!(Get-Prop $n "ConsecutiveFailureCount" $null)) { Set-ObjProp $n "ConsecutiveFailureCount" 1 }
        if (!(Get-Prop $n "LastFailedUsecs" $null)) { Set-ObjProp $n "LastFailedUsecs" (Convert-EtTextToUsecs (Clean (Get-Prop $n "LastFailedET" ""))) }
        $out += $n
    }
    @($out)
}

function Index-ByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = [string](Get-Prop $r "ObjectKey" "")
        if ($k) { $h[$k] = $r }
    }
    $h
}

function Merge-UniqueRowsByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = [string](Get-Prop $r "ObjectKey" "")
        if (!$k) { continue }
        if (!$h.ContainsKey($k)) { $h[$k] = $r }
        else {
            $oldUsecs = [int64](Get-Prop $h[$k] "LastFailedUsecs" 0)
            $newUsecs = [int64](Get-Prop $r "LastFailedUsecs" 0)
            if ($newUsecs -ge $oldUsecs) { $h[$k] = $r }
        }
    }
    @($h.Values)
}

function Merge-FailedRunKeys($ExistingRow, $NewRow) {
    $keys = @()
    if ($ExistingRow) { $keys += As-Array (Get-Prop $ExistingRow "FailedRunKeys" @()) }
    if ($NewRow) { $keys += As-Array (Get-Prop $NewRow "FailedRunKeys" @()) }
    @($keys | Where-Object { $_ } | Select-Object -Unique)
}

function Test-ClearanceForRows($Rows, $ClustersById, [string]$ApiKey) {
    $cleared = @()
    $running = @()
    $cancelled = @()
    $unknown = @()

    foreach ($group in (@($Rows) | Group-Object ClusterId, ProtectionGroupId)) {
        $sample = $group.Group[0]
        $clusterId = [string](Get-Prop $sample "ClusterId" "")
        $pgId = [string](Get-Prop $sample "ProtectionGroupId" "")
        $cluster = $null
        if ($ClustersById.ContainsKey($clusterId)) { $cluster = $ClustersById[$clusterId] }
        if (!$cluster -or !$pgId) {
            foreach ($r in $group.Group) {
                $u = Clone-Row $r
                Set-ObjProp $u "Status" "UnknownNeedsReview"
                Set-ObjProp $u "Message" "$((Clean (Get-Prop $u 'Message' ''))) | Unable to verify clearance because ClusterId/ProtectionGroupId is unavailable."
                $unknown += $u
            }
            continue
        }

        try {
            $runs = Get-ProtectionGroupRuns -Cluster $cluster -ProtectionGroupId $pgId -RunLimit $NumRuns -ApiKey $ApiKey
        } catch {
            foreach ($r in $group.Group) {
                $u = Clone-Row $r
                Set-ObjProp $u "Status" "UnknownNeedsReview"
                Set-ObjProp $u "Message" "$((Clean (Get-Prop $u 'Message' ''))) | Unable to verify clearance: $($_.Exception.Message)"
                $unknown += $u
            }
            continue
        }

        foreach ($row in $group.Group) {
            $resolved = $false
            $runsForType = @($runs | Where-Object {
                $i = Get-FirstLocalBackupInfo $_
                $i -and (Clean (Get-Prop $i "runType" "")) -eq (Clean (Get-Prop $row "RunType" ""))
            } | Sort-Object { Get-RunEffectiveUsecs $_ } -Descending)

            foreach ($run in $runsForType) {
                $info = Get-FirstLocalBackupInfo $run
                if (!$info) { continue }
                $effectiveUsecs = Get-RunEffectiveUsecs $run
                if ($effectiveUsecs -le [int64](Get-Prop $row "LastFailedUsecs" 0)) { continue }

                $status = Clean (Get-Prop $info "status" "")
                if (Is-RunningStatus $status) {
                    $n = Clone-Row $row
                    Set-ObjProp $n "Status" "RunningAtLatestCheck"
                    Set-ObjProp $n "LatestRunStatus" $status
                    Set-ObjProp $n "LastSeenET" (Convert-UsecsToEtText $effectiveUsecs)
                    $running += $n
                    $resolved = $true
                    break
                }

                if (Is-CancelledStatus $status) {
                    $n = Clone-Row $row
                    Set-ObjProp $n "Status" "CancelledAfterFailure"
                    Set-ObjProp $n "LatestRunStatus" $status
                    Set-ObjProp $n "LastSeenET" (Convert-UsecsToEtText $effectiveUsecs)
                    $cancelled += $n
                    $resolved = $true
                    break
                }

                if (Is-SuccessStatus $status) {
                    if ((Clean (Get-Prop $row "ObjectType" "")) -eq "ProtectionGroup") {
                        $n = Clone-Row $row
                        Set-ObjProp $n "Status" "NewlyClearedThisCheck"
                        Set-ObjProp $n "ClearedET" (Convert-UsecsToEtText $effectiveUsecs)
                        Set-ObjProp $n "LatestRunStatus" $status
                        Set-ObjProp $n "LastSeenET" (Convert-UsecsToEtText $effectiveUsecs)
                        $cleared += $n
                        $resolved = $true
                        break
                    }

                    foreach ($ro in (As-Array (Get-Prop $run "objects" @()))) {
                        if (!(Is-SuccessObject $ro)) { continue }
                        $candidateKey = Get-RunObjectKey -RunObject $ro -ClusterId $clusterId -EnvironmentLabel (Clean (Get-Prop $row "Environment" "")) -ProtectionGroupId $pgId -ProtectionGroupName (Clean (Get-Prop $row "ProtectionGroup" ""))
                        if ($candidateKey -eq (Clean (Get-Prop $row "ObjectKey" ""))) {
                            $n = Clone-Row $row
                            Set-ObjProp $n "Status" "NewlyClearedThisCheck"
                            Set-ObjProp $n "ClearedET" (Convert-UsecsToEtText $effectiveUsecs)
                            Set-ObjProp $n "LatestRunStatus" $status
                            Set-ObjProp $n "LastSeenET" (Convert-UsecsToEtText $effectiveUsecs)
                            $cleared += $n
                            $resolved = $true
                            break
                        }
                    }
                    if ($resolved) { break }
                }
            }

            if (!$resolved) {
                $n = Clone-Row $row
                Set-ObjProp $n "Status" "UnknownNeedsReview"
                $unknown += $n
            }
        }
    }

    [pscustomobject]@{
        Cleared = @($cleared)
        Running = @($running)
        Cancelled = @($cancelled)
        Unknown = @($unknown)
    }
}

function Get-LatestLegacyFailureCsv {
    if ($LegacyFailureCsvPath -and (Test-Path $LegacyFailureCsvPath)) { return $LegacyFailureCsvPath }
    if (!(Test-Path $LegacyFailureOutputRoot)) { return "" }
    $latest = Get-ChildItem -Path $LegacyFailureOutputRoot -Filter "BackupFailures_AllEnvironments_*.csv" -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($latest) { return $latest.FullName }
    ""
}

function Import-LegacyFailureCsvRows([string]$Path, [string]$Incident, $Window) {
    $rows = @()
    if (!(Test-Path $Path)) { return @() }

    $csvRows = @(Import-Csv -Path $Path)
    foreach ($r in $csvRows) {
        $cluster = Clean (Get-Prop $r "Cluster" "")
        $env = Clean (Get-Prop $r "Environment" "")
        $pg = Clean (Get-Prop $r "ProtectionGroup" "")
        $hostName = Clean (Get-Prop $r "Host" "")
        $objName = Clean (Get-Prop $r "ObjectName" "")
        if (!$objName) { $objName = Clean (Get-Prop $r "DatabaseName" "") }
        $runType = Clean (Get-Prop $r "RunType" "")
        $endTime = Clean (Get-Prop $r "EndTime" "")
        $msg = Clean (Get-Prop $r "FailedMessage" "")
        $key = "LEGACY|$cluster|$env|$pg|$hostName|$objName|$runType"
        $usecs = Convert-EtTextToUsecs $endTime
        $failedRunKey = "LEGACY|$key|$runType|$usecs"

        $row = [pscustomobject]@{
            IncidentNumber = $Incident
            WindowKey = $Window.WindowKey
            Status = "NewlyFailedThisCheck"
            Cluster = $cluster
            Environment = $env
            ProtectionGroup = $pg
            Host = $hostName
            ObjectName = $objName
            ObjectType = ""
            RunType = $runType
            FirstFailedET = $endTime
            LastFailedET = $endTime
            LastFailedUsecs = $usecs
            ClearedET = ""
            LastSeenET = $endTime
            LatestRunStatus = "Failed"
            ConsecutiveFailureCount = 1
            Message = $msg
            ObjectKey = $key
            ClusterId = ""
            ProtectionGroupId = ""
            EnvironmentFilter = ""
            FailedRunKeys = @($failedRunKey)
        }
        $rows += $row
    }

    @($rows | Group-Object ObjectKey | ForEach-Object { $_.Group | Sort-Object LastFailedUsecs -Descending | Select-Object -First 1 })
}

function Format-Section($Title, $Rows, [int]$Max = 25) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("")
    $lines.Add($Title)
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add("- None")
        return ($lines -join [Environment]::NewLine)
    }
    foreach ($r in ($list | Sort-Object LastFailedET -Descending | Select-Object -First $Max)) {
        $cleared = ""
        if (Clean (Get-Prop $r "ClearedET" "")) { $cleared = " | Cleared: $($r.ClearedET)" }
        $lines.Add(("- {0} | {1} | {2} | {3} | {4} | Status: {5} | LastFailed: {6}{7} | Count: {8} | {9}" -f $r.Cluster, $r.Environment, $r.ProtectionGroup, $r.ObjectName, $r.RunType, $r.Status, $r.LastFailedET, $cleared, $r.ConsecutiveFailureCount, $r.Message))
    }
    if ($list.Count -gt $Max) { $lines.Add("- ... $($list.Count - $Max) more rows in CSV") }
    ($lines -join [Environment]::NewLine)
}

function Get-SummaryCounts($Current, $New, $ClearedThisRun, $Refailed, $Running, $Cancelled, $Unknown) {
    [pscustomobject]@{
        CurrentActive = @($Current).Count
        NewFailures = @($New).Count
        ClearedThisRun = @($ClearedThisRun).Count
        Refailed = @($Refailed).Count
        Running = @($Running).Count
        Cancelled = @($Cancelled).Count
        Unknown = @($Unknown).Count
        ConsecutiveActive = @($Current | Where-Object { [int](Get-Prop $_ "ConsecutiveFailureCount" 0) -gt 1 }).Count
    }
}

function Write-WorkNotes($Path, $Incident, $Window, [string]$OutputFolder, $Current, $New, $ClearedThisRun, $Refailed, $Running, $Cancelled, $Unknown) {
    $clusters = @($Current | Select-Object -ExpandProperty Cluster -Unique | Where-Object { $_ })
    $envs = @($Current | Select-Object -ExpandProperty Environment -Unique | Where-Object { $_ })
    $counts = Get-SummaryCounts $Current $New $ClearedThisRun $Refailed $Running $Cancelled $Unknown

    $warningLine = ""
    if ($script:Warnings.Count -gt 0) { $warningLine = "- Report completed with warnings: $($script:Warnings.Count). See summary.txt." }

    $txt = @"
Backup Failure Window Update

Incident: $Incident
Compute Window: $($Window.WindowLabel)
Generated At: $($Window.GeneratedET) ET
Evidence Folder: $OutputFolder

Summary:
- Current active/unresolved failures: $($counts.CurrentActive)
- New failures since previous check: $($counts.NewFailures)
- Failures cleared by later successful backup since previous check: $($counts.ClearedThisRun)
- Re-failed after earlier clear: $($counts.Refailed)
- Consecutive/repeated active failures: $($counts.ConsecutiveActive)
- Running / awaiting completion: $($counts.Running)
- Cancelled after failure: $($counts.Cancelled)
- Unknown / needs review: $($counts.Unknown)
- Impacted clusters: $($clusters.Count)
- Impacted environments: $($envs -join ', ')
- Scope: latest $NumRuns runs per protection group/run type.
$warningLine

$(Format-Section "New Failures Since Previous Check:" $New 15)
$(Format-Section "Failures Cleared By Later Successful Backup Since Previous Check:" $ClearedThisRun 15)
$(Format-Section "Re-Failed After Earlier Clear:" $Refailed 15)
$(Format-Section "Running / Awaiting Completion:" $Running 15)
$(Format-Section "Cancelled / Not Cleared:" $Cancelled 15)
$(Format-Section "Unknown / Needs Review:" $Unknown 15)

Current active failure list:
See current_failures.csv

Evidence files:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
- worknotes.txt
- summary.txt
- closing_summary.txt
- state.json
"@
    $txt | Set-Content -Path $Path -Encoding UTF8
}

function Write-SummaryFile($Path, $Incident, $Window, [string]$OutputFolder, $Current, $AllCleared, $Lifecycle, $New, $ClearedThisRun, $Refailed, $Running, $Cancelled, $Unknown) {
    $counts = Get-SummaryCounts $Current $New $ClearedThisRun $Refailed $Running $Cancelled $Unknown
    $clearTotal = @($AllCleared).Count

    $carryForwardText = ""
    if (@($Current).Count -eq 0) {
        $carryForwardText = "No active backup failures remain based on the latest check."
    } else {
        $carryForwardText = "$(@($Current).Count) objects remain active/unresolved and require follow-up tracking. See current_failures.csv."
    }

    $warningsText = ""
    if ($script:Warnings.Count -gt 0) {
        $warningsText = "`r`nWarnings:`r`n" + (($script:Warnings | ForEach-Object { "- $_" }) -join "`r`n")
    } else {
        $warningsText = "`r`nWarnings:`r`n- None"
    }

    $retentionText = ""
    if ($script:RetentionActions.Count -gt 0) {
        $retentionText = "`r`nRetention Actions:`r`n" + (($script:RetentionActions | ForEach-Object { "- $_" }) -join "`r`n")
    } else {
        $retentionText = "`r`nRetention Actions:`r`n- None"
    }

    $txt = @"
Backup Failure Incident Summary

Incident: $Incident
Compute Window: $($Window.WindowLabel)
Last Updated: $($Window.GeneratedET) ET
Evidence Folder: $OutputFolder

Current State:
- Current active/unresolved failures: $($counts.CurrentActive)
- Total failures cleared by later successful backup: $clearTotal
- New failures since previous check: $($counts.NewFailures)
- Failures cleared by later successful backup since previous check: $($counts.ClearedThisRun)
- Re-failed after earlier clear: $($counts.Refailed)
- Consecutive/repeated active failures: $($counts.ConsecutiveActive)
- Running / awaiting completion: $($counts.Running)
- Cancelled after failure: $($counts.Cancelled)
- Unknown / needs review: $($counts.Unknown)
- Total lifecycle rows tracked: $(@($Lifecycle).Count)

Carry Forward / Handoff:
$carryForwardText

$(Format-Section "Current Active / Unresolved Failures:" $Current 60)
$(Format-Section "Failures Cleared By Later Successful Backup:" $AllCleared 60)
$(Format-Section "Running / Awaiting Completion:" $Running 40)
$(Format-Section "Cancelled / Not Cleared:" $Cancelled 40)
$(Format-Section "Unknown / Needs Review:" $Unknown 40)

Scope / Limitations:
- Cohesity run evaluation is limited to the latest $NumRuns runs per protection group/run type.
- Only Succeeded or SucceededWithWarning clears a previous failure.
- Running does not clear a previous failure.
- Cancelled/Canceled does not clear a previous failure.
- Missing from the current scan is not treated as cleared unless a later successful backup is verified.
- This is incident lifecycle tracking for observed/latest uncleared failures, not an audit-grade history of every failure event.

Evidence Files:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
- worknotes.txt
- summary.txt
- closing_summary.txt
- state.json
$warningsText
$retentionText
"@
    $txt | Set-Content -Path $Path -Encoding UTF8
}

function Write-ClosingSummaryFile($Path, $Incident, $WindowLabel, [string]$GeneratedEt, [string]$OutputFolder, $Current, $AllCleared, $Lifecycle, $Running, $Cancelled, $Unknown) {
    $carry = ""
    if (@($Current).Count -eq 0) { $carry = "No active backup failures remain based on the latest saved state." }
    else { $carry = "$(@($Current).Count) active/unresolved objects remain and should be carried forward or separately tracked." }

    $txt = @"
Backup Failure Incident Closure Summary

Incident: $Incident
Compute Window: $WindowLabel
Generated At: $GeneratedEt ET
Evidence Folder: $OutputFolder

Closure State:
- Active/unresolved failures: $(@($Current).Count)
- Cleared by later successful backup: $(@($AllCleared).Count)
- Running / awaiting completion: $(@($Running).Count)
- Cancelled after failure: $(@($Cancelled).Count)
- Unknown / needs review: $(@($Unknown).Count)
- Total lifecycle rows tracked: $(@($Lifecycle).Count)

Carry Forward / Handoff:
$carry

$(Format-Section "Active / Unresolved Failures:" $Current 80)
$(Format-Section "Running / Awaiting Completion:" $Running 40)
$(Format-Section "Cancelled / Not Cleared:" $Cancelled 40)
$(Format-Section "Unknown / Needs Review:" $Unknown 40)

Scope / Limitations:
- Evaluation is limited to the latest $NumRuns runs per protection group/run type.
- Only a later Succeeded or SucceededWithWarning backup clears a failure.
- Running and cancelled runs remain unresolved.
- This closure summary is generated from the incident state/files; it does not perform a new Cohesity scan.

Evidence Files:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
- worknotes.txt
- summary.txt
- closing_summary.txt
- state.json
"@
    $txt | Set-Content -Path $Path -Encoding UTF8
}

function Write-ClosingSummaryFromExistingState([string]$Folder) {
    if ([string]::IsNullOrWhiteSpace($Folder) -or !(Test-Path $Folder)) { return }
    $statePath = Join-Path $Folder "state.json"
    $state = Read-Json $statePath
    if (!$state) { return }

    $incident = Clean (Get-Prop $state "IncidentNumber" (Split-Path $Folder -Leaf))
    $windowLabel = Clean (Get-Prop $state "WindowLabel" "")
    $windowObj = [pscustomobject]@{ WindowKey = Clean (Get-Prop $state "WindowKey" "") }
    $current = Normalize-ExistingRows (As-Array (Get-Prop $state "CurrentOpenFailures" @())) $incident $windowObj
    $cleared = Normalize-ExistingRows (As-Array (Get-Prop $state "ClearedBySuccess" (Get-Prop $state "AllRecovered" @()))) $incident $windowObj
    $lifecycle = Normalize-ExistingRows (As-Array (Get-Prop $state "LifecycleRows" @())) $incident $windowObj
    if ($lifecycle.Count -eq 0) { $lifecycle = @($current + $cleared) }

    $running = @($current | Where-Object { (Clean (Get-Prop $_ "Status" "")) -eq "RunningAtLatestCheck" })
    $cancelled = @($current | Where-Object { (Clean (Get-Prop $_ "Status" "")) -eq "CancelledAfterFailure" })
    $unknown = @($current | Where-Object { (Clean (Get-Prop $_ "Status" "")) -eq "UnknownNeedsReview" })

    Write-ClosingSummaryFile -Path (Join-Path $Folder "closing_summary.txt") -Incident $incident -WindowLabel $windowLabel -GeneratedEt (Get-NowEtText) -OutputFolder $Folder -Current $current -AllCleared $cleared -Lifecycle $lifecycle -Running $running -Cancelled $cancelled -Unknown $unknown
}

function Invoke-OutputRetention([string]$ActiveOutputFolder) {
    if (!(Test-Path $OutputRoot)) { return }

    $archiveRoot = Join-Path $OutputRoot "Archive"
    if (!(Test-Path $archiveRoot)) { New-Item -Path $archiveRoot -ItemType Directory -Force | Out-Null }

    $now = Get-Date
    $activeFull = ""
    try { $activeFull = (Resolve-Path $ActiveOutputFolder -ErrorAction SilentlyContinue).Path } catch {}

    $folders = Get-ChildItem -Path $OutputRoot -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -ne "Archive" }
    foreach ($folder in $folders) {
        $folderFull = $folder.FullName
        if ($activeFull -and $folderFull -eq $activeFull) { continue }

        $ageDate = $folder.LastWriteTime
        $state = Read-Json (Join-Path $folderFull "state.json")
        if ($state) {
            $dt = Parse-EtTextToDate (Clean (Get-Prop $state "WindowEndET" ""))
            if ($dt) { $ageDate = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($dt, [DateTimeKind]::Unspecified), $script:EtZone).ToLocalTime() }
        }
        $ageDays = ($now - $ageDate).TotalDays

        if ($ageDays -gt $ArchiveFoldersUntilDays) {
            try {
                Remove-Item -Path $folderFull -Recurse -Force
                Add-RetentionAction "Deleted old folder older than $ArchiveFoldersUntilDays days: $folderFull"
            } catch {
                Add-RunWarning "Failed to delete old folder $folderFull : $($_.Exception.Message)"
            }
            continue
        }

        if ($ageDays -gt $KeepFoldersDays) {
            $windowKey = ""
            if ($state) { $windowKey = Clean (Get-Prop $state "WindowKey" "") }
            $zipName = if ($windowKey) { "$($folder.Name)_$windowKey.zip" } else { "$($folder.Name).zip" }
            $zipPath = Join-Path $archiveRoot $zipName
            if (Test-Path $zipPath) {
                try {
                    Remove-Item -Path $folderFull -Recurse -Force
                    Add-RetentionAction "Removed folder already archived: $folderFull"
                } catch {
                    Add-RunWarning "Archive exists but failed to remove folder $folderFull : $($_.Exception.Message)"
                }
            } else {
                try {
                    Compress-Archive -Path (Join-Path $folderFull "*") -DestinationPath $zipPath -Force
                    if (Test-Path $zipPath) {
                        Remove-Item -Path $folderFull -Recurse -Force
                        Add-RetentionAction "Archived folder to $zipPath and removed original folder $folderFull"
                    } else {
                        Add-RunWarning "Zip was not created for $folderFull; folder retained."
                    }
                } catch {
                    Add-RunWarning "Failed to archive folder $folderFull : $($_.Exception.Message)"
                }
            }
        }
    }

    $zips = Get-ChildItem -Path $archiveRoot -Filter "*.zip" -File -ErrorAction SilentlyContinue
    foreach ($zip in $zips) {
        $ageDays = ($now - $zip.LastWriteTime).TotalDays
        if ($ageDays -gt $ArchiveFoldersUntilDays) {
            try {
                Remove-Item -Path $zip.FullName -Force
                Add-RetentionAction "Deleted old archive older than $ArchiveFoldersUntilDays days: $($zip.FullName)"
            } catch {
                Add-RunWarning "Failed to delete old archive $($zip.FullName) : $($_.Exception.Message)"
            }
        }
    }
}

# ---------------- Main ----------------

if ($NumRuns -ne 30) {
    Write-Warning "NumRuns was set to $NumRuns. Operational standard is 30; reports will call out the supplied value."
}

$window = Get-ComputeWindow
$registryBefore = Get-WindowRegistry
$currentWindowExists = $null -ne $registryBefore.Windows.PSObject.Properties[$window.WindowKey]
if (!$currentWindowExists) {
    $previousEntry = Get-PreviousWindowEntry -Registry $registryBefore -CurrentWindowKey $window.WindowKey
    if ($previousEntry) {
        $previousFolder = Clean (Get-Prop $previousEntry "OutputFolder" "")
        if ($previousFolder -and (Test-Path $previousFolder)) {
            Write-Host "Previous window detected. Refreshing previous incident closure summary..."
            Write-ClosingSummaryFromExistingState -Folder $previousFolder
            Write-Host "Previous closure summary: $(Join-Path $previousFolder 'closing_summary.txt')"
        }
    }
}

$lock = Resolve-IncidentLock $window
$incident = $lock.IncidentNumber
$outputFolder = $lock.OutputFolder
if (!(Test-Path $outputFolder)) { New-Item -Path $outputFolder -ItemType Directory -Force | Out-Null }
$statePath = Join-Path $outputFolder "state.json"

Write-Host ""
Write-Host "Incident : $incident"
Write-Host "Window   : $($window.WindowLabel)"
Write-Host "Output   : $outputFolder"
Write-Host "Scope    : latest $NumRuns runs per protection group/run type"
Write-Host ""

$csvColumns = "IncidentNumber","WindowKey","Status","Cluster","Environment","ProtectionGroup","Host","ObjectName","ObjectType","RunType","FirstFailedET","LastFailedET","ClearedET","LastSeenET","LatestRunStatus","ConsecutiveFailureCount","Message","ObjectKey"
Write-Csv @() (Join-Path $outputFolder "current_failures.csv") $csvColumns
Write-Csv @() (Join-Path $outputFolder "cleared_by_success.csv") $csvColumns
Write-Csv @() (Join-Path $outputFolder "incident_lifecycle.csv") $csvColumns

$previousState = Read-Json $statePath
$isFirstRunForIncident = $null -eq $previousState

$carryForwardRows = @()
if ($isFirstRunForIncident) {
    $registryForCarry = Get-WindowRegistry
    $previousEntryForCarry = Get-PreviousWindowEntry -Registry $registryForCarry -CurrentWindowKey $window.WindowKey
    if ($previousEntryForCarry) {
        $previousStatePath = Join-Path (Clean (Get-Prop $previousEntryForCarry "OutputFolder" "")) "state.json"
        $previousIncidentState = Read-Json $previousStatePath
        if ($previousIncidentState) {
            $carryForwardRows = Normalize-ExistingRows (As-Array (Get-Prop $previousIncidentState "CurrentOpenFailures" @())) $incident $window
        }
    }
}

$apiKey = ""
$clusters = @()
$currentRaw = @()
$usedLegacyCsv = ""

if ($UseLatestFailureCsv -and $isFirstRunForIncident) {
    $legacyCsv = Get-LatestLegacyFailureCsv
    if ($legacyCsv) {
        Write-Host "Using legacy failure CSV as first baseline:"
        Write-Host $legacyCsv
        $usedLegacyCsv = $legacyCsv
        $currentRaw = @(Import-LegacyFailureCsvRows -Path $legacyCsv -Incident $incident -Window $window)
    } else {
        Add-RunWarning "UseLatestFailureCsv was specified, but no legacy failure CSV was found. Falling back to live Cohesity scan."
    }
}

if ($currentRaw.Count -eq 0) {
    $apiKey = Get-CohesityApiKey
    $clusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers @{ accept="application/json"; apiKey=$apiKey }
    $clusters = @($clusterJson.cohesityClusters)
    if ($ClusterName) {
        $clusters = @($clusters | Where-Object { (Get-ClusterName $_) -eq $ClusterName -or (Get-ClusterName $_) -like $ClusterName -or [string](Get-Prop $_ "clusterId" "") -eq $ClusterName })
        if ($clusters.Count -eq 0) { throw "Cluster not found: $ClusterName" }
    }
    if ($clusters.Count -eq 0) { throw "No Cohesity clusters returned from Helios." }

    Write-Host "Starting Cohesity scan..."
    Write-Host ""
    $currentRaw = @(Collect-CurrentUnclearedFailures -IncidentNumber $incident -Window $window -Clusters $clusters -ApiKey $apiKey)
}

$previousOpen = @()
$previousCleared = @()
$previousLifecycle = @()
if ($previousState) {
    $previousOpen = Normalize-ExistingRows (As-Array (Get-Prop $previousState "CurrentOpenFailures" @())) $incident $window
    $previousCleared = Normalize-ExistingRows (As-Array (Get-Prop $previousState "ClearedBySuccess" (Get-Prop $previousState "AllRecovered" @()))) $incident $window
    $previousLifecycle = Normalize-ExistingRows (As-Array (Get-Prop $previousState "LifecycleRows" @())) $incident $window
}

$prevOpenByKey = Index-ByKey $previousOpen
$prevClearedByKey = Index-ByKey $previousCleared
$carryForwardByKey = Index-ByKey $carryForwardRows
$currentByKey = Index-ByKey $currentRaw

$currentFinal = @()
$newFailures = @()
$refailed = @()
$runningCurrent = @()
$cancelledCurrent = @()

foreach ($row in $currentRaw) {
    $key = [string](Get-Prop $row "ObjectKey" "")
    $baseStatus = Clean (Get-Prop $row "Status" "")

    if ($prevOpenByKey.ContainsKey($key)) {
        $old = $prevOpenByKey[$key]
        $mergedKeys = Merge-FailedRunKeys $old $row
        Set-ObjProp $row "FailedRunKeys" @($mergedKeys)
        Update-RowConsecutiveFields $row
        Set-ObjProp $row "FirstFailedET" (Clean (Get-Prop $old "FirstFailedET" (Get-Prop $row "FirstFailedET" "")))
        if ($baseStatus -notin @("RunningAtLatestCheck","CancelledAfterFailure")) { Set-ObjProp $row "Status" "OlderStillFailing" }
    } elseif ($prevClearedByKey.ContainsKey($key)) {
        Set-ObjProp $row "Status" "ReFailedAfterClear"
        Update-RowConsecutiveFields $row
        $refailed += $row
        $newFailures += $row
    } elseif ($carryForwardByKey.ContainsKey($key)) {
        $old = $carryForwardByKey[$key]
        $mergedKeys = Merge-FailedRunKeys $old $row
        Set-ObjProp $row "FailedRunKeys" @($mergedKeys)
        Update-RowConsecutiveFields $row
        Set-ObjProp $row "FirstFailedET" (Clean (Get-Prop $old "FirstFailedET" (Get-Prop $row "FirstFailedET" "")))
        if ($baseStatus -notin @("RunningAtLatestCheck","CancelledAfterFailure")) { Set-ObjProp $row "Status" "CarriedForwardStillFailing" }
    } else {
        if (!$baseStatus) { Set-ObjProp $row "Status" "NewlyFailedThisCheck" }
        Update-RowConsecutiveFields $row
        if ((Clean (Get-Prop $row "Status" "")) -eq "NewlyFailedThisCheck") { $newFailures += $row }
    }

    if ((Clean (Get-Prop $row "Status" "")) -eq "RunningAtLatestCheck") { $runningCurrent += $row }
    if ((Clean (Get-Prop $row "Status" "")) -eq "CancelledAfterFailure") { $cancelledCurrent += $row }
    $currentFinal += $row
}

$previousMissingNow = @()
if ($previousOpen.Count -gt 0) {
    $previousMissingNow = @($previousOpen | Where-Object { !$currentByKey.ContainsKey([string](Get-Prop $_ "ObjectKey" "")) })
}

$clearanceResult = [pscustomobject]@{ Cleared=@(); Running=@(); Cancelled=@(); Unknown=@() }
if ($previousMissingNow.Count -gt 0) {
    if (!$apiKey) { $apiKey = Get-CohesityApiKey }
    if ($clusters.Count -eq 0) {
        $clusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers @{ accept="application/json"; apiKey=$apiKey }
        $clusters = @($clusterJson.cohesityClusters)
        if ($ClusterName) {
            $clusters = @($clusters | Where-Object { (Get-ClusterName $_) -eq $ClusterName -or (Get-ClusterName $_) -like $ClusterName -or [string](Get-Prop $_ "clusterId" "") -eq $ClusterName })
        }
    }
    $clusterById = @{}
    foreach ($c in $clusters) { $clusterById[[string](Get-Prop $c "clusterId" "")] = $c }

    Write-Host ""
    Write-Host "Checking previously tracked failures that are not in the current failure scan..."
    $clearanceResult = Test-ClearanceForRows -Rows $previousMissingNow -ClustersById $clusterById -ApiKey $apiKey
}

$newlyCleared = @($clearanceResult.Cleared)
$runningFromMissing = @($clearanceResult.Running)
$cancelledFromMissing = @($clearanceResult.Cancelled)
$unknown = @($clearanceResult.Unknown)

foreach ($r in $newlyCleared) { Set-ObjProp $r "Status" "NewlyClearedThisCheck" }
$allCleared = Merge-UniqueRowsByKey @($previousCleared + $newlyCleared)
foreach ($r in $allCleared) {
    if ((Clean (Get-Prop $r "Status" "")) -eq "NewlyClearedThisCheck" -and (@($newlyCleared | Where-Object { (Get-Prop $_ "ObjectKey" "") -eq (Get-Prop $r "ObjectKey" "") }).Count -eq 0)) {
        Set-ObjProp $r "Status" "ClearedByLaterSuccess"
    }
    if ((Clean (Get-Prop $r "Status" "")) -eq "NewlyClearedThisCheck") {
        # Keep this run's status for worknotes/lifecycle.
    } elseif ((Clean (Get-Prop $r "Status" "")) -ne "ClearedByLaterSuccess") {
        Set-ObjProp $r "Status" "ClearedByLaterSuccess"
    }
}

$unresolvedFromMissing = @($runningFromMissing + $cancelledFromMissing + $unknown)
$currentOpenFinal = Merge-UniqueRowsByKey @($currentFinal + $unresolvedFromMissing)
$runningAll = @($runningCurrent + $runningFromMissing)
$cancelledAll = @($cancelledCurrent + $cancelledFromMissing)

$lifecycleRows = Merge-UniqueRowsByKey @($previousLifecycle + $currentOpenFinal + $allCleared)
$lifecycleRows = @($lifecycleRows | Sort-Object @{Expression={ [int64](Get-Prop $_ "LastFailedUsecs" 0) }; Descending=$true}, Status, Cluster, ProtectionGroup)

Write-Csv $currentOpenFinal (Join-Path $outputFolder "current_failures.csv") $csvColumns
Write-Csv $allCleared (Join-Path $outputFolder "cleared_by_success.csv") $csvColumns
Write-Csv $lifecycleRows (Join-Path $outputFolder "incident_lifecycle.csv") $csvColumns

Invoke-OutputRetention -ActiveOutputFolder $outputFolder

Write-WorkNotes -Path (Join-Path $outputFolder "worknotes.txt") -Incident $incident -Window $window -OutputFolder $outputFolder -Current $currentOpenFinal -New $newFailures -ClearedThisRun $newlyCleared -Refailed $refailed -Running $runningAll -Cancelled $cancelledAll -Unknown $unknown
Write-SummaryFile -Path (Join-Path $outputFolder "summary.txt") -Incident $incident -Window $window -OutputFolder $outputFolder -Current $currentOpenFinal -AllCleared $allCleared -Lifecycle $lifecycleRows -New $newFailures -ClearedThisRun $newlyCleared -Refailed $refailed -Running $runningAll -Cancelled $cancelledAll -Unknown $unknown
Write-ClosingSummaryFile -Path (Join-Path $outputFolder "closing_summary.txt") -Incident $incident -WindowLabel $window.WindowLabel -GeneratedEt $window.GeneratedET -OutputFolder $outputFolder -Current $currentOpenFinal -AllCleared $allCleared -Lifecycle $lifecycleRows -Running $runningAll -Cancelled $cancelledAll -Unknown $unknown

$state = [pscustomobject]@{
    IncidentNumber = $incident
    WindowKey = $window.WindowKey
    WindowLabel = $window.WindowLabel
    WindowStartET = $window.WindowStartET
    WindowEndET = $window.WindowEndET
    LastRunET = Get-NowEtText
    NumRunsEvaluated = $NumRuns
    ClusterFilter = $ClusterName
    OutputFolder = $outputFolder
    UsedLegacyFailureCsv = $usedLegacyCsv
    CurrentOpenFailures = @($currentOpenFinal)
    ClearedBySuccess = @($allCleared)
    LifecycleRows = @($lifecycleRows)
    LastRunNewFailures = @($newFailures)
    LastRunClearedBySuccess = @($newlyCleared)
    LastRunReFailed = @($refailed)
    LastRunRunning = @($runningAll)
    LastRunCancelled = @($cancelledAll)
    LastRunUnknownNeedsReview = @($unknown)
    Warnings = @($script:Warnings)
    RetentionActions = @($script:RetentionActions)
}
Write-Json $state $statePath

Write-Host ""
Write-Host "Summary:"
Write-Host "Current Active / Unresolved : $(@($currentOpenFinal).Count)"
Write-Host "New Failures This Check     : $(@($newFailures).Count)"
Write-Host "Cleared By Success This Run : $(@($newlyCleared).Count)"
Write-Host "Re-Failed This Check        : $(@($refailed).Count)"
Write-Host "Running / Awaiting Result   : $(@($runningAll).Count)"
Write-Host "Cancelled After Failure     : $(@($cancelledAll).Count)"
Write-Host "Unknown / Needs Review      : $(@($unknown).Count)"
Write-Host "Warnings                    : $($script:Warnings.Count)"
Write-Host ""
Write-Host "Files Created:"
Write-Host (Join-Path $outputFolder "current_failures.csv")
Write-Host (Join-Path $outputFolder "cleared_by_success.csv")
Write-Host (Join-Path $outputFolder "incident_lifecycle.csv")
Write-Host (Join-Path $outputFolder "worknotes.txt")
Write-Host (Join-Path $outputFolder "summary.txt")
Write-Host (Join-Path $outputFolder "closing_summary.txt")
Write-Host (Join-Path $outputFolder "state.json")