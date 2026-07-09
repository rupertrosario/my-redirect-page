<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
GET-only Cohesity Helios collector for backup failure incident updates.

This collector is intentionally aligned with the original working report:
backup_failures/Cohesity_Backup_Failures

Core rule:
- run.objects is the source of truth.
- Object-level rows win.
- PG/run-level fallback is allowed only when Cohesity returns no object evidence.
- A newer successful object backup clears an older failed object backup.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow',
    [string]$LegacyFailureOutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailures',
    [string]$HelperPath = ('X:\PowerShell\Cohesity_API_Scripts\Common\' + 'Api' + 'KeyAesHelper.ps1'),
    [string]$EncryptedFile = ('X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_' + 'api' + 'key.enc'),
    [string]$ClusterName = '',
    [int]$NumRuns = 20,
    [string]$IncidentNumber = '',
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = '',
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35,
    [int]$RequestTimeoutSec = 120
)

$ErrorActionPreference = 'Stop'
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:CsvColumns = @('IncidentNumber','WindowKey','Status','Cluster','Environment','ProtectionGroup','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','ClearedET','LastSeenET','LatestRunStatus','ConsecutiveFailureCount','Message','ObjectKey','ClusterId','ProtectionGroupId','EnvironmentFilter','FailedRunKeys')
$script:LifecycleColumns = @('Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','Status','OldestFailedET','NewestFailedET','LatestSuccessET','FailureRuns','Message')
$script:FailureColumns = @('Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','Status','OldestFailedET','NewestFailedET','LatestSuccessET','FailureRuns','Message')
$script:SuccessColumns = @('Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','LatestSuccessET','Message')

function Clean($Value) {
    if ($null -eq $Value) { return '' }
    if ($Value -is [array]) { $Value = @($Value) -join ' | ' }
    $t = [string]$Value
    $t = $t.Replace([char]13, ' ').Replace([char]10, ' ')
    $t = [regex]::Replace($t, '\s+', ' ')
    return $t.Replace([char]34, [char]39).Trim()
}

function As-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

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

function Add-RunWarning([string]$Message) {
    $m = Clean $Message
    if ($m) {
        $script:Warnings.Add($m) | Out-Null
        Write-Warning $m
    }
}

function To-Int64($Value) {
    try {
        $s = Clean $Value
        if (!$s) { return [int64]0 }
        return [int64]$s
    } catch {
        return [int64]0
    }
}

function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
}
$script:EtZone = Get-EtZone

function Get-NowEtDate { [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone) }
function Get-NowEtText { (Get-NowEtDate).ToString('yyyy-MM-dd HH:mm:ss') }

function Convert-UsecsToEtText($Usecs) {
    $u = To-Int64 $Usecs
    if ($u -le 0) { return '' }
    try {
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch { return '' }
}

function Convert-EtToUsecs([datetime]$EtDate) {
    $utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtDate, [DateTimeKind]::Unspecified), $script:EtZone)
    [int64](([DateTimeOffset]::new($utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000)
}

function Parse-EtTextToDate([string]$Text) {
    $t = Clean $Text
    if (!$t) { return $null }
    foreach ($fmt in @('yyyy-MM-dd HH:mm:ss','yyyy-MM-dd H:mm:ss','M/d/yyyy h:mm:ss tt','M/d/yyyy H:mm:ss','yyyy-MM-ddTHH:mm:ss')) {
        try { return [datetime]::ParseExact($t, $fmt, [Globalization.CultureInfo]::InvariantCulture) } catch {}
    }
    try { return [datetime]::Parse($t) } catch { return $null }
}

function Convert-EtTextToUsecs([string]$Text) {
    $dt = Parse-EtTextToDate $Text
    if ($null -eq $dt) { return 0 }
    Convert-EtToUsecs $dt
}

function Date-Sort($Value) {
    $d = Parse-EtTextToDate (Clean $Value)
    if ($d) { return $d.ToString('yyyy-MM-dd HH:mm:ss') }
    $t = Clean $Value
    if ($t) { return $t }
    return '0000-00-00 00:00:00'
}

function Read-Json([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    try { return ($raw | ConvertFrom-Json) } catch { return $null }
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
        ($Columns -join ',') | Set-Content -Path $Path -Encoding UTF8
    } else {
        $list | Select-Object -Property $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec $RequestTimeoutSec
    } else {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $RequestTimeoutSec
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    $r.Content | ConvertFrom-Json
}

function Get-CohesityApiKey {
    if (!(Test-Path $HelperPath)) { throw ('Missing API key helper: {0}' -f $HelperPath) }
    if (!(Test-Path $EncryptedFile)) { throw ('Missing encrypted key file: {0}' -f $EncryptedFile) }
    . $HelperPath
    $key = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($key)) { throw 'API key is blank from AES helper.' }
    $key.Trim()
}

function Get-ComputeWindow {
    $nowEt = Get-NowEtDate
    if ($nowEt.Hour -lt 18) { $start = $nowEt.Date.AddDays(-1).AddHours(18) } else { $start = $nowEt.Date.AddHours(18) }
    $end = $start.AddDays(1)
    [pscustomobject]@{
        WindowKey = ('{0}_1800ET' -f $start.ToString('yyyy-MM-dd'))
        WindowLabel = ('{0} 18:00 ET -> {1} 18:00 ET' -f $start.ToString('yyyy-MM-dd'), $end.ToString('yyyy-MM-dd'))
        WindowStartET = $start.ToString('yyyy-MM-dd HH:mm:ss')
        WindowEndET = $end.ToString('yyyy-MM-dd HH:mm:ss')
        GeneratedET = Get-NowEtText
    }
}

function Get-RegistryPath {
    if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
    Join-Path $OutputRoot 'BackupFailure_WindowRegistry.json'
}

function Get-WindowRegistry {
    $registry = Read-Json (Get-RegistryPath)
    if (!$registry) { $registry = [pscustomobject]@{ Windows = [pscustomobject]@{} } }
    if (!$registry.PSObject.Properties['Windows']) { $registry | Add-Member -MemberType NoteProperty -Name 'Windows' -Value ([pscustomobject]@{}) -Force }
    $registry
}

function Save-WindowRegistry($Registry) { Write-Json $Registry (Get-RegistryPath) }

function Resolve-IncidentLock($Window) {
    $registry = Get-WindowRegistry
    $existing = $registry.Windows.PSObject.Properties[$Window.WindowKey]
    if ($existing) {
        $entry = $existing.Value
        Set-ObjProp $entry 'LastRunET' $Window.GeneratedET
        Save-WindowRegistry $registry
        return $entry
    }

    $inc = $IncidentNumber
    if (!$inc) { $inc = Read-Host 'Enter incident number for this backup-failure window' }
    $inc = $inc.Trim().ToUpper()
    if ($inc -notmatch '^INC[0-9A-Z]+$') { throw ('Invalid incident number: {0}' -f $inc) }

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
    $n = Clean (Get-Prop $Cluster 'name' '')
    if (!$n) { $n = Clean (Get-Prop $Cluster 'clusterName' '') }
    if (!$n) { $n = Clean (Get-Prop $Cluster 'displayName' '') }
    if (!$n) { $n = ('Unknown-{0}' -f (Clean (Get-Prop $Cluster 'clusterId' ''))) }
    $n
}

function Get-EnvironmentMap {
    @(
        [pscustomobject]@{ Label='Oracle';        Filter='kOracle';        TargetType='kDatabase';       ParentHostNeeded=$true;  NasFallback=$false },
        [pscustomobject]@{ Label='SQL';           Filter='kSQL';           TargetType='kDatabase';       ParentHostNeeded=$true;  NasFallback=$false },
        [pscustomobject]@{ Label='Physical';      Filter='kPhysical';      TargetType='kHost';           ParentHostNeeded=$false; NasFallback=$false },
        [pscustomobject]@{ Label='GenericNas';    Filter='kGenericNas';    TargetType='kHost';           ParentHostNeeded=$false; NasFallback=$true  },
        [pscustomobject]@{ Label='HyperV';        Filter='kHyperV';        TargetType='kVirtualMachine'; ParentHostNeeded=$false; NasFallback=$false },
        [pscustomobject]@{ Label='Acropolis';     Filter='kAcropolis';     TargetType='kVirtualMachine'; ParentHostNeeded=$false; NasFallback=$false },
        [pscustomobject]@{ Label='RemoteAdapter'; Filter='kRemoteAdapter'; TargetType='kRemoteAdapter';  ParentHostNeeded=$false; NasFallback=$false },
        [pscustomobject]@{ Label='Isilon';        Filter='kIsilon';        TargetType='kHost';           ParentHostNeeded=$false; NasFallback=$true  }
    )
}

function Is-FailedStatus([string]$Status) { (Clean $Status) -in @('Failed','kFailed','Failure','kFailure','Error','kError') }
function Is-SuccessStatus([string]$Status) { (Clean $Status) -in @('Succeeded','SucceededWithWarning','kSucceeded','kSucceededWithWarning') }
function Is-RunningStatus([string]$Status) { (Clean $Status) -in @('Running','kRunning','Accepted','kAccepted','Queued','kQueued') }
function Is-CancelledStatus([string]$Status) { (Clean $Status) -in @('Canceled','Cancelled','kCanceled','kCancelled','Canceling','kCanceling') }
function Is-ActiveLifecycleStatus([string]$Status) { (Clean $Status) -in @('NewlyFailedThisCheck','OlderStillFailing','CurrentStillFailing','CarriedForwardStillFailing','ReFailedAfterClear','RunningAtLatestCheck','CancelledAfterFailure','UnknownNeedsReview') }
function Is-ClearedLifecycleStatus([string]$Status) { (Clean $Status) -in @('NewlyClearedThisCheck','ClearedByLaterSuccess') }

function Get-FirstLocalBackupInfo($Run) {
    $x = @(As-Array (Get-Prop $Run 'localBackupInfo' @()) | Select-Object -First 1)
    if ($x.Count -gt 0) { return $x[0] }
    return $null
}

function Get-RunEffectiveUsecs($Run) {
    $i = Get-FirstLocalBackupInfo $Run
    if (!$i) { return 0 }
    $end = To-Int64 (Get-Prop $i 'endTimeUsecs' 0)
    if ($end -gt 0) { return $end }
    To-Int64 (Get-Prop $i 'startTimeUsecs' 0)
}

function Get-ProtectionGroupName($ProtectionGroup, [string]$Fallback = '') {
    $n = Clean (Get-Prop $ProtectionGroup 'name' '')
    if (!$n) { $n = Clean (Get-Prop $ProtectionGroup 'protectionGroupName' '') }
    if (!$n) { $n = Clean (Get-Prop $ProtectionGroup 'displayName' '') }
    if (!$n) { $n = Clean $Fallback }
    $n
}

function Get-ProtectionGroupId($ProtectionGroup, [string]$Fallback = '') {
    $id = Clean (Get-Prop $ProtectionGroup 'id' '')
    if (!$id) { $id = Clean (Get-Prop $ProtectionGroup 'protectionGroupId' '') }
    if (!$id) { $id = Clean $Fallback }
    $id
}

function Get-FailedAttempts($RunObject) {
    $attempts = @()
    foreach ($lsi in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        $attempts += @(As-Array (Get-Prop $lsi 'failedAttempts' @()))
        foreach ($snap in As-Array (Get-Prop $lsi 'snapshotInfo' @())) { $attempts += @(As-Array (Get-Prop $snap 'failedAttempts' @())) }
    }
    foreach ($snap in As-Array (Get-Prop $RunObject 'snapshotInfo' @())) { $attempts += @(As-Array (Get-Prop $snap 'failedAttempts' @())) }
    @($attempts | Where-Object { $_ })
}

function Combine-FailedAttempts($RunObject) {
    $messages = @()
    foreach ($attempt in Get-FailedAttempts $RunObject) {
        foreach ($field in @('message','error','reason','errorMessage','failureMessage')) {
            $m = Clean (Get-Prop $attempt $field '')
            if ($m) { $messages += $m }
        }
    }
    Clean (($messages | Where-Object { $_ } | Select-Object -Unique) -join ' | ')
}

function Get-FailureMessage($RunObject, $RunInfo = $null) {
    $messages = @()
    $attemptText = Combine-FailedAttempts $RunObject
    if ($attemptText) { $messages += $attemptText }

    foreach ($container in @($RunObject, (Get-Prop $RunObject 'object' $null), $RunInfo)) {
        if ($null -eq $container) { continue }
        foreach ($field in @('error','message','messages','errorMessage','failureMessage','reason','lastError','lastFailureMessage')) {
            $m = Clean (Get-Prop $container $field '')
            if ($m) { $messages += $m }
        }
    }

    foreach ($lsi in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        foreach ($field in @('error','message','messages','errorMessage','failureMessage','reason','lastError','lastFailureMessage')) {
            $m = Clean (Get-Prop $lsi $field '')
            if ($m) { $messages += $m }
        }
        $st = Clean (Get-Prop $lsi 'status' '')
        if (Is-FailedStatus $st) { $messages += ('Object local snapshot status: {0}' -f $st) }
        foreach ($snap in As-Array (Get-Prop $lsi 'snapshotInfo' @())) {
            foreach ($field in @('error','message','messages','errorMessage','failureMessage','reason','lastError','lastFailureMessage')) {
                $m = Clean (Get-Prop $snap $field '')
                if ($m) { $messages += $m }
            }
            $sst = Clean (Get-Prop $snap 'status' '')
            if (Is-FailedStatus $sst) { $messages += ('Object snapshot status: {0}' -f $sst) }
        }
    }

    Clean (($messages | Where-Object { $_ } | Select-Object -Unique) -join ' | ')
}

function Has-ObjectFailureEvidence($RunObject) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject 'object' $null)) { return $false }
    if (@(Get-FailedAttempts $RunObject).Count -gt 0) { return $true }

    $statuses = @()
    $statuses += Clean (Get-Prop $RunObject 'status' '')
    $obj = Get-Prop $RunObject 'object' $null
    $statuses += Clean (Get-Prop $obj 'status' '')
    foreach ($lsi in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        $statuses += Clean (Get-Prop $lsi 'status' '')
        foreach ($snap in As-Array (Get-Prop $lsi 'snapshotInfo' @())) { $statuses += Clean (Get-Prop $snap 'status' '') }
    }
    if (@($statuses | Where-Object { Is-FailedStatus $_ }).Count -gt 0) { return $true }

    $msg = Get-FailureMessage $RunObject
    return [bool](Clean $msg)
}

function Is-SuccessForClear($RunObject) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject 'object' $null)) { return $false }
    return (-not (Has-ObjectFailureEvidence $RunObject))
}

function Get-ObjectKey($RunObject, [string]$ClusterId, [string]$EnvironmentLabel) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject 'object' $null)) { return '' }
    $obj = Get-Prop $RunObject 'object' $null
    $env = Clean (Get-Prop $obj 'environment' '')
    $type = Clean (Get-Prop $obj 'objectType' '')
    $name = Clean (Get-Prop $obj 'name' '')
    $objId = Clean (Get-Prop $obj 'id' '')
    $sourceId = Clean (Get-Prop $obj 'sourceId' '')
    return ('{0}|{1}|{2}|{3}|{4}|{5}|{6}' -f $ClusterId, $EnvironmentLabel, $env, $type, $name, $objId, $sourceId)
}

function Get-RunLevelKey([string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId, [string]$ProtectionGroupName, [string]$RunType) {
    ('{0}|{1}|{2}|RUNLEVEL|{3}|{4}' -f $ClusterId, $EnvironmentLabel, $ProtectionGroupId, $RunType, $ProtectionGroupName)
}

function Add-SuccessIndex([hashtable]$Index, [string]$Key, [int64]$Usecs, [string]$Status) {
    if (!$Key -or $Usecs -le 0) { return }
    if (!$Index.ContainsKey($Key) -or $Usecs -gt [int64]$Index[$Key].Usecs) {
        $Index[$Key] = [pscustomobject]@{ Usecs = $Usecs; ET = Convert-UsecsToEtText $Usecs; Status = Clean $Status }
    }
}

function Add-FailureRunKey([hashtable]$Map, [string]$Key, [string]$RunKey) {
    if (!$Key -or !$RunKey) { return }
    if (!$Map.ContainsKey($Key)) { $Map[$Key] = New-Object 'System.Collections.Generic.HashSet[string]' }
    [void]$Map[$Key].Add($RunKey)
}

function Get-UsecsFromRunKey([string]$RunKey) {
    if ([string]::IsNullOrWhiteSpace($RunKey)) { return 0 }
    try {
        $parts = $RunKey -split '\|'
        return [int64]$parts[$parts.Count - 1]
    } catch { return 0 }
}

function Update-RowFailureFields($Row, $Keys) {
    $keys = @($Keys | Where-Object { $_ } | Select-Object -Unique)
    Set-ObjProp $Row 'FailedRunKeys' @($keys)
    Set-ObjProp $Row 'ConsecutiveFailureCount' $keys.Count
    $times = @()
    foreach ($k in $keys) {
        $u = Get-UsecsFromRunKey $k
        if ($u -gt 0) { $times += $u }
    }
    if ($times.Count -gt 0) {
        $min = ($times | Measure-Object -Minimum).Minimum
        $max = ($times | Measure-Object -Maximum).Maximum
        Set-ObjProp $Row 'FirstFailedET' (Convert-UsecsToEtText $min)
        Set-ObjProp $Row 'LastFailedET' (Convert-UsecsToEtText $max)
        Set-ObjProp $Row 'LastFailedUsecs' ([int64]$max)
    }
}

function New-TrackingRow {
    param(
        [string]$IncidentNumber,
        $Window,
        [string]$ClusterName,
        [string]$ClusterId,
        $Env,
        [string]$ProtectionGroupName,
        [string]$ProtectionGroupId,
        [string]$ObjectKey,
        [string]$HostName,
        [string]$ObjectName,
        [string]$ObjectType,
        [string]$RunType,
        [int64]$StartUsecs,
        [int64]$EndUsecs,
        [string]$Message,
        [string]$Status,
        [string]$LatestRunStatus,
        [int64]$LatestRunUsecs,
        [string[]]$FailedRunKeys
    )

    $effective = if ($EndUsecs -gt 0) { $EndUsecs } else { $StartUsecs }
    if ($LatestRunUsecs -le 0) { $LatestRunUsecs = $effective }

    [pscustomobject]@{
        IncidentNumber = Clean $IncidentNumber
        WindowKey = Clean $Window.WindowKey
        Status = Clean $Status
        Cluster = Clean $ClusterName
        Environment = Clean $Env.Label
        ProtectionGroup = Clean $ProtectionGroupName
        Host = Clean $HostName
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = Convert-UsecsToEtText $effective
        LastFailedET = Convert-UsecsToEtText $effective
        LastFailedUsecs = $effective
        ClearedET = ''
        LastSeenET = Convert-UsecsToEtText $LatestRunUsecs
        LatestRunStatus = Clean $LatestRunStatus
        ConsecutiveFailureCount = 1
        Message = Clean $Message
        ObjectKey = Clean $ObjectKey
        ClusterId = Clean $ClusterId
        ProtectionGroupId = Clean $ProtectionGroupId
        EnvironmentFilter = Clean $Env.Filter
        FailedRunKeys = @($FailedRunKeys)
    }
}

function Mark-RowCleared($Row, [hashtable]$SuccessIndex, [string]$Key) {
    Set-ObjProp $Row 'Status' 'NewlyClearedThisCheck'
    if ($SuccessIndex.ContainsKey($Key)) {
        Set-ObjProp $Row 'ClearedET' (Clean $SuccessIndex[$Key].ET)
        Set-ObjProp $Row 'LastSeenET' (Clean $SuccessIndex[$Key].ET)
        Set-ObjProp $Row 'LatestRunStatus' (Clean $SuccessIndex[$Key].Status)
    }
}

function Get-ProtectionGroups($Cluster, $Env, [string]$ApiKey) {
    $clusterId = Clean (Get-Prop $Cluster 'clusterId' '')
    $headers = @{ accept = 'application/json'; apiKey = $ApiKey; accessClusterId = $clusterId }
    $pgs = @()
    foreach ($filter in ($Env.Filter.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        try {
            $uri = ('{0}/v2/data-protect/protection-groups?environments={1}&isDeleted=false&isPaused=false&isActive=true' -f $BaseUrl, $filter)
            $json = Invoke-HeliosGetJson -Uri $uri -Headers $headers
            if ($json -and $json.protectionGroups) { $pgs += @($json.protectionGroups) }
        } catch {
            Add-RunWarning ('Protection group lookup failed for {0} / {1} : {2}' -f (Get-ClusterName $Cluster), $filter, $_.Exception.Message)
        }
    }
    @($pgs | Sort-Object -Property id -Unique)
}

function Get-ProtectionGroupRuns($Cluster, [string]$ProtectionGroupId, [int]$RunLimit, [string]$ApiKey) {
    $clusterId = Clean (Get-Prop $Cluster 'clusterId' '')
    $headers = @{ accept = 'application/json'; apiKey = $ApiKey; accessClusterId = $clusterId }
    $escaped = [uri]::EscapeDataString($ProtectionGroupId)
    $uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false&includeObjectDetails=true' -f $BaseUrl, $escaped, $RunLimit)
    $json = Invoke-HeliosGetJson -Uri $uri -Headers $headers
    if ($json -and $json.runs) { return @($json.runs) }
    @()
}

function Get-ObjectNameMaps($Runs) {
    $idToName = @{}
    $hostById = @{}
    foreach ($run in $Runs) {
        foreach ($ro in (As-Array (Get-Prop $run 'objects' @()))) {
            $obj = Get-Prop $ro 'object' $null
            if (!$obj) { continue }
            $id = Clean (Get-Prop $obj 'id' '')
            $name = Clean (Get-Prop $obj 'name' '')
            if ($id -and $name -and !$idToName.ContainsKey($id)) { $idToName[$id] = $name }
            $otype = Clean (Get-Prop $obj 'objectType' '')
            $oenv = Clean (Get-Prop $obj 'environment' '')
            if ($id -and $name -and ($otype -eq 'kHost' -or $oenv -eq 'kPhysical')) { $hostById[$id] = $name }
        }
    }
    [pscustomobject]@{ IdToName = $idToName; HostById = $hostById }
}

function Get-ParentHostName($RunObject, $Maps) {
    $obj = Get-Prop $RunObject 'object' $null
    if (!$obj) { return '' }
    $sid = Clean (Get-Prop $obj 'sourceId' '')
    if ($sid -and $Maps.IdToName.ContainsKey($sid)) { return Clean $Maps.IdToName[$sid] }
    if ($sid -and $Maps.HostById.ContainsKey($sid)) { return Clean $Maps.HostById[$sid] }
    $otype = Clean (Get-Prop $obj 'objectType' '')
    $oenv = Clean (Get-Prop $obj 'environment' '')
    if ($otype -eq 'kHost' -or $oenv -eq 'kPhysical') { return Clean (Get-Prop $obj 'name' '') }
    return ''
}

function Test-TargetObject($RunObject, $Env, [string[]]$FilterSet) {
    $obj = Get-Prop $RunObject 'object' $null
    if (!$obj) { return $false }
    if ($Env.NasFallback) { return (Has-ObjectFailureEvidence $RunObject) }

    $otype = Clean (Get-Prop $obj 'objectType' '')
    $oenv = Clean (Get-Prop $obj 'environment' '')
    if ($otype -ne $Env.TargetType) { return $false }
    if (!$oenv) { return $true }
    return ($FilterSet -contains $oenv)
}

function Add-Or-UpdateCollectedRow([hashtable]$Bucket, $Row) {
    $key = Clean (Get-Prop $Row 'ObjectKey' '')
    if (!$key) { return }
    if (!$Bucket.ContainsKey($key)) { $Bucket[$key] = $Row; return }
    $oldUsecs = To-Int64 (Get-Prop $Bucket[$key] 'LastFailedUsecs' 0)
    $newUsecs = To-Int64 (Get-Prop $Row 'LastFailedUsecs' 0)
    if ($newUsecs -gt $oldUsecs) { $Bucket[$key] = $Row }
}

function Collect-RunRows($Incident, $Window, $Clusters, [string]$ApiKey) {
    $activeRows = @{}
    $clearedRows = @{}
    $successIndex = @{}

    $clusterList = @($Clusters | Sort-Object @{Expression={ Get-ClusterName $_ }})
    $clusterTotal = $clusterList.Count
    $clusterIndex = 0

    foreach ($cluster in $clusterList) {
        $clusterIndex++
        $clusterName = Get-ClusterName $cluster
        $clusterId = Clean (Get-Prop $cluster 'clusterId' '')
        Write-Host ('[{0}/{1}] Cluster: {2}' -f $clusterIndex, $clusterTotal, $clusterName)

        foreach ($env in (Get-EnvironmentMap)) {
            $envStartCount = $activeRows.Count + $clearedRows.Count
            $pgsChecked = 0
            $filterSet = @($env.Filter.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            $pgs = Get-ProtectionGroups -Cluster $cluster -Env $env -ApiKey $ApiKey

            foreach ($pg in $pgs) {
                $pgsChecked++
                $pgId = Get-ProtectionGroupId $pg
                $pgName = Get-ProtectionGroupName $pg
                if (!$pgId) { continue }

                try {
                    $runs = Get-ProtectionGroupRuns -Cluster $cluster -ProtectionGroupId $pgId -RunLimit $NumRuns -ApiKey $ApiKey
                } catch {
                    Add-RunWarning ('Runs lookup failed for {0} / {1} : {2}' -f $clusterName, $pgName, $_.Exception.Message)
                    continue
                }
                if ($runs.Count -eq 0) { continue }

                $runTypes = @(
                    $runs |
                        ForEach-Object { $i = Get-FirstLocalBackupInfo $_; if ($i) { Clean (Get-Prop $i 'runType' '') } } |
                        Where-Object { $_ } |
                        Select-Object -Unique
                )

                foreach ($runType in $runTypes) {
                    $runsForType = @(
                        $runs |
                            Where-Object { $i = Get-FirstLocalBackupInfo $_; $i -and (Clean (Get-Prop $i 'runType' '')) -eq $runType } |
                            Sort-Object { Get-RunEffectiveUsecs $_ } -Descending
                    )
                    if ($runsForType.Count -eq 0) { continue }

                    $latestInfo = Get-FirstLocalBackupInfo $runsForType[0]
                    $latestRunStatus = Clean (Get-Prop $latestInfo 'status' '')
                    $latestRunUsecs = Get-RunEffectiveUsecs $runsForType[0]
                    $maps = Get-ObjectNameMaps $runsForType
                    $cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $latestFailByKey = @{}
                    $failedKeysByKey = @{}
                    $anyObjectEvidenceInRunType = $false
                    $runLevelCleared = $false
                    $runLevelKey = Get-RunLevelKey $clusterId $env.Label $pgId $pgName $runType

                    foreach ($run in $runsForType) {
                        $info = Get-FirstLocalBackupInfo $run
                        if (!$info) { continue }
                        $status = Clean (Get-Prop $info 'status' '')
                        $startUsecs = To-Int64 (Get-Prop $info 'startTimeUsecs' 0)
                        $endUsecs = To-Int64 (Get-Prop $info 'endTimeUsecs' 0)
                        $effectiveUsecs = if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs }
                        $objectsAll = @(As-Array (Get-Prop $run 'objects' @()) | Where-Object { $_ -and (Get-Prop $_ 'object' $null) })

                        if ($objectsAll.Count -gt 0) { $anyObjectEvidenceInRunType = $true }

                        if ((Is-SuccessStatus $status) -and $objectsAll.Count -eq 0) {
                            $runLevelCleared = $true
                            Add-SuccessIndex $successIndex $runLevelKey $effectiveUsecs $status
                            continue
                        }

                        # Newer object success clears older object failure. Runs are processed newest to oldest.
                        foreach ($ob in $objectsAll) {
                            if (Is-SuccessForClear $ob) {
                                $k = Get-ObjectKey $ob $clusterId $env.Label
                                if ($k) {
                                    [void]$cleared.Add($k)
                                    Add-SuccessIndex $successIndex $k $effectiveUsecs $status
                                }
                            }
                        }

                        if ($objectsAll.Count -gt 0) {
                            $candidateObjects = @($objectsAll | Where-Object { (Test-TargetObject $_ $env $filterSet) -and (Has-ObjectFailureEvidence $_) })

                            # Physical can return failed object rows without failedAttempts, while the run is Failed.
                            if ($candidateObjects.Count -eq 0 -and $env.Label -eq 'Physical' -and (Is-FailedStatus $status)) {
                                $candidateObjects = @($objectsAll | Where-Object { Test-TargetObject $_ $env $filterSet })
                            }

                            # If a failed run returns objects but no explicit failed object evidence, emit object-level review rows.
                            if ($candidateObjects.Count -eq 0 -and (Is-FailedStatus $status)) {
                                $candidateObjects = @($objectsAll | Where-Object { Test-TargetObject $_ $env $filterSet })
                            }

                            foreach ($ob in $candidateObjects) {
                                $ok = Get-ObjectKey $ob $clusterId $env.Label
                                if (!$ok) { continue }
                                if ($latestFailByKey.ContainsKey($ok)) { continue }

                                $obj = Get-Prop $ob 'object' $null
                                $objName = Clean (Get-Prop $obj 'name' '')
                                $objType = Clean (Get-Prop $obj 'objectType' '')
                                $hostName = ''
                                if ($env.ParentHostNeeded) { $hostName = Get-ParentHostName $ob $maps }

                                $msg = Get-FailureMessage $ob $info
                                if (!$msg) {
                                    if (Is-FailedStatus $status) { $msg = 'Run marked Failed; Cohesity returned object but no failedAttempts[] message' }
                                    else { $msg = 'Object-level failure evidence found in run.objects' }
                                }

                                $rowStatus = 'NewlyFailedThisCheck'
                                $alreadyCleared = $cleared.Contains($ok)
                                if ($alreadyCleared) { $rowStatus = 'NewlyClearedThisCheck' }
                                elseif ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = 'RunningAtLatestCheck' }
                                elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = 'CancelledAfterFailure' }
                                elseif (!(Has-ObjectFailureEvidence $ob) -and (Is-FailedStatus $status)) { $rowStatus = 'UnknownNeedsReview' }

                                $runKey = ('{0}|{1}|{2}|{3}|{4}' -f $clusterId, $pgId, $ok, $runType, $effectiveUsecs)
                                Add-FailureRunKey $failedKeysByKey $ok $runKey
                                $row = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroupName $pgName -ProtectionGroupId $pgId -ObjectKey $ok -HostName $hostName -ObjectName $objName -ObjectType $objType -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                                if ($alreadyCleared) { Mark-RowCleared $row $successIndex $ok }
                                $latestFailByKey[$ok] = $row
                            }

                            continue
                        }

                        # PG/run-level fallback is only permitted when there is no object evidence at all for this PG/run type.
                        if (!$anyObjectEvidenceInRunType -and (Is-FailedStatus $status) -and !$runLevelCleared -and $latestFailByKey.Count -eq 0) {
                            $msg = Clean (Get-Prop $info 'messages' '')
                            if (!$msg) { $msg = ('{0} run failed - no object-level details returned' -f $env.Label) }
                            $runKey = ('{0}|{1}|{2}|{3}|{4}' -f $clusterId, $pgId, $runLevelKey, $runType, $effectiveUsecs)
                            Add-FailureRunKey $failedKeysByKey $runLevelKey $runKey
                            $row = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroupName $pgName -ProtectionGroupId $pgId -ObjectKey $runLevelKey -HostName '' -ObjectName '' -ObjectType '' -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status 'UnknownNeedsReview' -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            $latestFailByKey[$runLevelKey] = $row
                        }
                    }

                    # If object-level evidence exists, remove any PG/run-level fallback for the same PG/run type.
                    if ($anyObjectEvidenceInRunType -and $latestFailByKey.ContainsKey($runLevelKey)) {
                        $latestFailByKey.Remove($runLevelKey)
                    }

                    foreach ($k in $latestFailByKey.Keys) {
                        if ($failedKeysByKey.ContainsKey($k)) { Update-RowFailureFields $latestFailByKey[$k] @($failedKeysByKey[$k]) }
                        if (Is-ClearedLifecycleStatus (Get-Prop $latestFailByKey[$k] 'Status' '')) { Add-Or-UpdateCollectedRow $clearedRows $latestFailByKey[$k] }
                        else { Add-Or-UpdateCollectedRow $activeRows $latestFailByKey[$k] }
                    }
                }
            }

            $envRows = ($activeRows.Count + $clearedRows.Count) - $envStartCount
            Write-Host ('  {0,-13}: PGs checked: {1} | rows: {2}' -f $env.Label, $pgsChecked, $envRows)
        }
    }

    [pscustomobject]@{
        ActiveRows = @($activeRows.Values)
        ClearedRows = @($clearedRows.Values)
        Rows = @($activeRows.Values + $clearedRows.Values)
        SuccessIndex = $successIndex
    }
}

function Clone-Row($Row) {
    if ($null -eq $Row) { return $null }
    $Row | Select-Object *
}

function Test-BlankObjectRow($Row) {
    return (!(Clean (Get-Prop $Row 'ObjectName' '')) -and !(Clean (Get-Prop $Row 'ObjectType' '')))
}

function Get-RowGroupKey($Row) {
    ('{0}|{1}|{2}|{3}' -f (Clean (Get-Prop $Row 'Cluster' '')), (Clean (Get-Prop $Row 'ProtectionGroup' '')), (Clean (Get-Prop $Row 'Environment' '')), (Clean (Get-Prop $Row 'RunType' '')))
}

function Normalize-ExistingRows($Rows, [string]$Incident, $Window) {
    $out = @()
    foreach ($r in @($Rows)) {
        $n = Clone-Row $r
        if ($null -eq $n) { continue }
        $status = Clean (Get-Prop $n 'Status' '')
        if (!$status) { $status = 'UnknownNeedsReview' }
        Set-ObjProp $n 'Status' $status
        if (!(Get-Prop $n 'IncidentNumber' '')) { Set-ObjProp $n 'IncidentNumber' $Incident }
        if (!(Get-Prop $n 'WindowKey' '')) { Set-ObjProp $n 'WindowKey' $Window.WindowKey }
        if (!(Get-Prop $n 'LastFailedUsecs' $null)) { Set-ObjProp $n 'LastFailedUsecs' (Convert-EtTextToUsecs (Clean (Get-Prop $n 'LastFailedET' ''))) }
        if (!(Get-Prop $n 'FailedRunKeys' $null)) { Set-ObjProp $n 'FailedRunKeys' @() }
        if (!(Get-Prop $n 'ConsecutiveFailureCount' $null)) { Set-ObjProp $n 'ConsecutiveFailureCount' 1 }
        $out += $n
    }
    @($out)
}

function Index-ByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = Clean (Get-Prop $r 'ObjectKey' '')
        if ($k) { $h[$k] = $r }
    }
    $h
}

function Merge-FailedRunKeys($ExistingRow, $NewRow) {
    $keys = @()
    if ($ExistingRow) { $keys += As-Array (Get-Prop $ExistingRow 'FailedRunKeys' @()) }
    if ($NewRow) { $keys += As-Array (Get-Prop $NewRow 'FailedRunKeys' @()) }
    @($keys | Where-Object { $_ } | Select-Object -Unique)
}

function Merge-Lifecycle($CollectedRows, $PreviousOpenRows, $PreviousClearedRows, [hashtable]$SuccessIndex) {
    $current = @()
    $clearedThisRun = @()
    $collectedActive = @($CollectedRows | Where-Object { Is-ActiveLifecycleStatus (Get-Prop $_ 'Status' '') })
    $collectedCleared = @($CollectedRows | Where-Object { Is-ClearedLifecycleStatus (Get-Prop $_ 'Status' '') })
    $currentByKey = Index-ByKey $collectedActive
    $previousOpenByKey = Index-ByKey $PreviousOpenRows

    $groupsWithObjectRows = @{}
    foreach ($r in @($collectedActive + $collectedCleared)) {
        if (!(Test-BlankObjectRow $r)) { $groupsWithObjectRows[(Get-RowGroupKey $r)] = $true }
    }

    foreach ($c in @($collectedActive)) {
        $key = Clean (Get-Prop $c 'ObjectKey' '')
        $n = Clone-Row $c
        if ($previousOpenByKey.ContainsKey($key)) {
            $p = $previousOpenByKey[$key]
            Set-ObjProp $n 'FirstFailedET' (Clean (Get-Prop $p 'FirstFailedET' (Get-Prop $n 'FirstFailedET' '')))
            $priorStatus = Clean (Get-Prop $p 'Status' '')
            if ($priorStatus -eq 'ClearedByLaterSuccess' -or $priorStatus -eq 'NewlyClearedThisCheck') { Set-ObjProp $n 'Status' 'ReFailedAfterClear' }
            elseif ((Clean (Get-Prop $n 'Status' '')) -eq 'NewlyFailedThisCheck') { Set-ObjProp $n 'Status' 'OlderStillFailing' }
            Update-RowFailureFields $n (Merge-FailedRunKeys $p $n)
        }
        $current += $n
    }

    foreach ($c in @($collectedCleared)) {
        $n = Clone-Row $c
        $key = Clean (Get-Prop $n 'ObjectKey' '')
        if ($previousOpenByKey.ContainsKey($key)) {
            $p = $previousOpenByKey[$key]
            Set-ObjProp $n 'FirstFailedET' (Clean (Get-Prop $p 'FirstFailedET' (Get-Prop $n 'FirstFailedET' '')))
            Update-RowFailureFields $n (Merge-FailedRunKeys $p $n)
        }
        $clearedThisRun += $n
    }

    foreach ($p in @($PreviousOpenRows)) {
        $key = Clean (Get-Prop $p 'ObjectKey' '')
        if (!$key -or $currentByKey.ContainsKey($key)) { continue }

        # Do not carry stale PG/run-level rows forward when object-level evidence exists for the same PG/run type.
        if ((Test-BlankObjectRow $p) -and $groupsWithObjectRows.ContainsKey((Get-RowGroupKey $p))) { continue }

        # Do not carry old blank PG rows forward. The collector will emit a fresh no-object review row only when that condition is still true.
        if (Test-BlankObjectRow $p) { continue }

        if (@($collectedCleared | Where-Object { (Clean (Get-Prop $_ 'ObjectKey' '')) -eq $key }).Count -gt 0) { continue }
        $lastFailed = To-Int64 (Get-Prop $p 'LastFailedUsecs' 0)
        if ($lastFailed -le 0) { $lastFailed = Convert-EtTextToUsecs (Clean (Get-Prop $p 'LastFailedET' '')) }
        if ($SuccessIndex.ContainsKey($key) -and [int64]$SuccessIndex[$key].Usecs -gt $lastFailed) {
            $c = Clone-Row $p
            Set-ObjProp $c 'Status' 'NewlyClearedThisCheck'
            Set-ObjProp $c 'ClearedET' (Clean $SuccessIndex[$key].ET)
            Set-ObjProp $c 'LastSeenET' (Clean $SuccessIndex[$key].ET)
            Set-ObjProp $c 'LatestRunStatus' (Clean $SuccessIndex[$key].Status)
            $clearedThisRun += $c
        } else {
            $u = Clone-Row $p
            Set-ObjProp $u 'Status' 'CarriedForwardStillFailing'
            $current += $u
        }
    }

    $historicalCleared = @()
    foreach ($h in @($PreviousClearedRows)) {
        $x = Clone-Row $h
        if ($x) {
            if ((Clean (Get-Prop $x 'Status' '')) -eq 'NewlyClearedThisCheck') { Set-ObjProp $x 'Status' 'ClearedByLaterSuccess' }
            $historicalCleared += $x
        }
    }

    $allCleared = @(Merge-UniqueRowsByKey @($historicalCleared + $clearedThisRun))
    $lifecycle = @(Merge-UniqueRowsByKey @($current + $allCleared))
    [pscustomobject]@{
        Current = @($current)
        ClearedThisRun = @($clearedThisRun)
        AllCleared = @($allCleared)
        Lifecycle = @($lifecycle)
    }
}

function Merge-UniqueRowsByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = Clean (Get-Prop $r 'ObjectKey' '')
        if (!$k) { continue }
        if (!$h.ContainsKey($k)) { $h[$k] = $r }
        else {
            $oldUsecs = To-Int64 (Get-Prop $h[$k] 'LastFailedUsecs' 0)
            $newUsecs = To-Int64 (Get-Prop $r 'LastFailedUsecs' 0)
            if ($newUsecs -ge $oldUsecs) { $h[$k] = $r }
        }
    }
    @($h.Values)
}

function Convert-LifecycleRows($Rows) {
    foreach ($r in @($Rows)) {
        [pscustomobject]@{
            Cluster = Clean (Get-Prop $r 'Cluster' '')
            ProtectionGroup = Clean (Get-Prop $r 'ProtectionGroup' '')
            Environment = Clean (Get-Prop $r 'Environment' '')
            Host = Clean (Get-Prop $r 'Host' '')
            ObjectName = Clean (Get-Prop $r 'ObjectName' '')
            ObjectType = Clean (Get-Prop $r 'ObjectType' '')
            RunType = Clean (Get-Prop $r 'RunType' '')
            Status = Clean (Get-Prop $r 'Status' '')
            OldestFailedET = Clean (Get-Prop $r 'FirstFailedET' '')
            NewestFailedET = Clean (Get-Prop $r 'LastFailedET' '')
            LatestSuccessET = Clean (Get-Prop $r 'ClearedET' '')
            FailureRuns = Clean (Get-Prop $r 'ConsecutiveFailureCount' '')
            Message = Clean (Get-Prop $r 'Message' '')
        }
    }
}

function Format-Rows($Rows, [string[]]$Columns) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add(($Columns -join ' | '))
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add('- None')
        return ($lines -join [Environment]::NewLine)
    }
    foreach ($r in $list) {
        $values = @()
        foreach ($c in $Columns) {
            $prop = $r.PSObject.Properties[$c]
            if ($prop) { $values += (Clean $prop.Value) } else { $values += '' }
        }
        $lines.Add(($values -join ' | '))
    }
    $lines -join [Environment]::NewLine
}

function Get-OutputFolder($IncidentEntry) {
    $folder = Clean (Get-Prop $IncidentEntry 'OutputFolder' '')
    if (!$folder) { $folder = Join-Path $OutputRoot (Clean (Get-Prop $IncidentEntry 'IncidentNumber' '')) }
    if (!(Test-Path $folder)) { New-Item -Path $folder -ItemType Directory -Force | Out-Null }
    $folder
}

function Write-TextOutputs($Folder, $Incident, $Window, $LifecycleExport, $CurrentExport, $NewlyClearedExport, $PreviouslyClearedCount) {
    $failureText = Format-Rows $CurrentExport $script:FailureColumns
    $successText = Format-Rows $NewlyClearedExport $script:SuccessColumns
    $statusText = if ($script:Warnings.Count -gt 0) { 'Incomplete' } else { 'Complete' }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('Cohesity Backup Failure Incident Update')
    $lines.Add('')
    $lines.Add(('Incident: {0}' -f $Incident))
    $lines.Add(('Compute Window: {0}' -f $Window.WindowLabel))
    $lines.Add(('Generated At: {0} ET' -f $Window.GeneratedET))
    $lines.Add(('Cohesity API Collection Status: {0}' -f $statusText))
    $lines.Add(('Scope: latest {0} runs per protection group/run type.' -f $NumRuns))
    $lines.Add('')
    $lines.Add('Summary Counts:')
    $lines.Add(('- Active / unresolved failures: {0}' -f @($CurrentExport).Count))
    $lines.Add(('- Newly cleared this check: {0}' -f @($NewlyClearedExport).Count))
    $lines.Add(('- Previously cleared rows retained in lifecycle CSV: {0}' -f $PreviouslyClearedCount))
    $lines.Add(('- Total lifecycle rows tracked: {0}' -f @($LifecycleExport).Count))
    $lines.Add('')
    $lines.Add('Failure Section:')
    $lines.Add($failureText)
    $lines.Add('')
    $lines.Add('Success Section:')
    $lines.Add($successText)
    $lines.Add('')
    ($lines -join [Environment]::NewLine) | Set-Content -Path (Join-Path $Folder 'worknotes_summary.txt') -Encoding UTF8

    $closing = New-Object System.Collections.Generic.List[string]
    $closing.Add('Backup Failure Incident Closure Summary')
    $closing.Add('')
    $closing.Add(('Incident: {0}' -f $Incident))
    $closing.Add(('Compute Window: {0}' -f $Window.WindowLabel))
    $closing.Add(('Generated At: {0} ET' -f $Window.GeneratedET))
    $closing.Add('')
    $closing.Add('Failure Section:')
    $closing.Add($failureText)
    $closing.Add('')
    $closing.Add('Success Section:')
    $closing.Add($successText)
    $closing.Add('')
    ($closing -join [Environment]::NewLine) | Set-Content -Path (Join-Path $Folder 'closing_summary.txt') -Encoding UTF8
}

function Remove-TemporaryCollectorFiles([string]$Folder) {
    foreach ($name in @('worknotes.txt','summary.txt')) {
        $p = Join-Path $Folder $name
        if (Test-Path $p) { Remove-Item -Path $p -Force -ErrorAction SilentlyContinue }
    }
}

$window = Get-ComputeWindow
$incidentEntry = Resolve-IncidentLock $window
$incident = Clean (Get-Prop $incidentEntry 'IncidentNumber' '')
$outputFolder = Get-OutputFolder $incidentEntry
$statePath = Join-Path $outputFolder 'state.json'

$apiKey = Get-CohesityApiKey
$headers = @{ accept = 'application/json'; apiKey = $apiKey }
try {
    $clusterJson = Invoke-HeliosGetJson -Uri ('{0}/v2/mcm/cluster-mgmt/info' -f $BaseUrl) -Headers $headers
    $clusters = @($clusterJson.cohesityClusters)
} catch {
    throw ('Failed to query Helios clusters: {0}' -f $_.Exception.Message)
}
if (!$clusters -or $clusters.Count -eq 0) { throw 'No clusters returned from Helios.' }

if ($ClusterName) {
    $clusters = @($clusters | Where-Object {
        (Get-ClusterName $_) -eq $ClusterName -or
        (Clean (Get-Prop $_ 'name' '')) -eq $ClusterName -or
        (Clean (Get-Prop $_ 'clusterName' '')) -eq $ClusterName -or
        (Clean (Get-Prop $_ 'displayName' '')) -eq $ClusterName
    })
    if ($clusters.Count -eq 0) { throw ('Cluster not found: {0}' -f $ClusterName) }
}
$clusters = @($clusters | Sort-Object @{Expression={ Get-ClusterName $_ }})

Write-Host 'Processing clusters alphabetically.'
Write-Host ('Output folder: {0}' -f $outputFolder)
Write-Host ('NumRuns: {0}' -f $NumRuns)

$previousState = Read-Json $statePath
$previousOpen = @()
$previousCleared = @()
if ($previousState) {
    $previousOpen = Normalize-ExistingRows (As-Array (Get-Prop $previousState 'CurrentOpenFailures' @())) $incident $window
    $previousCleared = Normalize-ExistingRows (As-Array (Get-Prop $previousState 'ClearedBySuccess' @())) $incident $window
}

$collection = Collect-RunRows -Incident $incident -Window $window -Clusters $clusters -ApiKey $apiKey
$merged = Merge-Lifecycle -CollectedRows $collection.Rows -PreviousOpenRows $previousOpen -PreviousClearedRows $previousCleared -SuccessIndex $collection.SuccessIndex

$currentRows = @($merged.Current | Sort-Object Cluster,ProtectionGroup,Environment,@{Expression={Date-Sort (Get-Prop $_ 'LastFailedET' '')};Descending=$true})
$newlyClearedRows = @($merged.ClearedThisRun | Sort-Object @{Expression={Date-Sort (Get-Prop $_ 'ClearedET' '')};Descending=$true})
$allClearedRows = @($merged.AllCleared)
$lifecycleRows = @($merged.Lifecycle)

Write-Csv $currentRows (Join-Path $outputFolder 'current_failures.csv') $script:CsvColumns
Write-Csv $newlyClearedRows (Join-Path $outputFolder 'cleared_by_success.csv') $script:CsvColumns
Write-Csv $lifecycleRows (Join-Path $outputFolder 'incident_lifecycle_raw.csv') $script:CsvColumns

$lifecycleExport = @(Convert-LifecycleRows $lifecycleRows | Sort-Object Cluster,ProtectionGroup,Environment,@{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
$currentExport = @($lifecycleExport | Where-Object { Is-ActiveLifecycleStatus $_.Status } | Sort-Object @{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
$newlyClearedExport = @($lifecycleExport | Where-Object { $_.Status -eq 'NewlyClearedThisCheck' } | Sort-Object @{Expression={Date-Sort $_.LatestSuccessET};Descending=$true})
$previouslyClearedCount = @($lifecycleExport | Where-Object { $_.Status -eq 'ClearedByLaterSuccess' }).Count

Write-Csv $lifecycleExport (Join-Path $outputFolder 'incident_lifecycle.csv') $script:LifecycleColumns
Write-TextOutputs -Folder $outputFolder -Incident $incident -Window $window -LifecycleExport $lifecycleExport -CurrentExport $currentExport -NewlyClearedExport $newlyClearedExport -PreviouslyClearedCount $previouslyClearedCount

$state = [pscustomobject]@{
    IncidentNumber = $incident
    WindowKey = $window.WindowKey
    WindowLabel = $window.WindowLabel
    WindowStartET = $window.WindowStartET
    WindowEndET = $window.WindowEndET
    LastRunET = $window.GeneratedET
    NumRuns = $NumRuns
    RequestTimeoutSec = $RequestTimeoutSec
    ClusterName = $ClusterName
    Warnings = @($script:Warnings)
    CurrentOpenFailures = @($currentRows)
    ClearedBySuccess = @($allClearedRows)
    LastRunClearedBySuccess = @($newlyClearedRows)
    LifecycleRows = @($lifecycleRows)
}
Write-Json $state $statePath
Remove-TemporaryCollectorFiles $outputFolder

Write-Host ''
Write-Host 'Final Summary:'
Write-Host ('Cohesity API Collection Status : {0}' -f $(if ($script:Warnings.Count -gt 0) { 'Incomplete' } else { 'Complete' }))
Write-Host ('Active / Unresolved Failures   : {0}' -f $currentExport.Count)
Write-Host ('Newly Cleared This Check       : {0}' -f $newlyClearedExport.Count)
Write-Host ('Previously Cleared Retained    : {0}' -f $previouslyClearedCount)
Write-Host ('Total Lifecycle Rows           : {0}' -f $lifecycleExport.Count)
Write-Host ('Incomplete Collection Warnings : {0}' -f $script:Warnings.Count)
Write-Host ''
Write-Host 'Files Created:'
Write-Host (Join-Path $outputFolder 'worknotes_summary.txt')
Write-Host (Join-Path $outputFolder 'incident_lifecycle.csv')
Write-Host (Join-Path $outputFolder 'closing_summary.txt')
Write-Host (Join-Path $outputFolder 'cleared_by_success.csv')
Write-Host (Join-Path $outputFolder 'state.json')
