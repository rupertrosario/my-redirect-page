<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
GET-only Cohesity Helios collector for backup failure incident updates.
This version promotes failed objects from run.objects into the normal outputs.
If the diagnostic probe can see object.name/objectType, this collector must carry the same object into current_failures.csv, incident_lifecycle.csv, and worknotes_summary.txt.

Object-selection model:
- Object-level failedAttempts/status/error evidence is treated as a failed object even when the parent run is not simply Failed.
- Failed run + failed object evidence => object-level row.
- Failed run + objects returned but no explicit failed object evidence => object-level review rows.
- Failed run + no objects returned => blank object run-level review row.
- A previously failed object is suppressed when the same object has a later successful object backup.
- ProtectionGroup name is never copied into ObjectName.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow',
    [string]$LegacyFailureOutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailures',
    [string]$HelperPath = ('X:\PowerShell\Cohesity_API_Scripts\Common\' + 'Api' + 'KeyAesHelper.ps1'),
    [string]$EncryptedFile = ('X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_' + 'api' + 'key.enc'),
    [string]$ClusterName = '',
    [int]$NumRuns = 30,
    [string]$IncidentNumber = '',
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = '',
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35,
    [int]$RequestTimeoutSec = 60
)

$ErrorActionPreference = 'Stop'
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:CsvColumns = @('IncidentNumber','WindowKey','Status','Cluster','Environment','ProtectionGroup','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','ClearedET','LastSeenET','LatestRunStatus','ConsecutiveFailureCount','Message','ObjectKey','ClusterId','ProtectionGroupId','EnvironmentFilter','FailedRunKeys')
$script:LifecycleColumns = @('Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','Status','OldestFailedET','NewestFailedET','LatestSuccessET','FailureRuns','Message')
$script:FailureColumns = @('Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','Status','OldestFailedET','NewestFailedET','LatestSuccessET','FailureRuns','Message')
$script:SuccessColumns = @('Cluster','ProtectionGroup','Environment','RunType','LatestSuccessET')

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

function Get-NowEtDate {
    [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
}

function Get-NowEtText {
    (Get-NowEtDate).ToString('yyyy-MM-dd HH:mm:ss')
}

function Convert-UsecsToEtText($Usecs) {
    $u = To-Int64 $Usecs
    if ($u -le 0) { return '' }
    try {
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch {
        return ''
    }
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

function Save-WindowRegistry($Registry) {
    Write-Json $Registry (Get-RegistryPath)
}

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
        [pscustomobject]@{ Label='Oracle';        Filter='kOracle';        ParentHostNeeded=$true  },
        [pscustomobject]@{ Label='SQL';           Filter='kSQL';           ParentHostNeeded=$true  },
        [pscustomobject]@{ Label='Physical';      Filter='kPhysical';      ParentHostNeeded=$false },
        [pscustomobject]@{ Label='GenericNas';    Filter='kGenericNas';    ParentHostNeeded=$false },
        [pscustomobject]@{ Label='HyperV';        Filter='kHyperV';        ParentHostNeeded=$false },
        [pscustomobject]@{ Label='Acropolis';     Filter='kAcropolis';     ParentHostNeeded=$false },
        [pscustomobject]@{ Label='RemoteAdapter'; Filter='kRemoteAdapter'; ParentHostNeeded=$false },
        [pscustomobject]@{ Label='Isilon';        Filter='kIsilon';        ParentHostNeeded=$false }
    )
}

function Is-FailedStatus([string]$Status) { (Clean $Status) -in @('Failed','kFailed','Failure','kFailure','Error','kError') }
function Is-SuccessStatus([string]$Status) { (Clean $Status) -in @('Succeeded','SucceededWithWarning','kSucceeded','kSucceededWithWarning') }
function Is-RunningStatus([string]$Status) { (Clean $Status) -in @('Running','kRunning','Accepted','kAccepted','Queued','kQueued') }
function Is-CancelledStatus([string]$Status) { (Clean $Status) -in @('Canceled','Cancelled','kCanceled','kCancelled','Canceling','kCanceling') }
function Is-ActiveLifecycleStatus([string]$Status) { (Clean $Status) -in @('NewlyFailedThisCheck','OlderStillFailing','CurrentStillFailing','CarriedForwardStillFailing','ReFailedAfterClear','RunningAtLatestCheck','CancelledAfterFailure','UnknownNeedsReview') }

function Get-FirstLocalBackupInfo($Run) {
    @(As-Array (Get-Prop $Run 'localBackupInfo' @()) | Select-Object -First 1)[0]
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
        foreach ($snap in As-Array (Get-Prop $lsi 'snapshotInfo' @())) {
            $attempts += @(As-Array (Get-Prop $snap 'failedAttempts' @()))
        }
    }
    foreach ($snap in As-Array (Get-Prop $RunObject 'snapshotInfo' @())) {
        $attempts += @(As-Array (Get-Prop $snap 'failedAttempts' @()))
    }
    @($attempts | Where-Object { $_ })
}

function Get-FailureMessage($RunObject) {
    $messages = @()
    foreach ($attempt in Get-FailedAttempts $RunObject) {
        foreach ($field in @('message','error','reason','errorMessage','failureMessage')) {
            $m = Clean (Get-Prop $attempt $field '')
            if ($m) { $messages += $m }
        }
    }
    foreach ($container in @($RunObject, (Get-Prop $RunObject 'object' $null))) {
        if ($null -eq $container) { continue }
        foreach ($field in @('error','message','messages','errorMessage','failureMessage','reason','lastError','lastFailureMessage')) {
            $m = Clean (Get-Prop $container $field '')
            if ($m) { $messages += $m }
        }
    }
    foreach ($lsi in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        foreach ($field in @('error','message','messages','errorMessage','failureMessage','reason','lastError','lastFailureMessage','status')) {
            $m = Clean (Get-Prop $lsi $field '')
            if ($m -and (Is-FailedStatus $m)) { $messages += ('Object local snapshot status: {0}' -f $m) }
            elseif ($m -and $field -ne 'status') { $messages += $m }
        }
        foreach ($snap in As-Array (Get-Prop $lsi 'snapshotInfo' @())) {
            foreach ($field in @('error','message','messages','errorMessage','failureMessage','reason','lastError','lastFailureMessage','status')) {
                $m = Clean (Get-Prop $snap $field '')
                if ($m -and (Is-FailedStatus $m)) { $messages += ('Object snapshot status: {0}' -f $m) }
                elseif ($m -and $field -ne 'status') { $messages += $m }
            }
        }
    }
    Clean (($messages | Where-Object { $_ } | Select-Object -Unique) -join ' | ')
}

function Get-ObjectFailureEvidence($RunObject) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject 'object' $null)) {
        return [pscustomobject]@{ HasFailure = $false; Message = '' }
    }

    $statuses = @()
    $statuses += Clean (Get-Prop $RunObject 'status' '')
    $obj = Get-Prop $RunObject 'object' $null
    $statuses += Clean (Get-Prop $obj 'status' '')
    foreach ($lsi in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        $statuses += Clean (Get-Prop $lsi 'status' '')
        foreach ($snap in As-Array (Get-Prop $lsi 'snapshotInfo' @())) {
            $statuses += Clean (Get-Prop $snap 'status' '')
        }
    }

    $attempts = @(Get-FailedAttempts $RunObject)
    $msg = Get-FailureMessage $RunObject
    $hasFailure = ($attempts.Count -gt 0) -or (@($statuses | Where-Object { Is-FailedStatus $_ }).Count -gt 0) -or [bool]$msg

    [pscustomobject]@{
        HasFailure = [bool]$hasFailure
        Message = $msg
    }
}

function Get-ObjectKey($RunObject, [string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId, [string]$ProtectionGroupName) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject 'object' $null)) { return '' }
    $obj = Get-Prop $RunObject 'object' $null
    $objId = Clean (Get-Prop $obj 'id' '')
    if ($objId) { return ('{0}|{1}|OBJECTID|{2}' -f $ClusterId, $EnvironmentLabel, $objId) }
    $env = Clean (Get-Prop $obj 'environment' '')
    $type = Clean (Get-Prop $obj 'objectType' '')
    $name = Clean (Get-Prop $obj 'name' '')
    $sourceId = Clean (Get-Prop $obj 'sourceId' '')
    return ('{0}|{1}|{2}|{3}|{4}|{5}' -f $ClusterId, $EnvironmentLabel, $env, $type, $name, $sourceId)
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
    } catch {
        return 0
    }
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
    @($pgs | Sort-Object -Property name,id -Unique)
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

function Get-ObjectNameMap($Runs) {
    $m = @{}
    foreach ($run in $Runs) {
        foreach ($ro in (As-Array (Get-Prop $run 'objects' @()))) {
            $obj = Get-Prop $ro 'object' $null
            $id = Clean (Get-Prop $obj 'id' '')
            $name = Clean (Get-Prop $obj 'name' '')
            if ($id -and $name) { $m[$id] = $name }
        }
    }
    $m
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

function Collect-CurrentObjectFailures($Incident, $Window, $Clusters, [string]$ApiKey) {
    $rows = @()
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
            $before = $rows.Count
            $pgsChecked = 0
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

                $groups = @($runs | Group-Object -Property {
                    $i = Get-FirstLocalBackupInfo $_
                    $rt = Clean (Get-Prop $i 'runType' '')
                    if ($rt) { $rt } else { 'Unknown' }
                })

                foreach ($group in $groups) {
                    $runType = Clean $group.Name
                    $runsForType = @($group.Group | Sort-Object { Get-RunEffectiveUsecs $_ } -Descending)
                    if ($runsForType.Count -eq 0) { continue }

                    $latestInfo = Get-FirstLocalBackupInfo $runsForType[0]
                    $latestRunStatus = Clean (Get-Prop $latestInfo 'status' '')
                    $latestRunUsecs = Get-RunEffectiveUsecs $runsForType[0]
                    $objectNameById = Get-ObjectNameMap $runsForType
                    $cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $latestByKey = @{}
                    $failedKeysByKey = @{}
                    $runLevelKey = Get-RunLevelKey $clusterId $env.Label $pgId $pgName $runType

                    foreach ($run in $runsForType) {
                        $info = Get-FirstLocalBackupInfo $run
                        if (!$info) { continue }

                        $status = Clean (Get-Prop $info 'status' '')
                        $startUsecs = To-Int64 (Get-Prop $info 'startTimeUsecs' 0)
                        $endUsecs = To-Int64 (Get-Prop $info 'endTimeUsecs' 0)
                        $effectiveUsecs = if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs }

                        $objectsAll = @(As-Array (Get-Prop $run 'objects' @()) | Where-Object { $_ -and (Get-Prop $_ 'object' $null) })
                        $candidateObjects = @($objectsAll | Where-Object { (Get-ObjectFailureEvidence $_).HasFailure })

                        if ((Is-SuccessStatus $status) -and $candidateObjects.Count -eq 0) {
                            if ($objectsAll.Count -eq 0) {
                                [void]$cleared.Add($runLevelKey)
                                Add-SuccessIndex $successIndex $runLevelKey $effectiveUsecs $status
                            }
                            foreach ($ob in $objectsAll) {
                                $ck = Get-ObjectKey $ob $clusterId $env.Label $pgId $pgName
                                if ($ck) {
                                    [void]$cleared.Add($ck)
                                    Add-SuccessIndex $successIndex $ck $effectiveUsecs $status
                                }
                            }
                            continue
                        }

                        if ((!(Is-FailedStatus $status)) -and $candidateObjects.Count -eq 0) { continue }

                        $reviewObjects = @()
                        if ($candidateObjects.Count -eq 0 -and (Is-FailedStatus $status) -and $objectsAll.Count -gt 0) {
                            $reviewObjects = @($objectsAll)
                        }

                        $objectsToWrite = @($candidateObjects + $reviewObjects)
                        $foundObjectFailure = $false

                        foreach ($ob in $objectsToWrite) {
                            $obj = Get-Prop $ob 'object' $null
                            if ($null -eq $obj) { continue }

                            $ok = Get-ObjectKey $ob $clusterId $env.Label $pgId $pgName
                            if (!$ok -or $cleared.Contains($ok)) { continue }

                            $evidence = Get-ObjectFailureEvidence $ob
                            $msg = Clean $evidence.Message
                            if (!$msg) {
                                if ($candidateObjects.Count -eq 0 -and $objectsAll.Count -gt 0) {
                                    $msg = 'Run marked Failed; Cohesity returned this object without explicit failedAttempts/status/error evidence'
                                } else {
                                    $msg = 'Object-level failure evidence found in run.objects'
                                }
                            }

                            $objType = Clean (Get-Prop $obj 'objectType' '')
                            $objName = Clean (Get-Prop $obj 'name' '')
                            $hostName = ''
                            if ($env.ParentHostNeeded) {
                                $sourceId = Clean (Get-Prop $obj 'sourceId' '')
                                if ($sourceId -and $objectNameById.ContainsKey($sourceId)) { $hostName = $objectNameById[$sourceId] }
                                if ($objType -eq 'kHost' -or (Clean (Get-Prop $obj 'environment' '')) -eq 'kPhysical') { $hostName = $objName }
                            }

                            $rowStatus = 'NewlyFailedThisCheck'
                            if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = 'RunningAtLatestCheck' }
                            elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = 'CancelledAfterFailure' }
                            elseif ($reviewObjects.Count -gt 0 -and $candidateObjects.Count -eq 0) { $rowStatus = 'UnknownNeedsReview' }

                            $runKey = ('{0}|{1}|{2}|{3}|{4}' -f $clusterId, $pgId, $ok, $runType, $effectiveUsecs)
                            Add-FailureRunKey $failedKeysByKey $ok $runKey
                            if (!$latestByKey.ContainsKey($ok)) {
                                $latestByKey[$ok] = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroupName $pgName -ProtectionGroupId $pgId -ObjectKey $ok -HostName $hostName -ObjectName $objName -ObjectType $objType -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                            $foundObjectFailure = $true
                        }

                        if (!$foundObjectFailure -and $objectsAll.Count -eq 0 -and (Is-FailedStatus $status) -and !$cleared.Contains($runLevelKey)) {
                            $msg = Clean (Get-Prop $info 'messages' '')
                            if (!$msg) { $msg = 'Run marked failed; no object-level details returned' }
                            $rowStatus = 'UnknownNeedsReview'
                            if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = 'RunningAtLatestCheck' }
                            elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = 'CancelledAfterFailure' }
                            $runKey = ('{0}|{1}|{2}|{3}|{4}' -f $clusterId, $pgId, $runLevelKey, $runType, $effectiveUsecs)
                            Add-FailureRunKey $failedKeysByKey $runLevelKey $runKey
                            if (!$latestByKey.ContainsKey($runLevelKey)) {
                                $latestByKey[$runLevelKey] = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroupName $pgName -ProtectionGroupId $pgId -ObjectKey $runLevelKey -HostName '' -ObjectName '' -ObjectType '' -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                        }
                    }

                    foreach ($k in $latestByKey.Keys) {
                        if ($failedKeysByKey.ContainsKey($k)) { Update-RowFailureFields $latestByKey[$k] @($failedKeysByKey[$k]) }
                        $rows += $latestByKey[$k]
                    }
                }
            }
            $envFailures = $rows.Count - $before
            Write-Host ('  {0,-13}: PGs checked: {1} | failures: {2}' -f $env.Label, $pgsChecked, $envFailures)
        }
    }

    [pscustomobject]@{
        CurrentFailures = @(Merge-UniqueRowsByKey $rows)
        SuccessIndex = $successIndex
    }
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

function Merge-Lifecycle($CurrentRows, $PreviousOpenRows, $PreviousClearedRows, [hashtable]$SuccessIndex) {
    $current = @()
    $clearedThisRun = @()
    $currentByKey = Index-ByKey $CurrentRows
    $previousOpenByKey = Index-ByKey $PreviousOpenRows

    foreach ($c in @($CurrentRows)) {
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

    foreach ($p in @($PreviousOpenRows)) {
        $key = Clean (Get-Prop $p 'ObjectKey' '')
        if (!$key -or $currentByKey.ContainsKey($key)) { continue }
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

function Convert-LifecycleRows($Rows) {
    foreach ($r in @($Rows)) {
        $objectName = Clean (Get-Prop $r 'ObjectName' '')
        $objectType = Clean (Get-Prop $r 'ObjectType' '')
        if ($objectType -eq 'ProtectionGroup') { $objectName = ''; $objectType = '' }
        [pscustomobject]@{
            Cluster = Clean (Get-Prop $r 'Cluster' '')
            ProtectionGroup = Clean (Get-Prop $r 'ProtectionGroup' '')
            Environment = Clean (Get-Prop $r 'Environment' '')
            Host = Clean (Get-Prop $r 'Host' '')
            ObjectName = $objectName
            ObjectType = $objectType
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
    $successText = Format-Rows (@($NewlyClearedExport | Select-Object -Property $script:SuccessColumns)) $script:SuccessColumns
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('Cohesity Backup Failure Incident Update')
    $lines.Add('')
    $lines.Add(('Incident: {0}' -f $Incident))
    $lines.Add(('Compute Window: {0}' -f $Window.WindowLabel))
    $lines.Add(('Generated At: {0} ET' -f $Window.GeneratedET))
    $lines.Add(('Cohesity API Collection Status: {0}' -f $(if ($script:Warnings.Count -gt 0) { 'Incomplete' } else { 'Complete' })))
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

$previousState = Read-Json $statePath
$previousOpen = @()
$previousCleared = @()
if ($previousState) {
    $previousOpen = Normalize-ExistingRows (As-Array (Get-Prop $previousState 'CurrentOpenFailures' @())) $incident $window
    $previousCleared = Normalize-ExistingRows (As-Array (Get-Prop $previousState 'ClearedBySuccess' @())) $incident $window
}

$collection = Collect-CurrentObjectFailures -Incident $incident -Window $window -Clusters $clusters -ApiKey $apiKey
$merged = Merge-Lifecycle -CurrentRows $collection.CurrentFailures -PreviousOpenRows $previousOpen -PreviousClearedRows $previousCleared -SuccessIndex $collection.SuccessIndex

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
Write-Host (Join-Path $outputFolder 'state.json')
