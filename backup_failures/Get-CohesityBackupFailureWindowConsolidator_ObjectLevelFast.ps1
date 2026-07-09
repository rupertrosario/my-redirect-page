<#
.SYNOPSIS
Experimental fast object-level Cohesity backup failure collector.

.DESCRIPTION
This file is intentionally separate from the current working production collector.
It does not replace:
  - Cohesity_Backup_Failure_INC_Status_Update.ps1
  - Get-CohesityBackupFailureWindowConsolidator.ps1

Design:
  - Protection Group is context only.
  - Object is the decision point.
  - PG/run status is used only as a lightweight filter to decide whether object details are needed.
  - RemoteAdapter is excluded.
  - OutputRoot is separate by default to avoid overwriting the current production reports.

Main status values are operator-facing only:
  Failure, Success, Cancelled, Running

Lifecycle/change tracking is kept in a separate Change column:
  New, Existing, CarriedForward, Cleared, PreviouslyCleared
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [string]$OutputRoot = 'X:/PowerShell/Data/Cohesity/BackupFailureWindow_ObjectLevelFast',
    [string]$HelperPath = 'X:/PowerShell/Cohesity_API_Scripts/Common/ApiKeyAesHelper.ps1',
    [string]$EncryptedFile = 'X:/PowerShell/Cohesity_API_Scripts/Common/Secure/cohesity_apikey.enc',
    [string]$ClusterName = '',
    [string]$IncidentNumber = '',
    [int]$NumRuns = 20,
    [int]$RequestTimeoutSec = 60,
    [switch]$ResetState
)

$ErrorActionPreference = 'Stop'
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:Columns = @('IncidentNumber','Status','Change','Cluster','Environment','ProtectionGroup','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','LatestSuccessET','LastSeenET','Message','ObjectKey','ClusterId','ProtectionGroupId','EnvironmentFilter','FailureRuns')
$script:LifecycleColumns = @('Status','Change','Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','LatestSuccessET','Message')

function Clean($Value) {
    if ($null -eq $Value) { return '' }
    if ($Value -is [array]) { $Value = @($Value) -join ' | ' }
    $t = [string]$Value
    $t = $t.Replace([char]13, ' ').Replace([char]10, ' ')
    while ($t.Contains('  ')) { $t = $t.Replace('  ', ' ') }
    return $t.Replace([char]34, [char]39).Trim()
}

function AsArray($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function GetProp($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
    if ($Object -is [hashtable]) {
        if ($Object.ContainsKey($Name)) { return $Object[$Name] }
        return $Default
    }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { return $p.Value }
    return $Default
}

function AddWarning([string]$Message) {
    $m = Clean $Message
    if ($m) {
        $script:Warnings.Add($m) | Out-Null
        Write-Warning $m
    }
}

function ToInt64($Value) {
    try {
        $s = Clean $Value
        if (!$s) { return [int64]0 }
        return [int64]$s
    } catch { return [int64]0 }
}

function GetEtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
}
$script:EtZone = GetEtZone

function UsecsToEt($Usecs) {
    $u = ToInt64 $Usecs
    if ($u -le 0) { return '' }
    try {
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch { return '' }
}

function NowEtText { ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)).ToString('yyyy-MM-dd HH:mm:ss') }

function ParseEt($Text) {
    $t = Clean $Text
    if (!$t) { return $null }
    try { return [datetime]::Parse($t) } catch { return $null }
}

function DateSort($Text) {
    $d = ParseEt $Text
    if ($d) { return $d.ToString('yyyy-MM-dd HH:mm:ss') }
    return '0000-00-00 00:00:00'
}

function InvokeHeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec $RequestTimeoutSec
    } else {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $RequestTimeoutSec
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function GetApiKey {
    if (!(Test-Path $HelperPath)) { throw ('API key helper not found: {0}' -f $HelperPath) }
    if (!(Test-Path $EncryptedFile)) { throw ('Encrypted API key not found: {0}' -f $EncryptedFile) }
    . $HelperPath
    $k = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($k)) { throw 'API key helper returned blank key.' }
    return $k.Trim()
}

function ReadJson([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    try {
        $raw = Get-Content -Path $Path -Raw
        if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
        return ($raw | ConvertFrom-Json)
    } catch { return $null }
}

function WriteJson($Object, [string]$Path) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function WriteCsv($Rows, [string]$Path, [string[]]$Columns) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $list = @($Rows)
    if ($list.Count -eq 0) {
        ($Columns -join ',') | Set-Content -Path $Path -Encoding UTF8
    } else {
        $list | Select-Object -Property $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function GetClusterName($Cluster) {
    $n = Clean (GetProp $Cluster 'name' '')
    if (!$n) { $n = Clean (GetProp $Cluster 'clusterName' '') }
    if (!$n) { $n = Clean (GetProp $Cluster 'displayName' '') }
    if (!$n) { $n = 'Unknown-' + (Clean (GetProp $Cluster 'clusterId' '')) }
    return $n
}

function GetEnvironments {
    @(
        [pscustomobject]@{ Label='Oracle';     Filter='kOracle';     TargetType='kDatabase';       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label='SQL';        Filter='kSQL';        TargetType='kDatabase';       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label='Physical';   Filter='kPhysical';   TargetType='kHost';           ParentHostNeeded=$false },
        [pscustomobject]@{ Label='GenericNas'; Filter='kGenericNas'; TargetType='kHost';           ParentHostNeeded=$false },
        [pscustomobject]@{ Label='HyperV';     Filter='kHyperV';     TargetType='kVirtualMachine'; ParentHostNeeded=$false },
        [pscustomobject]@{ Label='Acropolis';  Filter='kAcropolis';  TargetType='kVirtualMachine'; ParentHostNeeded=$false },
        [pscustomobject]@{ Label='Isilon';     Filter='kIsilon';     TargetType='kHost';           ParentHostNeeded=$false }
    )
}

function IsFailed($Status) { (Clean $Status) -in @('Failed','kFailed','Failure','kFailure','Error','kError') }
function IsSuccess($Status) { (Clean $Status) -in @('Succeeded','kSucceeded') }
function IsSuccessWarning($Status) { (Clean $Status) -in @('SucceededWithWarning','kSucceededWithWarning') }
function IsRunning($Status) { (Clean $Status) -in @('Running','kRunning','Accepted','kAccepted','Queued','kQueued') }
function IsCancelled($Status) { (Clean $Status) -in @('Canceled','Cancelled','kCanceled','kCancelled','Canceling','kCanceling') }

function GetRunInfo($Run) {
    $arr = @(AsArray (GetProp $Run 'localBackupInfo' @()))
    if ($arr.Count -gt 0) { return $arr[0] }
    return $null
}

function GetRunUsecs($Run) {
    $i = GetRunInfo $Run
    if (!$i) { return 0 }
    $e = ToInt64 (GetProp $i 'endTimeUsecs' 0)
    if ($e -gt 0) { return $e }
    return (ToInt64 (GetProp $i 'startTimeUsecs' 0))
}

function GetRunStatus($Run) {
    $i = GetRunInfo $Run
    if (!$i) { return '' }
    return (Clean (GetProp $i 'status' ''))
}

function GetRunType($Run) {
    $i = GetRunInfo $Run
    if (!$i) { return '' }
    return (Clean (GetProp $i 'runType' ''))
}

function HasRunMessage($Run) {
    $i = GetRunInfo $Run
    if (!$i) { return $false }
    foreach ($f in @('messages','message','error','errorMessage','failureMessage','reason')) {
        if (Clean (GetProp $i $f '')) { return $true }
    }
    return $false
}

function IsSuspiciousRun($Run) {
    $s = GetRunStatus $Run
    if (IsFailed $s) { return $true }
    if (IsRunning $s) { return $true }
    if (IsCancelled $s) { return $true }
    if (IsSuccessWarning $s) { return $true }
    if (HasRunMessage $Run) { return $true }
    return $false
}

function GetPgId($Pg) {
    $id = Clean (GetProp $Pg 'id' '')
    if (!$id) { $id = Clean (GetProp $Pg 'protectionGroupId' '') }
    return $id
}

function GetPgName($Pg) {
    $n = Clean (GetProp $Pg 'name' '')
    if (!$n) { $n = Clean (GetProp $Pg 'protectionGroupName' '') }
    if (!$n) { $n = Clean (GetProp $Pg 'displayName' '') }
    return $n
}

function GetPgKey([string]$ClusterId, [string]$EnvLabel, [string]$PgId) {
    return ('{0}|{1}|{2}' -f $ClusterId,$EnvLabel,$PgId)
}

function GetObjectKey($RunObject, [string]$ClusterId, [string]$PgId, [string]$EnvLabel, [string]$RunType) {
    $o = GetProp $RunObject 'object' $null
    if (!$o) { return '' }
    $oEnv = Clean (GetProp $o 'environment' '')
    $oType = Clean (GetProp $o 'objectType' '')
    $oName = Clean (GetProp $o 'name' '')
    $oId = Clean (GetProp $o 'id' '')
    $sourceId = Clean (GetProp $o 'sourceId' '')
    return ('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}' -f $ClusterId,$PgId,$EnvLabel,$RunType,$oEnv,$oType,$oName,($oId + '|' + $sourceId))
}

function GetFailedAttempts($RunObject) {
    $out = @()
    foreach ($lsi in AsArray (GetProp $RunObject 'localSnapshotInfo' @())) {
        $out += @(AsArray (GetProp $lsi 'failedAttempts' @()))
        foreach ($snap in AsArray (GetProp $lsi 'snapshotInfo' @())) {
            $out += @(AsArray (GetProp $snap 'failedAttempts' @()))
        }
    }
    foreach ($snap in AsArray (GetProp $RunObject 'snapshotInfo' @())) {
        $out += @(AsArray (GetProp $snap 'failedAttempts' @()))
    }
    return @($out | Where-Object { $_ })
}

function GetObjectStatuses($RunObject) {
    $st = @()
    $st += Clean (GetProp $RunObject 'status' '')
    $obj = GetProp $RunObject 'object' $null
    if ($obj) { $st += Clean (GetProp $obj 'status' '') }
    foreach ($lsi in AsArray (GetProp $RunObject 'localSnapshotInfo' @())) {
        $st += Clean (GetProp $lsi 'status' '')
        foreach ($snap in AsArray (GetProp $lsi 'snapshotInfo' @())) {
            $st += Clean (GetProp $snap 'status' '')
        }
    }
    return @($st | Where-Object { $_ })
}

function GetObjectMessage($RunObject, $RunInfo) {
    $msgs = @()
    foreach ($fa in GetFailedAttempts $RunObject) {
        foreach ($f in @('message','error','reason','errorMessage','failureMessage')) {
            $m = Clean (GetProp $fa $f '')
            if ($m) { $msgs += $m }
        }
    }
    foreach ($container in @($RunObject, (GetProp $RunObject 'object' $null), $RunInfo)) {
        if (!$container) { continue }
        foreach ($f in @('message','messages','error','reason','errorMessage','failureMessage','lastError')) {
            $m = Clean (GetProp $container $f '')
            if ($m) { $msgs += $m }
        }
    }
    return (Clean (($msgs | Where-Object { $_ } | Select-Object -Unique) -join ' | '))
}

function GetObjectState($RunObject, [string]$RunStatus) {
    if (@(GetFailedAttempts $RunObject).Count -gt 0) { return 'Failure' }
    $statuses = @(GetObjectStatuses $RunObject)
    if (@($statuses | Where-Object { IsFailed $_ }).Count -gt 0) { return 'Failure' }
    if (@($statuses | Where-Object { IsCancelled $_ }).Count -gt 0) { return 'Cancelled' }
    if (@($statuses | Where-Object { IsRunning $_ }).Count -gt 0) { return 'Running' }
    if (IsCancelled $RunStatus) { return 'Cancelled' }
    if (IsRunning $RunStatus) { return 'Running' }
    return 'Success'
}

function GetParentHost($RunObject, [hashtable]$IdToName, [hashtable]$HostById) {
    $obj = GetProp $RunObject 'object' $null
    if (!$obj) { return '' }
    $sid = Clean (GetProp $obj 'sourceId' '')
    if ($sid -and $IdToName.ContainsKey($sid)) { return Clean $IdToName[$sid] }
    if ($sid -and $HostById.ContainsKey($sid)) { return Clean $HostById[$sid] }
    $otype = Clean (GetProp $obj 'objectType' '')
    $oenv = Clean (GetProp $obj 'environment' '')
    if ($otype -eq 'kHost' -or $oenv -eq 'kPhysical') { return Clean (GetProp $obj 'name' '') }
    return ''
}

function TestTargetObject($RunObject, $Env, [string[]]$FilterSet) {
    $obj = GetProp $RunObject 'object' $null
    if (!$obj) { return $false }
    $otype = Clean (GetProp $obj 'objectType' '')
    $oenv = Clean (GetProp $obj 'environment' '')
    if ($Env.Label -in @('GenericNas','Isilon')) { return $true }
    if ($otype -ne $Env.TargetType) { return $false }
    if (!$oenv) { return $true }
    return ($FilterSet -contains $oenv)
}

function NewRow($Incident, $Status, $Change, $ClusterName, $ClusterId, $Env, $PgName, $PgId, $Host, $ObjectName, $ObjectType, $RunType, $FirstFailedUsecs, $LastFailedUsecs, $SuccessUsecs, $SeenUsecs, $Message, $ObjectKey, $FailureRuns) {
    [pscustomobject]@{
        IncidentNumber = Clean $Incident
        Status = Clean $Status
        Change = Clean $Change
        Cluster = Clean $ClusterName
        Environment = Clean $Env.Label
        ProtectionGroup = Clean $PgName
        Host = Clean $Host
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = UsecsToEt $FirstFailedUsecs
        LastFailedET = UsecsToEt $LastFailedUsecs
        LatestSuccessET = UsecsToEt $SuccessUsecs
        LastSeenET = UsecsToEt $SeenUsecs
        Message = Clean $Message
        ObjectKey = Clean $ObjectKey
        ClusterId = Clean $ClusterId
        ProtectionGroupId = Clean $PgId
        EnvironmentFilter = Clean $Env.Filter
        FailureRuns = Clean $FailureRuns
    }
}

function MergeByObjectKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = Clean (GetProp $r 'ObjectKey' '')
        if (!$k) { continue }
        if (!$h.ContainsKey($k)) { $h[$k] = $r }
        else {
            $old = DateSort (GetProp $h[$k] 'LastSeenET' '')
            $new = DateSort (GetProp $r 'LastSeenET' '')
            if ($new -ge $old) { $h[$k] = $r }
        }
    }
    return @($h.Values)
}

function IndexPreviousOpenByPg($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $pgKey = GetPgKey (Clean (GetProp $r 'ClusterId' '')) (Clean (GetProp $r 'Environment' '')) (Clean (GetProp $r 'ProtectionGroupId' ''))
        if (!$h.ContainsKey($pgKey)) { $h[$pgKey] = @() }
        $h[$pgKey] = @($h[$pgKey] + $r)
    }
    return $h
}

function GetProtectionGroups($Cluster, $Env, $ApiKey) {
    $clusterId = Clean (GetProp $Cluster 'clusterId' '')
    $headers = @{ accept='application/json'; apiKey=$ApiKey; accessClusterId=$clusterId }
    $all = @()
    foreach ($filter in @($Env.Filter.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        try {
            $uri = ('{0}/v2/data-protect/protection-groups?environments={1}&isDeleted=false&isPaused=false&isActive=true' -f $BaseUrl,$filter)
            $json = InvokeHeliosGetJson $uri $headers
            if ($json -and $json.protectionGroups) { $all += @($json.protectionGroups) }
        } catch {
            AddWarning ('PG lookup failed: {0} / {1} / {2}' -f (GetClusterName $Cluster),$filter,$_.Exception.Message)
        }
    }
    return @($all | Sort-Object -Property id -Unique)
}

function GetRuns($Cluster, [string]$PgId, [bool]$WithObjectDetails, $ApiKey) {
    $clusterId = Clean (GetProp $Cluster 'clusterId' '')
    $headers = @{ accept='application/json'; apiKey=$ApiKey; accessClusterId=$clusterId }
    $escapedPgId = [uri]::EscapeDataString($PgId)
    if ($WithObjectDetails) {
        $uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false&includeObjectDetails=true' -f $BaseUrl,$escapedPgId,$NumRuns)
    } else {
        $uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false' -f $BaseUrl,$escapedPgId,$NumRuns)
    }
    $json = InvokeHeliosGetJson $uri $headers
    if ($json -and $json.runs) { return @($json.runs) }
    return @()
}

function ProcessDetailedRuns($Runs, $Incident, $ClusterName, $ClusterId, $Env, $PgName, $PgId, $PreviousRowsForPg) {
    $active = @()
    $cleared = @()
    $successIndex = @{}
    $seenState = @{}
    $failureRunCount = @{}
    $idToName = @{}
    $hostById = @{}
    $filterSet = @($Env.Filter.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })

    foreach ($run in $Runs) {
        foreach ($ro in AsArray (GetProp $run 'objects' @())) {
            $obj = GetProp $ro 'object' $null
            if (!$obj) { continue }
            $id = Clean (GetProp $obj 'id' '')
            $name = Clean (GetProp $obj 'name' '')
            $otype = Clean (GetProp $obj 'objectType' '')
            $oenv = Clean (GetProp $obj 'environment' '')
            if ($id -and $name -and !$idToName.ContainsKey($id)) { $idToName[$id] = $name }
            if ($id -and $name -and ($otype -eq 'kHost' -or $oenv -eq 'kPhysical')) { $hostById[$id] = $name }
        }
    }

    $runsSorted = @($Runs | Sort-Object { GetRunUsecs $_ } -Descending)
    foreach ($run in $runsSorted) {
        $info = GetRunInfo $run
        if (!$info) { continue }
        $runStatus = GetRunStatus $run
        $runType = GetRunType $run
        $usecs = GetRunUsecs $run
        if (!$runType) { $runType = 'Unknown' }

        foreach ($ro in AsArray (GetProp $run 'objects' @())) {
            if (!(TestTargetObject $ro $Env $filterSet)) { continue }
            $obj = GetProp $ro 'object' $null
            if (!$obj) { continue }
            $key = GetObjectKey $ro $ClusterId $PgId $Env.Label $runType
            if (!$key) { continue }

            $state = GetObjectState $ro $runStatus
            $objName = Clean (GetProp $obj 'name' '')
            $objType = Clean (GetProp $obj 'objectType' '')
            $host = ''
            if ($Env.ParentHostNeeded) { $host = GetParentHost $ro $idToName $hostById }
            $msg = GetObjectMessage $ro $info
            if (!$msg -and $state -eq 'Failure') { $msg = 'Object-level failure detected' }
            if (!$msg -and $state -eq 'Running') { $msg = 'Latest object/run state is running' }
            if (!$msg -and $state -eq 'Cancelled') { $msg = 'Latest object/run state is cancelled' }

            if ($state -eq 'Success') {
                if (!$successIndex.ContainsKey($key)) {
                    $successIndex[$key] = [pscustomobject]@{ Usecs=$usecs; ObjectName=$objName; ObjectType=$objType; Host=$host; RunType=$runType; Message=$msg }
                }
                continue
            }

            if ($state -in @('Failure','Running','Cancelled')) {
                if (!$failureRunCount.ContainsKey($key)) { $failureRunCount[$key] = 0 }
                $failureRunCount[$key] = [int]$failureRunCount[$key] + 1

                if ($successIndex.ContainsKey($key)) {
                    if (!$seenState.ContainsKey($key)) {
                        $s = $successIndex[$key]
                        $cleared += NewRow $Incident 'Success' 'Cleared' $ClusterName $ClusterId $Env $PgName $PgId $s.Host $s.ObjectName $s.ObjectType $s.RunType $usecs $usecs $s.Usecs $s.Usecs $msg $key $failureRunCount[$key]
                        $seenState[$key] = 'Success'
                    }
                    continue
                }

                if (!$seenState.ContainsKey($key)) {
                    $change = 'New'
                    if (@($PreviousRowsForPg | Where-Object { (Clean (GetProp $_ 'ObjectKey' '')) -eq $key }).Count -gt 0) { $change = 'Existing' }
                    $active += NewRow $Incident $state $change $ClusterName $ClusterId $Env $PgName $PgId $host $objName $objType $runType $usecs $usecs 0 $usecs $msg $key $failureRunCount[$key]
                    $seenState[$key] = $state
                }
            }
        }
    }

    foreach ($prev in @($PreviousRowsForPg)) {
        $key = Clean (GetProp $prev 'ObjectKey' '')
        if (!$key) { continue }
        if (@($active | Where-Object { (Clean (GetProp $_ 'ObjectKey' '')) -eq $key }).Count -gt 0) { continue }
        if (@($cleared | Where-Object { (Clean (GetProp $_ 'ObjectKey' '')) -eq $key }).Count -gt 0) { continue }
        if ($successIndex.ContainsKey($key)) {
            $s = $successIndex[$key]
            $cleared += NewRow $Incident 'Success' 'Cleared' $ClusterName $ClusterId $Env $PgName $PgId $s.Host $s.ObjectName $s.ObjectType $s.RunType 0 0 $s.Usecs $s.Usecs 'Previously failed object has newer successful backup' $key 0
        } else {
            $active += NewRow $Incident (Clean (GetProp $prev 'Status' 'Failure')) 'CarriedForward' $ClusterName $ClusterId $Env $PgName $PgId (GetProp $prev 'Host' '') (GetProp $prev 'ObjectName' '') (GetProp $prev 'ObjectType' '') (GetProp $prev 'RunType' '') 0 0 0 0 (GetProp $prev 'Message' '') $key (GetProp $prev 'FailureRuns' '')
        }
    }

    return [pscustomobject]@{ Active=@($active); Cleared=@($cleared) }
}

if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
if (!$IncidentNumber) { $IncidentNumber = Read-Host 'Enter incident number for fast object-level test' }
$IncidentNumber = $IncidentNumber.Trim().ToUpper()
if (!$IncidentNumber) { throw 'IncidentNumber is required.' }
$outputFolder = Join-Path $OutputRoot $IncidentNumber
if (!(Test-Path $outputFolder)) { New-Item -Path $outputFolder -ItemType Directory -Force | Out-Null }
$statePath = Join-Path $outputFolder 'state.json'
if ($ResetState -and (Test-Path $statePath)) { Remove-Item -Path $statePath -Force }

$oldState = ReadJson $statePath
$previousOpen = @()
$previousCleared = @()
if ($oldState) {
    $previousOpen = @(AsArray (GetProp $oldState 'CurrentOpenFailures' @()))
    $previousCleared = @(AsArray (GetProp $oldState 'ClearedBySuccess' @()))
}
$previousByPg = IndexPreviousOpenByPg $previousOpen

$apiKey = GetApiKey
$baseHeaders = @{ accept='application/json'; apiKey=$apiKey }
$clusterJson = InvokeHeliosGetJson ('{0}/v2/mcm/cluster-mgmt/info' -f $BaseUrl) $baseHeaders
$clusters = @($clusterJson.cohesityClusters)
if ($ClusterName) {
    $clusters = @($clusters | Where-Object { (GetClusterName $_) -eq $ClusterName })
    if ($clusters.Count -eq 0) { throw ('Cluster not found: {0}' -f $ClusterName) }
}
$clusters = @($clusters | Sort-Object { GetClusterName $_ })

$activeRows = @()
$clearedRows = @()
$noObjectEvidence = @()
$detailCalls = 0
$lightCalls = 0
$skippedPgs = 0
$pgChecked = 0

Write-Host ''
Write-Host 'Running experimental fast object-level collector.'
Write-Host ('OutputRoot : {0}' -f $OutputRoot)
Write-Host ('Incident   : {0}' -f $IncidentNumber)
Write-Host ('NumRuns    : {0}' -f $NumRuns)
Write-Host ('Timeout    : {0}' -f $RequestTimeoutSec)
Write-Host 'RemoteAdapter is excluded.'
Write-Host ''

$clusterIndex = 0
foreach ($cluster in $clusters) {
    $clusterIndex++
    $clusterId = Clean (GetProp $cluster 'clusterId' '')
    $cname = GetClusterName $cluster
    Write-Host ('[{0}/{1}] Cluster: {2}' -f $clusterIndex,$clusters.Count,$cname)

    foreach ($env in GetEnvironments) {
        $envDetail = 0
        $envSkipped = 0
        $envPgs = 0
        $pgs = GetProtectionGroups $cluster $env $apiKey
        foreach ($pg in $pgs) {
            $envPgs++
            $pgChecked++
            $pgId = GetPgId $pg
            $pgName = GetPgName $pg
            if (!$pgId) { continue }
            $pgKey = GetPgKey $clusterId $env.Label $pgId
            $prevForPg = @()
            if ($previousByPg.ContainsKey($pgKey)) { $prevForPg = @($previousByPg[$pgKey]) }

            try {
                $lightRuns = GetRuns $cluster $pgId $false $apiKey
                $lightCalls++
            } catch {
                AddWarning ('Light run lookup failed: {0} / {1} / {2}' -f $cname,$pgName,$_.Exception.Message)
                continue
            }
            if ($lightRuns.Count -eq 0) { continue }

            $hasSuspicious = @($lightRuns | Where-Object { IsSuspiciousRun $_ }).Count -gt 0
            $needsDetail = ($hasSuspicious -or $prevForPg.Count -gt 0)
            if (!$needsDetail) {
                $skippedPgs++
                $envSkipped++
                continue
            }

            try {
                $detailRuns = GetRuns $cluster $pgId $true $apiKey
                $detailCalls++
                $envDetail++
            } catch {
                AddWarning ('Detail run lookup failed: {0} / {1} / {2}' -f $cname,$pgName,$_.Exception.Message)
                continue
            }

            $objectCount = 0
            foreach ($r in $detailRuns) { $objectCount += @(AsArray (GetProp $r 'objects' @())).Count }
            if ($objectCount -eq 0 -and $hasSuspicious) {
                $noObjectEvidence += [pscustomobject]@{
                    Cluster=$cname; Environment=$env.Label; ProtectionGroup=$pgName; ProtectionGroupId=$pgId; Reason='Suspicious/failed PG run but Cohesity returned no run.objects. Not included in object-level failure output.'
                }
                continue
            }

            $result = ProcessDetailedRuns $detailRuns $IncidentNumber $cname $clusterId $env $pgName $pgId $prevForPg
            $activeRows += @($result.Active)
            $clearedRows += @($result.Cleared)
        }
        Write-Host ('  {0,-11}: PGs {1} | detail {2} | skipped {3}' -f $env.Label,$envPgs,$envDetail,$envSkipped)
    }
}

$activeRows = @(MergeByObjectKey $activeRows | Sort-Object Cluster,ProtectionGroup,ObjectName)
$clearedRows = @(MergeByObjectKey $clearedRows | Sort-Object Cluster,ProtectionGroup,ObjectName)
$previouslyCleared = @($previousCleared | ForEach-Object {
    $x = $_ | Select-Object *
    if ((Clean (GetProp $x 'Status' '')) -eq 'Success') { $x.Change = 'PreviouslyCleared' }
    $x
})
$allCleared = @(MergeByObjectKey @($previouslyCleared + $clearedRows))
$lifecycle = @(MergeByObjectKey @($activeRows + $allCleared))

WriteCsv $activeRows (Join-Path $outputFolder 'current_failures.csv') $script:Columns
WriteCsv $clearedRows (Join-Path $outputFolder 'cleared_by_success.csv') $script:Columns
WriteCsv $lifecycle (Join-Path $outputFolder 'incident_lifecycle.csv') $script:Columns
WriteCsv $noObjectEvidence (Join-Path $outputFolder 'no_object_evidence_review.csv') @('Cluster','Environment','ProtectionGroup','ProtectionGroupId','Reason')

$failureTextRows = @($activeRows | Select-Object -Property $script:LifecycleColumns)
$successTextRows = @($clearedRows | Select-Object -Property $script:LifecycleColumns)

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('Cohesity Backup Failure Object-Level Fast Test')
$lines.Add('')
$lines.Add(('Incident: {0}' -f $IncidentNumber))
$lines.Add(('Generated At: {0} ET' -f (NowEtText)))
$lines.Add(('Collection Status: {0}' -f $(if ($script:Warnings.Count -gt 0) { 'Incomplete' } else { 'Complete' })))
$lines.Add(('Scope: latest {0} runs; RemoteAdapter excluded.' -f $NumRuns))
$lines.Add(('API calls: light={0}, detail={1}, skippedPGs={2}' -f $lightCalls,$detailCalls,$skippedPgs))
$lines.Add('')
$lines.Add('Summary Counts:')
$lines.Add(('- Active object failures/running/cancelled: {0}' -f $activeRows.Count))
$lines.Add(('- Newly cleared by object success: {0}' -f $clearedRows.Count))
$lines.Add(('- No-object-evidence review rows: {0}' -f @($noObjectEvidence).Count))
$lines.Add('')
$lines.Add('Failure Section:')
$lines.Add(($failureTextRows | Format-Table -AutoSize | Out-String).Trim())
$lines.Add('')
$lines.Add('Success Section:')
$lines.Add(($successTextRows | Format-Table -AutoSize | Out-String).Trim())
$lines.Add('')
$lines.Add('Warnings:')
if ($script:Warnings.Count -eq 0) { $lines.Add('- None') } else { foreach ($w in $script:Warnings) { $lines.Add(('- {0}' -f $w)) } }
($lines -join [Environment]::NewLine) | Set-Content -Path (Join-Path $outputFolder 'worknotes_summary.txt') -Encoding UTF8

$state = [pscustomobject]@{
    IncidentNumber = $IncidentNumber
    GeneratedET = NowEtText
    NumRuns = $NumRuns
    RequestTimeoutSec = $RequestTimeoutSec
    RemoteAdapterExcluded = $true
    LightApiCalls = $lightCalls
    DetailApiCalls = $detailCalls
    SkippedProtectionGroups = $skippedPgs
    ProtectionGroupsChecked = $pgChecked
    Warnings = @($script:Warnings)
    CurrentOpenFailures = @($activeRows)
    ClearedBySuccess = @($allCleared)
    LastRunClearedBySuccess = @($clearedRows)
    NoObjectEvidenceReview = @($noObjectEvidence)
}
WriteJson $state $statePath

Write-Host ''
Write-Host 'Fast object-level run completed.'
Write-Host ('ProtectionGroups checked : {0}' -f $pgChecked)
Write-Host ('Light API calls          : {0}' -f $lightCalls)
Write-Host ('Detail API calls         : {0}' -f $detailCalls)
Write-Host ('Skipped clean PGs        : {0}' -f $skippedPgs)
Write-Host ('Active object rows       : {0}' -f $activeRows.Count)
Write-Host ('Cleared object rows      : {0}' -f $clearedRows.Count)
Write-Host ('Output folder            : {0}' -f $outputFolder)
