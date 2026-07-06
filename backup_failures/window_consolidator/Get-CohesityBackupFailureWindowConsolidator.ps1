<#
.SYNOPSIS
  Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
  Operator-focused incident evidence tool for Cohesity backup failures.
  It locks one ServiceNow incident to one Dynatrace compute window and consolidates
  failed, recovered, still-failing, new, repeated, running, and cancelled backup activity.

  The engineer enters the incident only once when a new Dynatrace compute window starts.
  Later runs in the same window reuse the locked mapping from the registry.

.DEFAULTS
  Helios URL : https://helios.cohesity.com
  API key    : X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt
  Output     : X:\PowerShell\Data\Cohesity\BackupFailureWindow
  Window     : Dynatrace compute_window, America/New_York, 18:00 ET -> next day 18:00 ET
#>

[CmdletBinding()]
param(
    [string]$HeliosBaseUrl = 'https://helios.cohesity.com',
    [string]$ApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow',
    [string]$IncidentNumber,
    [int]$DynatraceWindowStartHourET = 18,
    [int]$MaxRunsPerProtectionGroup = 120,
    [int]$MaxClusters = 0,
    [int]$MaxProtectionGroupsPerCluster = 0,
    [bool]$ShowGridView = $true,
    [switch]$MultipleGridViews
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

function Get-EasternTimeZone {
    try { return [System.TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
    catch { return [System.TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
}

function Get-NowET {
    [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), (Get-EasternTimeZone))
}

function Format-ET($Value) {
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { return '' }
    try { return ([datetime]$Value).ToString('yyyy-MM-dd HH:mm:ss') } catch { return [string]$Value }
}

function Format-UtcForSnow([datetime]$UtcValue) {
    $UtcValue.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')
}

function Convert-ETToUtc([datetime]$EtValue) {
    $tz = Get-EasternTimeZone
    $unspecified = [datetime]::SpecifyKind($EtValue, [DateTimeKind]::Unspecified)
    [System.TimeZoneInfo]::ConvertTimeToUtc($unspecified, $tz)
}

function Convert-UsecsToET($Usecs) {
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return $null }
    try {
        $epoch = [datetime]::SpecifyKind([datetime]'1970-01-01T00:00:00Z', [DateTimeKind]::Utc)
        $utc = $epoch.AddSeconds(([double]$Usecs) / 1000000)
        return [System.TimeZoneInfo]::ConvertTimeFromUtc($utc, (Get-EasternTimeZone))
    } catch { return $null }
}

function Get-WindowKey([datetime]$StartET) {
    # Must match Dynatrace compute_window output shape: yyyy-MM-dd_1800ET
    '{0}_{1}ET' -f $StartET.ToString('yyyy-MM-dd'), $StartET.ToString('HHmm')
}

function Get-DynatraceComputeWindow {
    # Mirrors DT compute_window:
    # Time zone America/New_York, daily boundary 18:00 ET, DST-aware.
    $nowEt = Get-NowET
    $todayBoundary = $nowEt.Date.AddHours($DynatraceWindowStartHourET)
    if ($nowEt -lt $todayBoundary) { $start = $todayBoundary.AddDays(-1) } else { $start = $todayBoundary }
    $end = $start.AddDays(1)
    $startUtc = Convert-ETToUtc $start
    $endUtc = Convert-ETToUtc $end

    [pscustomobject]@{
        StartET = $start
        EndET = $end
        WindowKey = Get-WindowKey $start
        WindowLabel = ('{0} ET -> {1} ET' -f $start.ToString('yyyy-MM-dd HH:mm'), $end.ToString('yyyy-MM-dd HH:mm'))
        SnStartUtc = Format-UtcForSnow $startUtc
        SnEndUtc = Format-UtcForSnow $endUtc
        Source = 'Dynatrace_compute_window'
    }
}

function Read-Json($Path, $Default) {
    if (Test-Path $Path) {
        $raw = Get-Content -Path $Path -Raw
        if (-not [string]::IsNullOrWhiteSpace($raw)) { return ($raw | ConvertFrom-Json) }
    }
    return $Default
}

function Write-Json($Object, $Path) {
    $folder = Split-Path -Path $Path -Parent
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 50 | Set-Content -Path $Path -Encoding UTF8
}

function Sanitize-Name([string]$Name) {
    if ([string]::IsNullOrWhiteSpace($Name)) { return 'Unknown' }
    (($Name.Trim() -replace '[\\/:*?"<>|]', '_') -replace '\s+', '_')
}

function Ensure-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    @($Value)
}

function Get-Prop($Obj, [string[]]$Names) {
    if ($null -eq $Obj) { return $null }
    foreach ($name in $Names) {
        if ($Obj.PSObject.Properties.Name -contains $name) { return $Obj.$name }
    }
    return $null
}

function First-Array($Obj, [string[]]$Names) {
    foreach ($name in $Names) {
        $v = Get-Prop $Obj @($name)
        if ($null -ne $v) { return @(Ensure-Array $v) }
    }
    @()
}

function Normalize-Status($Status) {
    if ($null -eq $Status) { return 'Unknown' }
    $s = ([string]$Status).Trim() -replace '^k', ''
    switch -Regex ($s) {
        'SucceededWithWarning|Succeeded|Success|Warning' { 'Succeeded'; break }
        'Fail|Failed|Failure|Error' { 'Failed'; break }
        'Cancel|Canceled|Cancelled' { 'Canceled'; break }
        'Running|Accepted|Started|InProgress' { 'Running'; break }
        default { $s }
    }
}

function Normalize-RunType($RunType) {
    if ($null -eq $RunType) { return 'Unknown' }
    $s = ([string]$RunType).Trim() -replace '^k', ''
    switch -Regex ($s) {
        'Full' { 'Full'; break }
        'Incremental|Regular' { 'Incremental'; break }
        'Log' { 'Log'; break }
        default { $s }
    }
}

function Normalize-Environment($Env) {
    if ($null -eq $Env) { return 'Unknown' }
    $s = ([string]$Env).Trim() -replace '^k', ''
    switch -Regex ($s) {
        'Acropolis' { 'Nutanix'; break }
        'GenericNas' { 'NAS'; break }
        'Isilon' { 'Isilon'; break }
        'Physical' { 'Physical'; break }
        'SQL' { 'SQL'; break }
        'Oracle' { 'Oracle'; break }
        'HyperV' { 'HyperV'; break }
        default { $s }
    }
}

function Get-Message($Obj) {
    if ($null -eq $Obj) { return '' }
    foreach ($f in @('errorMessage','message','errorMsg','failureMessage','warningMessage','reason','description')) {
        $v = Get-Prop $Obj @($f)
        if ($v) { return [string]$v }
    }
    foreach ($f in @('error','errors','warnings')) {
        $x = Get-Prop $Obj @($f)
        if ($x) {
            $first = @(Ensure-Array $x)[0]
            foreach ($m in @('errorMsg','message','errorMessage','reason')) {
                $v = Get-Prop $first @($m)
                if ($v) { return [string]$v }
            }
        }
    }
    ''
}

function Get-RunStatus($Run) {
    $s = Get-Prop $Run @('status','backupRunStatus','runStatus')
    if (-not $s) { $s = Get-Prop (Get-Prop $Run @('localBackupInfo')) @('status') }
    Normalize-Status $s
}

function Get-RunType($Run) {
    $t = Get-Prop $Run @('runType','backupRunType')
    if (-not $t) { $t = Get-Prop (Get-Prop $Run @('localBackupInfo')) @('runType') }
    Normalize-RunType $t
}

function Get-RunStartET($Run) {
    $u = Get-Prop $Run @('startTimeUsecs','runStartTimeUsecs','runStartTime')
    if (-not $u) { $u = Get-Prop (Get-Prop $Run @('localBackupInfo')) @('startTimeUsecs','runStartTimeUsecs') }
    Convert-UsecsToET $u
}

function Get-RunEndET($Run) {
    $u = Get-Prop $Run @('endTimeUsecs','endUsecs','runEndTimeUsecs','runEndTime')
    if (-not $u) { $u = Get-Prop (Get-Prop $Run @('localBackupInfo')) @('endTimeUsecs','runEndTimeUsecs') }
    if (-not $u) { $u = Get-Prop $Run @('startTimeUsecs','runStartTimeUsecs') }
    Convert-UsecsToET $u
}

function Get-RunObjects($Run) {
    $objects = First-Array $Run @('objects','objectRuns','objectRunList','tasks','taskRuns')
    if ($objects.Count -eq 0) {
        $inner = Get-Prop $Run @('run','protectionRun','backupRun')
        if ($inner) { $objects = First-Array $inner @('objects','objectRuns','objectRunList','tasks','taskRuns') }
    }
    $objects
}

function Get-ObjectName($Obj) {
    $o = Get-Prop $Obj @('object','entity','source')
    $n = Get-Prop $Obj @('name','objectName','displayName')
    if (-not $n) { $n = Get-Prop $o @('name','objectName','displayName') }
    if (-not $n) { $n = 'UnknownObject' }
    [string]$n
}

function Get-ObjectId($Obj) {
    $o = Get-Prop $Obj @('object','entity','source')
    $id = Get-Prop $Obj @('id','objectId','entityId','sourceId','uid')
    if (-not $id) { $id = Get-Prop $o @('id','objectId','entityId','sourceId','uid') }
    [string]$id
}

function Get-ObjectType($Obj, $Environment) {
    $o = Get-Prop $Obj @('object','entity','source')
    $t = Get-Prop $Obj @('type','objectType','entityType')
    if (-not $t) { $t = Get-Prop $o @('type','objectType','entityType') }
    if (-not $t) { $t = $Environment }
    ([string]$t -replace '^k', '')
}

function Get-HostName($Obj) {
    $h = Get-Prop $Obj @('host','hostName','parentName','sourceName','registeredSourceName')
    if ($h) { return [string]$h }
    $o = Get-Prop $Obj @('object','entity','source')
    $h = Get-Prop $o @('parentName','hostName','sourceName','registeredSourceName')
    if ($h) { return [string]$h }
    ''
}

function Get-ObjectStatus($Obj, $RunStatus) {
    $s = Get-Prop $Obj @('status','runStatus','protectionStatus','backupStatus')
    if (-not $s) {
        $lsi = Get-Prop $Obj @('localSnapshotInfo')
        $si = Get-Prop $lsi @('snapshotInfo')
        if ($si) { $s = Get-Prop $si @('status','snapshotStatus') }
    }
    if (-not $s) { $s = $RunStatus }
    Normalize-Status $s
}

function Get-ObjectKey($ClusterId, $Environment, $PgId, $PgName, $Obj) {
    $id = Get-ObjectId $Obj
    if (-not [string]::IsNullOrWhiteSpace($id)) { return "$ClusterId|$Environment|$PgId|$id" }
    "$ClusterId|$Environment|$PgName|$(Get-HostName $Obj)|$(Get-ObjectName $Obj)"
}

function Invoke-CohesityGet($Path, $Headers, $Query) {
    $uri = $HeliosBaseUrl.TrimEnd('/') + $Path
    if ($Query -and $Query.Count -gt 0) {
        $pairs = foreach ($k in $Query.Keys) {
            if ($null -ne $Query[$k] -and [string]$Query[$k] -ne '') {
                '{0}={1}' -f [uri]::EscapeDataString([string]$k), [uri]::EscapeDataString([string]$Query[$k])
            }
        }
        if ($pairs) { $uri += '?' + ($pairs -join '&') }
    }
    $last = $null
    for ($i = 1; $i -le 3; $i++) {
        try { return Invoke-RestMethod -Method GET -Uri $uri -Headers $Headers -TimeoutSec 120 }
        catch { $last = $_; Start-Sleep -Seconds ([math]::Min($i * 2, 6)) }
    }
    throw $last
}

function Get-ApiKey {
    if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
    $k = (Get-Content -Path $ApiKeyPath -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($k)) { throw "API key file is empty: $ApiKeyPath" }
    $k
}

function Get-Clusters($Headers) {
    $json = Invoke-CohesityGet '/v2/mcm/cluster-mgmt/info' $Headers @{}
    $list = First-Array $json @('clusters','clusterInfo','clusterInfos','items','data')
    $out = foreach ($c in $list) {
        $id = Get-Prop $c @('clusterId','id','uuid')
        $name = Get-Prop $c @('clusterName','name','displayName','hostname')
        if ($id -or $name) { [pscustomobject]@{ ClusterId = [string]$id; ClusterName = [string]$name } }
    }
    if ($MaxClusters -gt 0) { return @($out | Select-Object -First $MaxClusters) }
    @($out)
}

function Get-ProtectionGroups($Headers) {
    $json = Invoke-CohesityGet '/v2/data-protect/protection-groups' $Headers @{ isDeleted='false'; isActive='true'; includeLastRunInfo='true' }
    $pgs = First-Array $json @('protectionGroups','protectionGroupInfos','items','data')
    if ($MaxProtectionGroupsPerCluster -gt 0) { return @($pgs | Select-Object -First $MaxProtectionGroupsPerCluster) }
    @($pgs)
}

function Get-Runs($Headers, $PgId) {
    $encoded = [uri]::EscapeDataString([string]$PgId)
    $json = Invoke-CohesityGet "/v2/data-protect/protection-groups/$encoded/runs" $Headers @{ numRuns=[string]$MaxRunsPerProtectionGroup; includeObjectDetails='true' }
    First-Array $json @('runs','protectionRuns','items','data')
}

function Resolve-WindowMapping($Window) {
    if (-not (Test-Path $OutputRoot)) { New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null }
    $registryPath = Join-Path $OutputRoot 'BackupFailure_WindowRegistry.json'
    $default = [pscustomobject]@{
        TimeZone = 'America/New_York'
        WindowMode = 'DynatraceDaily18ET'
        WindowDurationHours = 24
        WindowStartHourET = $DynatraceWindowStartHourET
        Windows = [pscustomobject]@{}
    }
    $registry = Read-Json $registryPath $default
    foreach ($required in @('Windows')) {
        if (-not ($registry.PSObject.Properties.Name -contains $required)) { $registry | Add-Member -MemberType NoteProperty -Name $required -Value ([pscustomobject]@{}) }
    }
    foreach ($field in @('TimeZone','WindowMode','WindowDurationHours','WindowStartHourET')) {
        if (-not ($registry.PSObject.Properties.Name -contains $field)) {
            $registry | Add-Member -MemberType NoteProperty -Name $field -Value $default.$field
        }
    }
    $registry.TimeZone = 'America/New_York'
    $registry.WindowMode = 'DynatraceDaily18ET'
    $registry.WindowDurationHours = 24
    $registry.WindowStartHourET = $DynatraceWindowStartHourET

    $key = $Window.WindowKey
    if ($registry.Windows.PSObject.Properties.Name -contains $key) {
        $m = $registry.Windows.$key
        if ($IncidentNumber -and $IncidentNumber.Trim() -ne $m.IncidentNumber) {
            throw "Window $key is already locked to $($m.IncidentNumber). Do not change the incident until the next Dynatrace compute window."
        }
        $m.LastRunET = Format-ET (Get-NowET)
        Write-Json $registry $registryPath
        return [pscustomobject]@{ RegistryPath=$registryPath; WindowKey=$key; Mapping=$m; IsNew=$false }
    }

    $inc = $IncidentNumber
    if (-not $inc) {
        Write-Host "New Dynatrace compute window detected: $($Window.WindowLabel)" -ForegroundColor Yellow
        $inc = Read-Host 'Enter incident number for this window'
    }
    if ([string]::IsNullOrWhiteSpace($inc)) { throw 'Incident number is required for a new Dynatrace compute window.' }
    $inc = $inc.Trim().ToUpper()

    $previous = $null
    foreach ($p in $registry.Windows.PSObject.Properties) {
        $w = $p.Value
        try {
            if ([datetime]$w.WindowEndET -le $Window.StartET) {
                if (-not $previous -or [datetime]$w.WindowEndET -gt [datetime]$previous.WindowEndET) { $previous = $w }
            }
        } catch {}
    }

    $folder = Join-Path $OutputRoot (Sanitize-Name $inc)
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
    $m = [pscustomobject]@{
        IncidentNumber = $inc
        WindowKey = $key
        WindowLabel = $Window.WindowLabel
        WindowStartET = Format-ET $Window.StartET
        WindowEndET = Format-ET $Window.EndET
        SnStartUtc = $Window.SnStartUtc
        SnEndUtc = $Window.SnEndUtc
        WindowLocked = $true
        WindowSource = $Window.Source
        FirstRunET = Format-ET (Get-NowET)
        LastRunET = Format-ET (Get-NowET)
        CarryForwardFromIncident = $(if ($previous) { $previous.IncidentNumber } else { '' })
        CarryForwardToIncident = ''
        SnowSysId = ''
        SnowWorkNotesReadEnabled = $false
        OutputFolder = $folder
    }
    $registry.Windows | Add-Member -MemberType NoteProperty -Name $key -Value $m
    Write-Json $registry $registryPath
    [pscustomobject]@{ RegistryPath=$registryPath; WindowKey=$key; Mapping=$m; IsNew=$true }
}

function New-Event($Incident,$Time,$ClusterId,$Cluster,$Env,$PgId,$Pg,$Host,$ObjName,$ObjType,$ObjId,$ObjKey,$RunType,$EventType,$Msg,$RunStart,$RunEnd) {
    [pscustomobject]@{
        IncidentNumber=$Incident; EventTimeET=Format-ET $Time; ClusterId=$ClusterId; Cluster=$Cluster; Environment=$Env;
        ProtectionGroupId=$PgId; ProtectionGroup=$Pg; Host=$Host; ObjectName=$ObjName; ObjectType=$ObjType; ObjectId=$ObjId;
        ObjectKey=$ObjKey; RunType=$RunType; EventType=$EventType; Message=$Msg; RunStartET=Format-ET $RunStart; RunEndET=Format-ET $RunEnd
    }
}

function Collect-Events($Headers,$Window,$Incident) {
    $events = @(); $runEvidence = @(); $warnings = @()
    foreach ($cluster in (Get-Clusters $Headers)) {
        $clusterId = $cluster.ClusterId
        $clusterName = if ($cluster.ClusterName) { $cluster.ClusterName } else { $clusterId }
        $h = @{}; foreach ($k in $Headers.Keys) { $h[$k] = $Headers[$k] }
        if ($clusterId) { $h['accessClusterId'] = [string]$clusterId }

        try { $pgs = Get-ProtectionGroups $h } catch { $warnings += "Cluster $clusterName PG query failed: $($_.Exception.Message)"; continue }
        foreach ($pg in $pgs) {
            $pgId = [string](Get-Prop $pg @('id','protectionGroupId','uid'))
            if (-not $pgId) { $pgId = [string](Get-Prop $pg @('name','protectionGroupName')) }
            $pgName = [string](Get-Prop $pg @('name','protectionGroupName'))
            if (-not $pgName) { $pgName = $pgId }
            $env = Normalize-Environment (Get-Prop $pg @('environment','env','protectionSourceEnvironment'))
            try { $runs = Get-Runs $h $pgId } catch { $warnings += "Cluster $clusterName PG $pgName run query failed: $($_.Exception.Message)"; continue }

            $oldest = $null
            foreach ($run in $runs) {
                $runStart = Get-RunStartET $run
                $runEnd = Get-RunEndET $run
                $eventTime = if ($runEnd) { $runEnd } else { $runStart }
                if (-not $eventTime) { continue }
                if (-not $oldest -or $eventTime -lt $oldest) { $oldest = $eventTime }
                if ($eventTime -lt $Window.StartET -or $eventTime -ge $Window.EndET) { continue }

                $runStatus = Get-RunStatus $run
                $runType = Get-RunType $run
                $objects = Get-RunObjects $run
                $runEvidence += [pscustomobject]@{
                    IncidentNumber=$Incident; Cluster=$clusterName; Environment=$env; ProtectionGroup=$pgName;
                    RunType=$runType; RunStatus=$runStatus; RunStartET=Format-ET $runStart; RunEndET=Format-ET $runEnd;
                    ObjectDetailCount=$objects.Count; Message=Get-Message $run
                }

                if ($objects.Count -eq 0) {
                    if ($runStatus -in @('Failed','Canceled','Running','Succeeded')) {
                        $eventType = if ($runStatus -eq 'Failed') { 'Failed' } elseif ($runStatus -eq 'Canceled') { 'CancelledRun' } elseif ($runStatus -eq 'Running') { 'RunningRun' } else { 'Succeeded' }
                        $key = "$clusterId|$env|$pgId|PG_LEVEL|$runType"
                        $events += New-Event $Incident $eventTime $clusterId $clusterName $env $pgId $pgName '' $pgName 'ProtectionGroup' '' $key $runType $eventType (Get-Message $run) $runStart $runEnd
                    }
                    continue
                }

                foreach ($obj in $objects) {
                    $objStatus = Get-ObjectStatus $obj $runStatus
                    $eventType = $null
                    if ($objStatus -eq 'Running' -or $runStatus -eq 'Running') { $eventType = 'RunningRun' }
                    elseif ($objStatus -eq 'Canceled' -or $runStatus -eq 'Canceled') { $eventType = 'CancelledRun' }
                    elseif ($objStatus -eq 'Failed' -or $runStatus -eq 'Failed') { $eventType = 'Failed' }
                    elseif ($objStatus -eq 'Succeeded' -or $runStatus -eq 'Succeeded') { $eventType = 'Succeeded' }
                    if (-not $eventType) { continue }

                    $objName = Get-ObjectName $obj; $objId = Get-ObjectId $obj; $objType = Get-ObjectType $obj $env; $host = Get-HostName $obj
                    $key = Get-ObjectKey $clusterId $env $pgId $pgName $obj
                    $msg = Get-Message $obj; if (-not $msg) { $msg = Get-Message $run }
                    $events += New-Event $Incident $eventTime $clusterId $clusterName $env $pgId $pgName $host $objName $objType $objId $key $runType $eventType $msg $runStart $runEnd
                }
            }
            if ($oldest -and $oldest -gt $Window.StartET) { $warnings += "PG $pgName on $clusterName may be truncated; oldest returned run is after window start. Increase MaxRunsPerProtectionGroup." }
        }
    }
    [pscustomobject]@{ Events=$events; RunEvidence=$runEvidence; Warnings=$warnings }
}

function New-SectionRow($Event,$Section,$Status,$FirstFailedET,$LastFailedET,$RecoveredET,$ConsecCount) {
    [pscustomobject]@{
        Section=$Section; Status=$Status; IncidentNumber=$Event.IncidentNumber; Cluster=$Event.Cluster; Environment=$Event.Environment;
        ProtectionGroup=$Event.ProtectionGroup; Host=$Event.Host; ObjectName=$Event.ObjectName; ObjectType=$Event.ObjectType; RunType=$Event.RunType;
        FirstFailedET=$FirstFailedET; LastFailedET=$LastFailedET; RecoveredET=$RecoveredET; ConsecutiveFailureCount=$ConsecCount;
        Message=$Event.Message; ObjectKey=$Event.ObjectKey
    }
}

function Build-Tables($Events,$PreviousState,$Window,$Incident,$RunEvidence,$Warnings) {
    $prevFailing = @{}
    if ($PreviousState -and ($PreviousState.PSObject.Properties.Name -contains 'Objects')) {
        foreach ($p in @($PreviousState.Objects)) {
            if ($p.CurrentStatus -in @('StillFailing','ReFailed')) { $prevFailing[[string]$p.ObjectKey] = $p }
        }
    }

    $byKey = @{}
    foreach ($e in @($Events | Sort-Object EventTimeET)) {
        if (-not $byKey.ContainsKey($e.ObjectKey)) { $byKey[$e.ObjectKey] = @() }
        $byKey[$e.ObjectKey] += $e
    }

    $current=@(); $recovered=@(); $newFail=@(); $newRec=@(); $consec=@(); $running=@(); $cancelled=@(); $state=@()
    foreach ($key in $byKey.Keys) {
        $evs = @($byKey[$key] | Sort-Object EventTimeET)
        $fails = @($evs | Where-Object EventType -eq 'Failed')
        $succ = @($evs | Where-Object EventType -eq 'Succeeded')
        foreach ($r in @($evs | Where-Object EventType -eq 'RunningRun')) { $running += New-SectionRow $r 'Running Run' 'RunningAtLatestCheck' '' '' '' 0 }
        foreach ($c in @($evs | Where-Object EventType -eq 'CancelledRun')) { $cancelled += New-SectionRow $c 'Cancelled Run' 'CancelledInWindow' '' $c.EventTimeET '' 0 }
        if ($fails.Count -eq 0) { continue }

        $firstFail = $fails | Select-Object -First 1
        $lastFail = $fails | Select-Object -Last 1
        $lastSuccessBefore = $succ | Where-Object { [datetime]$_.EventTimeET -lt [datetime]$lastFail.EventTimeET } | Select-Object -Last 1
        $laterSuccess = $succ | Where-Object { [datetime]$_.EventTimeET -gt [datetime]$lastFail.EventTimeET } | Select-Object -First 1
        $consecCount = if ($lastSuccessBefore) { @($fails | Where-Object { [datetime]$_.EventTimeET -gt [datetime]$lastSuccessBefore.EventTimeET }).Count } else { $fails.Count }

        if ($laterSuccess) {
            $recovered += New-SectionRow $lastFail 'Recovered In Window' 'RecoveredInWindow' $firstFail.EventTimeET $lastFail.EventTimeET $laterSuccess.EventTimeET $consecCount
            if ($prevFailing.ContainsKey($key)) { $newRec += New-SectionRow $lastFail 'New Recovery' 'NewlyRecoveredThisCheck' $firstFail.EventTimeET $lastFail.EventTimeET $laterSuccess.EventTimeET $consecCount }
            $state += [pscustomobject]@{ ObjectKey=$key; Cluster=$lastFail.Cluster; Environment=$lastFail.Environment; ProtectionGroup=$lastFail.ProtectionGroup; Host=$lastFail.Host; ObjectName=$lastFail.ObjectName; ObjectType=$lastFail.ObjectType; RunType=$lastFail.RunType; CurrentStatus='RecoveredInWindow'; FirstFailedET=$firstFail.EventTimeET; LastFailedET=$lastFail.EventTimeET; RecoveredET=$laterSuccess.EventTimeET; ConsecutiveFailureCount=$consecCount; LastMessage=$lastFail.Message }
        } else {
            $status = if ($lastSuccessBefore) { 'ReFailed' } else { 'StillFailing' }
            $row = New-SectionRow $lastFail 'Current Still Failing' $status $firstFail.EventTimeET $lastFail.EventTimeET '' $consecCount
            $current += $row
            if (-not $prevFailing.ContainsKey($key)) { $newFail += New-SectionRow $lastFail 'New Failure' 'NewlyFailedThisCheck' $firstFail.EventTimeET $lastFail.EventTimeET '' $consecCount }
            if ($consecCount -gt 1) { $consec += New-SectionRow $lastFail 'Consecutive Failure' 'ConsecutiveFailure' $firstFail.EventTimeET $lastFail.EventTimeET '' $consecCount }
            $state += [pscustomobject]@{ ObjectKey=$key; Cluster=$lastFail.Cluster; Environment=$lastFail.Environment; ProtectionGroup=$lastFail.ProtectionGroup; Host=$lastFail.Host; ObjectName=$lastFail.ObjectName; ObjectType=$lastFail.ObjectType; RunType=$lastFail.RunType; CurrentStatus=$status; FirstFailedET=$firstFail.EventTimeET; LastFailedET=$lastFail.EventTimeET; RecoveredET=''; ConsecutiveFailureCount=$consecCount; LastMessage=$lastFail.Message }
        }
    }

    $failedEvents = @($Events | Where-Object EventType -eq 'Failed')
    $failedKeys = @($failedEvents | Select-Object -ExpandProperty ObjectKey -Unique)
    $clusters = @($Events | Where-Object Cluster | Select-Object -ExpandProperty Cluster -Unique)
    $envs = @($Events | Where-Object Environment | Select-Object -ExpandProperty Environment -Unique)
    $pgs = @($Events | Where-Object ProtectionGroup | Select-Object -ExpandProperty ProtectionGroup -Unique)
    $summary = @(
        [pscustomobject]@{Metric='IncidentNumber';Value=$Incident},
        [pscustomobject]@{Metric='WindowKey';Value=$Window.WindowKey},
        [pscustomobject]@{Metric='WindowLabel';Value=$Window.WindowLabel},
        [pscustomobject]@{Metric='WindowStartET';Value=Format-ET $Window.StartET},
        [pscustomobject]@{Metric='WindowEndET';Value=Format-ET $Window.EndET},
        [pscustomobject]@{Metric='SnStartUtc';Value=$Window.SnStartUtc},
        [pscustomobject]@{Metric='SnEndUtc';Value=$Window.SnEndUtc},
        [pscustomobject]@{Metric='GeneratedAtET';Value=Format-ET (Get-NowET)},
        [pscustomobject]@{Metric='TotalUniqueObjectsFailedInWindow';Value=$failedKeys.Count},
        [pscustomobject]@{Metric='RecoveredInWindow';Value=$recovered.Count},
        [pscustomobject]@{Metric='StillFailingAtLatestCheck';Value=$current.Count},
        [pscustomobject]@{Metric='NewFailuresSincePreviousRun';Value=$newFail.Count},
        [pscustomobject]@{Metric='NewRecoveriesSincePreviousRun';Value=$newRec.Count},
        [pscustomobject]@{Metric='ConsecutiveRepeatedFailures';Value=$consec.Count},
        [pscustomobject]@{Metric='ReFailedInWindow';Value=@($current | Where-Object Status -eq 'ReFailed').Count},
        [pscustomobject]@{Metric='RunningRunsSeen';Value=$running.Count},
        [pscustomobject]@{Metric='CancelledRunsSeen';Value=$cancelled.Count},
        [pscustomobject]@{Metric='ImpactedClusters';Value=$clusters.Count},
        [pscustomobject]@{Metric='ImpactedEnvironments';Value=($envs -join '; ')},
        [pscustomobject]@{Metric='ImpactedProtectionGroups';Value=$pgs.Count},
        [pscustomobject]@{Metric='WarningCount';Value=$Warnings.Count}
    )

    [pscustomobject]@{
        Summary=$summary; CurrentFailing=$current; Recovered=$recovered; NewFailures=$newFail; NewRecoveries=$newRec; Consecutive=$consec;
        CarryForward=$current; EventHistory=@($Events | Sort-Object EventTimeET); RunEvidence=$RunEvidence; QuickView=@($current+$recovered+$newFail+$newRec+$consec+$running+$cancelled);
        Running=$running; Cancelled=$cancelled; ObjectState=$state; Warnings=$Warnings
    }
}

function Get-SafeSheetName($Name) {
    $s = $Name -replace '[\\/\?\*\[\]:]', '_'
    if ($s.Length -gt 31) { $s.Substring(0,31) } else { $s }
}

function Get-SafeTableName($Name) {
    $s = ('T_' + (Sanitize-Name $Name)) -replace '[^A-Za-z0-9_]', '_'
    if ($s.Length -gt 200) { $s.Substring(0,200) } else { $s }
}

function Export-Xlsx($Path, [System.Collections.IDictionary]$Sheets) {
    if (Test-Path $Path) { Remove-Item $Path -Force }
    $cmd = Get-Command Export-Excel -ErrorAction SilentlyContinue
    if ($cmd) {
        $first = $true
        foreach ($name in $Sheets.Keys) {
            $rows = @($Sheets[$name]); if ($rows.Count -eq 0) { $rows = @([pscustomobject]@{Info='No rows'}) }
            $params = @{Path=$Path; WorksheetName=(Get-SafeSheetName $name); AutoSize=$true; FreezeTopRow=$true; BoldTopRow=$true; TableName=(Get-SafeTableName $name)}
            if (-not $first) { $params.Append = $true }
            $rows | Export-Excel @params
            $first = $false
        }
        return
    }

    $excel = $null
    try {
        $excel = New-Object -ComObject Excel.Application
        $excel.Visible = $false; $excel.DisplayAlerts = $false
        $wb = $excel.Workbooks.Add()
        while ($wb.Worksheets.Count -gt 1) { $wb.Worksheets.Item(1).Delete() }
        $idx = 0
        foreach ($name in $Sheets.Keys) {
            $idx++
            $ws = if ($idx -eq 1) { $wb.Worksheets.Item(1) } else { $wb.Worksheets.Add([Type]::Missing, $wb.Worksheets.Item($wb.Worksheets.Count)) }
            $ws.Name = Get-SafeSheetName $name
            $rows = @($Sheets[$name]); if ($rows.Count -eq 0) { $rows = @([pscustomobject]@{Info='No rows'}) }
            $headers = @($rows[0].PSObject.Properties.Name)
            for ($c=0; $c -lt $headers.Count; $c++) { $ws.Cells.Item(1,$c+1).Value2=$headers[$c]; $ws.Cells.Item(1,$c+1).Font.Bold=$true }
            for ($r=0; $r -lt $rows.Count; $r++) { for ($c=0; $c -lt $headers.Count; $c++) { $ws.Cells.Item($r+2,$c+1).Value2=[string]$rows[$r].PSObject.Properties[$headers[$c]].Value } }
            $ws.Columns.AutoFit() | Out-Null
        }
        $wb.SaveAs($Path,51); $wb.Close($true)
    } catch { throw "XLSX export failed. Install ImportExcel module or run on a machine with Excel installed. $($_.Exception.Message)" }
    finally { if ($excel) { $excel.Quit() | Out-Null } }
}

function New-WorkNotes($Tables,$Window,$Incident,$WorkbookName) {
    $h=@{}; foreach($r in $Tables.Summary){$h[$r.Metric]=$r.Value}
    @(
        'Backup Failure Window Summary','',
        "Incident: $Incident",
        "Locked Compute Window: $($Window.WindowLabel)",
        "SNOW Compare UTC: $($Window.SnStartUtc) to $($Window.SnEndUtc)",
        "Generated At: $(Format-ET (Get-NowET)) ET",
        'Source: Cohesity Helios API / PowerShell Window Consolidator','',
        'Summary:',
        "- Total unique objects failed in this window: $($h['TotalUniqueObjectsFailedInWindow'])",
        "- Recovered within this window: $($h['RecoveredInWindow'])",
        "- Still failing at latest check within this window: $($h['StillFailingAtLatestCheck'])",
        "- New failures since previous check: $($h['NewFailuresSincePreviousRun'])",
        "- New recoveries since previous check: $($h['NewRecoveriesSincePreviousRun'])",
        "- Consecutive/repeated failures: $($h['ConsecutiveRepeatedFailures'])",
        "- Re-failed after recovery in same window: $($h['ReFailedInWindow'])",
        "- Running backup runs seen: $($h['RunningRunsSeen'])",
        "- Cancelled backup runs seen: $($h['CancelledRunsSeen'])",
        "- Impacted clusters: $($h['ImpactedClusters'])",
        "- Impacted environments: $($h['ImpactedEnvironments'])",
        "- Impacted protection groups: $($h['ImpactedProtectionGroups'])",'',
        'Current Still Failing: See workbook tab 02_Current_Still_Failing',
        'Recovered During Window: See workbook tab 03_Recovered_In_Window',
        'Consecutive / Repeated Failures: See workbook tab 06_Consecutive_Failures',
        'Carry Forward Baseline: See workbook tab 07_Carry_Forward_Baseline','',
        'Note: Running runs are listed separately and are not treated as failed or recovered until they complete.','',
        "Attachment: $WorkbookName"
    ) -join [Environment]::NewLine
}

function Show-Output($Tables,$Window,$Incident,$WorkbookPath,$WorkNotesPath,$StatePath) {
    $h=@{}; foreach($r in $Tables.Summary){$h[$r.Metric]=$r.Value}
    Write-Host ''; Write-Host "Incident: $Incident" -ForegroundColor Cyan
    Write-Host "Window  : $($Window.WindowLabel)"; Write-Host ''
    Write-Host 'Summary:' -ForegroundColor Cyan
    Write-Host "Total Failed In Window       : $($h['TotalUniqueObjectsFailedInWindow'])"
    Write-Host "Recovered In Window          : $($h['RecoveredInWindow'])"
    Write-Host "Still Failing Now            : $($h['StillFailingAtLatestCheck'])"
    Write-Host "New Failures Since Last Run  : $($h['NewFailuresSincePreviousRun'])"
    Write-Host "New Recoveries Since Last Run: $($h['NewRecoveriesSincePreviousRun'])"
    Write-Host "Consecutive Failures         : $($h['ConsecutiveRepeatedFailures'])"
    Write-Host "Running Runs Seen            : $($h['RunningRunsSeen'])"
    Write-Host "Cancelled Runs Seen          : $($h['CancelledRunsSeen'])"; Write-Host ''

    if ($ShowGridView -and (Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
        if ($MultipleGridViews) {
            $Tables.CurrentFailing | Out-GridView -Title "$Incident - Current Still Failing"
            $Tables.Recovered | Out-GridView -Title "$Incident - Recovered In Window"
            $Tables.Consecutive | Out-GridView -Title "$Incident - Consecutive Failures"
        } else { $Tables.QuickView | Out-GridView -Title "$Incident - Backup Failure Window Quick View" }
    } else {
        $Tables.QuickView | Select-Object Section,Cluster,Environment,ProtectionGroup,ObjectName,RunType,Status,LastFailedET,RecoveredET,ConsecutiveFailureCount | Format-Table -AutoSize
    }
    Write-Host 'Files Created:' -ForegroundColor Cyan
    Write-Host $WorkbookPath; Write-Host $WorkNotesPath; Write-Host $StatePath; Write-Host ''
    Write-Host 'Next Step: Attach XLSX to incident and paste WorkNotes_Paste.txt into work_notes.' -ForegroundColor Yellow
}

try {
    $window = Get-DynatraceComputeWindow
    $mappingInfo = Resolve-WindowMapping $window
    $mapping = $mappingInfo.Mapping
    $incident = $mapping.IncidentNumber
    $folder = $mapping.OutputFolder
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }

    $statePath = Join-Path $folder ("{0}_State.json" -f (Sanitize-Name $incident))
    $previousState = Read-Json $statePath $null
    $headers = @{ accept='application/json'; apiKey=(Get-ApiKey) }

    $collection = Collect-Events $headers $window $incident
    $tables = Build-Tables $collection.Events $previousState $window $incident $collection.RunEvidence $collection.Warnings

    $runStatus = @(
        [pscustomobject]@{Field='ScriptResult';Value='Success'},
        [pscustomobject]@{Field='GeneratedAtET';Value=Format-ET (Get-NowET)},
        [pscustomobject]@{Field='IncidentNumber';Value=$incident},
        [pscustomobject]@{Field='WindowKey';Value=$mappingInfo.WindowKey},
        [pscustomobject]@{Field='WindowLabel';Value=$window.WindowLabel},
        [pscustomobject]@{Field='WindowStartET';Value=Format-ET $window.StartET},
        [pscustomobject]@{Field='WindowEndET';Value=Format-ET $window.EndET},
        [pscustomobject]@{Field='SnStartUtc';Value=$window.SnStartUtc},
        [pscustomobject]@{Field='SnEndUtc';Value=$window.SnEndUtc},
        [pscustomobject]@{Field='WindowSource';Value=$window.Source},
        [pscustomobject]@{Field='WindowLockStatus';Value='Locked'},
        [pscustomobject]@{Field='PreviousRunFound';Value=[bool]$previousState},
        [pscustomobject]@{Field='WarningCount';Value=$collection.Warnings.Count}
    )
    $metadata = @(
        [pscustomobject]@{Field='HeliosBaseUrl';Value=$HeliosBaseUrl},
        [pscustomobject]@{Field='ApiKeyPath';Value=$ApiKeyPath},
        [pscustomobject]@{Field='OutputRoot';Value=$OutputRoot},
        [pscustomobject]@{Field='RegistryPath';Value=$mappingInfo.RegistryPath},
        [pscustomobject]@{Field='MaxRunsPerProtectionGroup';Value=$MaxRunsPerProtectionGroup},
        [pscustomobject]@{Field='DynatraceWindowStartHourET';Value=$DynatraceWindowStartHourET},
        [pscustomobject]@{Field='WindowDurationHours';Value=24},
        [pscustomobject]@{Field='ProductionApiMode';Value='GET-only'}
    )
    $warnTable = @($collection.Warnings | ForEach-Object { [pscustomobject]@{Warning=$_} }); if ($warnTable.Count -eq 0) { $warnTable=@([pscustomobject]@{Warning='No warnings'}) }

    $workbookPath = Join-Path $folder ("{0}_BackupFailure_WindowSummary.xlsx" -f (Sanitize-Name $incident))
    $workNotesPath = Join-Path $folder ("{0}_WorkNotes_Paste.txt" -f (Sanitize-Name $incident))
    $sheets = [ordered]@{
        '00_Run_Status'=$runStatus; '01_Summary'=$tables.Summary; '02_Current_Still_Failing'=$tables.CurrentFailing; '03_Recovered_In_Window'=$tables.Recovered;
        '04_New_Failures_Latest'=$tables.NewFailures; '05_New_Recoveries_Latest'=$tables.NewRecoveries; '06_Consecutive_Failures'=$tables.Consecutive;
        '07_Carry_Forward_Baseline'=$tables.CarryForward; '08_Event_History'=$tables.EventHistory; '09_Run_Evidence'=$tables.RunEvidence; '10_Metadata'=$metadata; '11_Warnings'=$warnTable
    }
    Export-Xlsx $workbookPath $sheets
    New-WorkNotes $tables $window $incident (Split-Path $workbookPath -Leaf) | Set-Content -Path $workNotesPath -Encoding UTF8
    Write-Json ([pscustomobject]@{
        IncidentNumber=$incident
        WindowKey=$mappingInfo.WindowKey
        WindowLabel=$window.WindowLabel
        WindowStartET=Format-ET $window.StartET
        WindowEndET=Format-ET $window.EndET
        SnStartUtc=$window.SnStartUtc
        SnEndUtc=$window.SnEndUtc
        WindowSource=$window.Source
        WindowLocked=$true
        SnowSysId=''
        SnowWorkNotesReadEnabled=$false
        LastRunET=Format-ET (Get-NowET)
        Objects=$tables.ObjectState
        Summary=$tables.Summary
        WorkbookPath=$workbookPath
        WorkNotesPath=$workNotesPath
    }) $statePath
    Show-Output $tables $window $incident $workbookPath $workNotesPath $statePath
} catch {
    Write-Host ''; Write-Host 'SCRIPT RESULT: FAILED' -ForegroundColor Red; Write-Host $_.Exception.Message -ForegroundColor Red; throw
}
