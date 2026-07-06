<#
.SYNOPSIS
  Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
  GET-only Cohesity Helios evidence tool for backup-failure incidents.
  The script locks one incident to the Dynatrace compute_window:
  America/New_York, 18:00 ET -> next day 18:00 ET.

  First run in a new DT window asks once for the incident number.
  Later runs in the same DT window reuse BackupFailure_WindowRegistry.json.

  Output behavior:
  - If ImportExcel / Export-Excel is available, create XLSX.
  - If XLSX export is unavailable, automatically create CSV fallback evidence.
  - No Microsoft Excel installation is required for CSV fallback.
#>

[CmdletBinding()]
param(
    [string]$HeliosBaseUrl = 'https://helios.cohesity.com',
    [string]$ApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow',
    [string]$IncidentNumber,
    [int]$MaxClusters = 0,
    [int]$MaxProtectionGroupsPerCluster = 0,
    [int]$MaxRunsPerProtectionGroup = 120,
    [bool]$ShowGridView = $true,
    [switch]$MultipleGridViews,
    [switch]$ForceCsv
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

function Get-Etz {
    try { [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
    catch { [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
}

function Get-NowEt {
    [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), (Get-Etz))
}

function FmtEt($Value) {
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { return '' }
    ([datetime]$Value).ToString('yyyy-MM-dd HH:mm:ss')
}

function FmtUtc([datetime]$Value) {
    $Value.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')
}

function EtToUtc([datetime]$Value) {
    [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($Value, [DateTimeKind]::Unspecified), (Get-Etz))
}

function UsecToEt($Usecs) {
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return $null }
    try {
        $epoch = [datetime]::SpecifyKind([datetime]'1970-01-01T00:00:00Z', [DateTimeKind]::Utc)
        [TimeZoneInfo]::ConvertTimeFromUtc($epoch.AddSeconds(([double]$Usecs) / 1000000), (Get-Etz))
    } catch { $null }
}

function Arr($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    @($Value)
}

function Prop($Object, [string[]]$Names) {
    if ($null -eq $Object) { return $null }
    foreach ($name in $Names) {
        if ($Object.PSObject.Properties.Name -contains $name) { return $Object.$name }
    }
    $null
}

function FirstArr($Object, [string[]]$Names) {
    foreach ($name in $Names) {
        $value = Prop $Object @($name)
        if ($null -ne $value) { return @(Arr $value) }
    }
    @()
}

function SafeName([string]$Name) {
    if ([string]::IsNullOrWhiteSpace($Name)) { return 'Unknown' }
    (($Name.Trim() -replace '[\/:*?"<>|]', '_') -replace '\s+', '_')
}

function Get-DtWindow {
    $now = Get-NowEt
    $start = $now.Date.AddHours(18)
    if ($now -lt $start) { $start = $start.AddDays(-1) }
    $end = $start.AddDays(1)

    [pscustomobject]@{
        StartET     = $start
        EndET       = $end
        WindowKey   = ('{0}_1800ET' -f $start.ToString('yyyy-MM-dd'))
        WindowLabel = ('{0} ET -> {1} ET' -f $start.ToString('yyyy-MM-dd HH:mm'), $end.ToString('yyyy-MM-dd HH:mm'))
        SnStartUtc  = FmtUtc (EtToUtc $start)
        SnEndUtc    = FmtUtc (EtToUtc $end)
        Source      = 'Dynatrace_compute_window'
    }
}

function Read-Json($Path, $Default) {
    if (Test-Path $Path) {
        $raw = Get-Content -Path $Path -Raw
        if (-not [string]::IsNullOrWhiteSpace($raw)) { return ($raw | ConvertFrom-Json) }
    }
    $Default
}

function Write-Json($Object, $Path) {
    $dir = Split-Path -Path $Path -Parent
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 50 | Set-Content -Path $Path -Encoding UTF8
}

function Resolve-Window($Window) {
    if (-not (Test-Path $OutputRoot)) { New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null }

    $registryPath = Join-Path $OutputRoot 'BackupFailure_WindowRegistry.json'
    $default = [pscustomobject]@{
        TimeZone            = 'America/New_York'
        WindowMode          = 'DynatraceDaily18ET'
        WindowDurationHours = 24
        WindowStartHourET   = 18
        Windows             = [pscustomobject]@{}
    }

    $registry = Read-Json $registryPath $default
    if (-not ($registry.PSObject.Properties.Name -contains 'Windows')) {
        $registry | Add-Member -MemberType NoteProperty -Name Windows -Value ([pscustomobject]@{})
    }

    $key = $Window.WindowKey
    if ($registry.Windows.PSObject.Properties.Name -contains $key) {
        $mapping = $registry.Windows.$key
        if ($IncidentNumber -and $IncidentNumber.Trim().ToUpper() -ne $mapping.IncidentNumber) {
            throw "Window $key is locked to $($mapping.IncidentNumber). Do not overwrite the locked incident."
        }
        $mapping.LastRunET = FmtEt (Get-NowEt)
        Write-Json $registry $registryPath
        return [pscustomobject]@{ RegistryPath = $registryPath; WindowKey = $key; Mapping = $mapping; IsNew = $false }
    }

    $inc = $IncidentNumber
    if (-not $inc) {
        Write-Host "New Dynatrace compute window detected: $($Window.WindowLabel)" -ForegroundColor Yellow
        $inc = Read-Host 'Enter incident number for this window'
    }
    if ([string]::IsNullOrWhiteSpace($inc)) { throw 'Incident number is required for a new DT compute window.' }
    $inc = $inc.Trim().ToUpper()

    $folder = Join-Path $OutputRoot (SafeName $inc)
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }

    $mapping = [pscustomobject]@{
        IncidentNumber           = $inc
        WindowKey                = $key
        WindowLabel              = $Window.WindowLabel
        WindowStartET            = FmtEt $Window.StartET
        WindowEndET              = FmtEt $Window.EndET
        SnStartUtc               = $Window.SnStartUtc
        SnEndUtc                 = $Window.SnEndUtc
        WindowLocked             = $true
        WindowSource             = $Window.Source
        FirstRunET               = FmtEt (Get-NowEt)
        LastRunET                = FmtEt (Get-NowEt)
        SnowSysId                = ''
        SnowWorkNotesReadEnabled = $false
        OutputFolder             = $folder
    }

    $registry.Windows | Add-Member -MemberType NoteProperty -Name $key -Value $mapping
    Write-Json $registry $registryPath
    [pscustomobject]@{ RegistryPath = $registryPath; WindowKey = $key; Mapping = $mapping; IsNew = $true }
}

function Get-ApiKeyValue {
    if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
    $key = (Get-Content -Path $ApiKeyPath -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key file is empty: $ApiKeyPath" }
    $key
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

    Invoke-RestMethod -Method GET -Uri $uri -Headers $Headers -TimeoutSec 120
}

function NormStatus($Status) {
    if (-not $Status) { return 'Unknown' }
    $value = ([string]$Status).Trim() -replace '^k', ''
    switch -Regex ($value) {
        'SucceededWithWarning|Succeeded|Success|Warning' { 'Succeeded'; break }
        'Fail|Failed|Failure|Error' { 'Failed'; break }
        'Cancel|Canceled|Cancelled' { 'Canceled'; break }
        'Running|Started|InProgress|Progress|Accepted' { 'Running'; break }
        default { $value }
    }
}

function NormEnv($Env) {
    if (-not $Env) { return 'Unknown' }
    $value = ([string]$Env).Trim() -replace '^k', ''
    switch -Regex ($value) {
        'Acropolis' { 'Nutanix'; break }
        'GenericNas' { 'NAS'; break }
        default { $value }
    }
}

function RunStatus($Run) {
    $status = Prop $Run @('status','backupRunStatus','runStatus')
    if (-not $status) { $status = Prop (Prop $Run @('localBackupInfo')) @('status') }
    NormStatus $status
}

function RunType($Run) {
    $type = Prop $Run @('runType','backupRunType')
    if (-not $type) { $type = Prop (Prop $Run @('localBackupInfo')) @('runType') }
    if (-not $type) { return 'Unknown' }
    ([string]$type -replace '^k', '')
}

function RunStart($Run) {
    $usecs = Prop $Run @('startTimeUsecs','runStartTimeUsecs')
    if (-not $usecs) { $usecs = Prop (Prop $Run @('localBackupInfo')) @('startTimeUsecs') }
    UsecToEt $usecs
}

function RunEnd($Run) {
    $usecs = Prop $Run @('endTimeUsecs','endUsecs','runEndTimeUsecs')
    if (-not $usecs) { $usecs = Prop (Prop $Run @('localBackupInfo')) @('endTimeUsecs') }
    if (-not $usecs) { $usecs = Prop $Run @('startTimeUsecs','runStartTimeUsecs') }
    UsecToEt $usecs
}

function Msg($Object) {
    foreach ($field in @('errorMessage','message','errorMsg','failureMessage','warningMessage','reason')) {
        $value = Prop $Object @($field)
        if ($value) { return [string]$value }
    }
    ''
}

function ObjName($Object) {
    $inner = Prop $Object @('object','entity','source')
    $name = Prop $Object @('name','objectName','displayName')
    if (-not $name) { $name = Prop $inner @('name','objectName','displayName') }
    if ($name) { [string]$name } else { 'UnknownObject' }
}

function ObjId($Object) {
    $inner = Prop $Object @('object','entity','source')
    $id = Prop $Object @('id','objectId','entityId','sourceId','uid')
    if (-not $id) { $id = Prop $inner @('id','objectId','entityId','sourceId','uid') }
    [string]$id
}

function ObjType($Object, $Environment) {
    $inner = Prop $Object @('object','entity','source')
    $type = Prop $Object @('type','objectType','entityType')
    if (-not $type) { $type = Prop $inner @('type','objectType','entityType') }
    if ($type) { ([string]$type -replace '^k', '') } else { $Environment }
}

function HostName($Object) {
    $host = Prop $Object @('host','hostName','parentName','sourceName','registeredSourceName')
    if ($host) { return [string]$host }
    $inner = Prop $Object @('object','entity','source')
    $host = Prop $inner @('parentName','hostName','sourceName','registeredSourceName')
    if ($host) { [string]$host } else { '' }
}

function ObjStatus($Object, $RunStatus) {
    $status = Prop $Object @('status','runStatus','protectionStatus','backupStatus')
    if (-not $status) { $status = $RunStatus }
    NormStatus $status
}

function ObjKey($ClusterId, $Environment, $PgId, $PgName, $Object) {
    $id = ObjId $Object
    if ($id) { return "$ClusterId|$Environment|$PgId|$id" }
    "$ClusterId|$Environment|$PgName|$(HostName $Object)|$(ObjName $Object)"
}

function Get-Clusters($Headers) {
    $json = Invoke-CohesityGet '/v2/mcm/cluster-mgmt/info' $Headers @{}
    $out = foreach ($cluster in (FirstArr $json @('clusters','clusterInfo','clusterInfos','items','data'))) {
        $id = Prop $cluster @('clusterId','id','uuid')
        $name = Prop $cluster @('clusterName','name','displayName','hostname')
        if ($id -or $name) { [pscustomobject]@{ ClusterId = [string]$id; ClusterName = [string]$name } }
    }
    if ($MaxClusters -gt 0) { @($out | Select-Object -First $MaxClusters) } else { @($out) }
}

function Get-ProtectionGroups($Headers) {
    $json = Invoke-CohesityGet '/v2/data-protect/protection-groups' $Headers @{ isDeleted='false'; isActive='true'; includeLastRunInfo='true' }
    $pgs = FirstArr $json @('protectionGroups','protectionGroupInfos','items','data')
    if ($MaxProtectionGroupsPerCluster -gt 0) { @($pgs | Select-Object -First $MaxProtectionGroupsPerCluster) } else { @($pgs) }
}

function Get-Runs($Headers, $PgId) {
    $encodedPgId = [uri]::EscapeDataString([string]$PgId)
    $json = Invoke-CohesityGet "/v2/data-protect/protection-groups/$encodedPgId/runs" $Headers @{ numRuns=[string]$MaxRunsPerProtectionGroup; includeObjectDetails='true' }
    FirstArr $json @('runs','protectionRuns','items','data')
}

function New-EventRow($Incident, $Time, $ClusterId, $Cluster, $Environment, $PgId, $PgName, $Host, $ObjectName, $ObjectType, $ObjectId, $ObjectKey, $RunType, $EventType, $Message, $RunStart, $RunEnd) {
    [pscustomobject]@{
        IncidentNumber    = $Incident
        EventTimeET       = FmtEt $Time
        ClusterId         = $ClusterId
        Cluster           = $Cluster
        Environment       = $Environment
        ProtectionGroupId = $PgId
        ProtectionGroup   = $PgName
        Host              = $Host
        ObjectName        = $ObjectName
        ObjectType        = $ObjectType
        ObjectId          = $ObjectId
        ObjectKey         = $ObjectKey
        RunType           = $RunType
        EventType         = $EventType
        Message           = $Message
        RunStartET        = FmtEt $RunStart
        RunEndET          = FmtEt $RunEnd
    }
}

function Collect-Events($Headers, $Window, $Incident) {
    $events = @()
    $evidence = @()
    $warnings = @()

    foreach ($cluster in (Get-Clusters $Headers)) {
        $clusterId = $cluster.ClusterId
        $clusterName = if ($cluster.ClusterName) { $cluster.ClusterName } else { $clusterId }
        $clusterHeaders = @{}
        foreach ($key in $Headers.Keys) { $clusterHeaders[$key] = $Headers[$key] }
        if ($clusterId) { $clusterHeaders['accessClusterId'] = $clusterId }

        try { $pgs = Get-ProtectionGroups $clusterHeaders }
        catch { $warnings += "Cluster $clusterName PG query failed: $($_.Exception.Message)"; continue }

        foreach ($pg in $pgs) {
            $pgId = [string](Prop $pg @('id','protectionGroupId','uid'))
            if (-not $pgId) { $pgId = [string](Prop $pg @('name','protectionGroupName')) }
            $pgName = [string](Prop $pg @('name','protectionGroupName'))
            if (-not $pgName) { $pgName = $pgId }
            $env = NormEnv (Prop $pg @('environment','env','protectionSourceEnvironment'))

            try { $runs = Get-Runs $clusterHeaders $pgId }
            catch { $warnings += "Cluster $clusterName PG $pgName run query failed: $($_.Exception.Message)"; continue }

            $oldest = $null
            foreach ($run in $runs) {
                $runStart = RunStart $run
                $runEnd = RunEnd $run
                $eventTime = if ($runEnd) { $runEnd } else { $runStart }
                if (-not $eventTime) { continue }
                if (-not $oldest -or $eventTime -lt $oldest) { $oldest = $eventTime }
                if ($eventTime -lt $Window.StartET -or $eventTime -ge $Window.EndET) { continue }

                $runStatus = RunStatus $run
                $runType = RunType $run
                $objects = FirstArr $run @('objects','objectRuns','objectRunList','tasks','taskRuns')

                $evidence += [pscustomobject]@{
                    IncidentNumber    = $Incident
                    Cluster           = $clusterName
                    Environment       = $env
                    ProtectionGroup   = $pgName
                    RunType           = $runType
                    RunStatus         = $runStatus
                    RunStartET        = FmtEt $runStart
                    RunEndET          = FmtEt $runEnd
                    ObjectDetailCount = $objects.Count
                    Message           = Msg $run
                }

                if ($objects.Count -eq 0) {
                    if ($runStatus -in @('Failed','Canceled','Running')) {
                        $eventType = if ($runStatus -eq 'Failed') { 'Failed' } elseif ($runStatus -eq 'Canceled') { 'CancelledRun' } else { 'RunningRun' }
                        $objectKey = "$clusterId|$env|$pgId|PG_LEVEL|$runType"
                        $events += New-EventRow $Incident $eventTime $clusterId $clusterName $env $pgId $pgName '' $pgName 'ProtectionGroup' '' $objectKey $runType $eventType (Msg $run) $runStart $runEnd
                    }
                    continue
                }

                foreach ($object in $objects) {
                    $objectStatus = ObjStatus $object $runStatus
                    $eventType = $null
                    if ($objectStatus -eq 'Running' -or $runStatus -eq 'Running') { $eventType = 'RunningRun' }
                    elseif ($objectStatus -eq 'Canceled' -or $runStatus -eq 'Canceled') { $eventType = 'CancelledRun' }
                    elseif ($objectStatus -eq 'Failed' -or $runStatus -eq 'Failed') { $eventType = 'Failed' }
                    elseif ($objectStatus -eq 'Succeeded' -or $runStatus -eq 'Succeeded') { $eventType = 'Succeeded' }
                    if (-not $eventType) { continue }

                    $objectId = ObjId $object
                    $objectKey = ObjKey $clusterId $env $pgId $pgName $object
                    $message = Msg $object
                    if (-not $message) { $message = Msg $run }

                    $events += New-EventRow $Incident $eventTime $clusterId $clusterName $env $pgId $pgName (HostName $object) (ObjName $object) (ObjType $object $env) $objectId $objectKey $runType $eventType $message $runStart $runEnd
                }
            }

            if ($oldest -and $oldest -gt $Window.StartET) {
                $warnings += "PG $pgName on $clusterName may be truncated; increase MaxRunsPerProtectionGroup."
            }
        }
    }

    [pscustomobject]@{ Events = $events; RunEvidence = $evidence; Warnings = $warnings }
}

function New-SectionRow($Event, $Section, $Status, $FirstFailedET, $LastFailedET, $RecoveredET, $Count) {
    [pscustomobject]@{
        Section                 = $Section
        Status                  = $Status
        IncidentNumber          = $Event.IncidentNumber
        Cluster                 = $Event.Cluster
        Environment             = $Event.Environment
        ProtectionGroup         = $Event.ProtectionGroup
        Host                    = $Event.Host
        ObjectName              = $Event.ObjectName
        ObjectType              = $Event.ObjectType
        RunType                 = $Event.RunType
        FirstFailedET           = $FirstFailedET
        LastFailedET            = $LastFailedET
        RecoveredET             = $RecoveredET
        ConsecutiveFailureCount = $Count
        Message                 = $Event.Message
        ObjectKey               = $Event.ObjectKey
    }
}

function Build-Tables($Events, $PreviousState, $Window, $Incident, $RunEvidence, $Warnings) {
    $prevFailing = @{}
    if ($PreviousState -and ($PreviousState.PSObject.Properties.Name -contains 'Objects')) {
        foreach ($item in @($PreviousState.Objects)) {
            if ($item.CurrentStatus -in @('StillFailing','ReFailed')) { $prevFailing[[string]$item.ObjectKey] = $item }
        }
    }

    $byKey = @{}
    foreach ($event in @($Events | Sort-Object EventTimeET)) {
        if (-not $byKey.ContainsKey($event.ObjectKey)) { $byKey[$event.ObjectKey] = @() }
        $byKey[$event.ObjectKey] += $event
    }

    $current = @(); $recovered = @(); $newFail = @(); $newRec = @(); $consec = @(); $running = @(); $cancelled = @(); $state = @()

    foreach ($key in $byKey.Keys) {
        $evs = @($byKey[$key] | Sort-Object EventTimeET)
        $fails = @($evs | Where-Object EventType -eq 'Failed')
        $successes = @($evs | Where-Object EventType -eq 'Succeeded')

        foreach ($run in @($evs | Where-Object EventType -eq 'RunningRun')) { $running += New-SectionRow $run 'Running Run' 'RunningAtLatestCheck' '' '' '' 0 }
        foreach ($cancel in @($evs | Where-Object EventType -eq 'CancelledRun')) { $cancelled += New-SectionRow $cancel 'Cancelled Run' 'CancelledInWindow' '' $cancel.EventTimeET '' 0 }
        if ($fails.Count -eq 0) { continue }

        $firstFail = $fails | Select-Object -First 1
        $lastFail = $fails | Select-Object -Last 1
        $successBeforeLastFail = $successes | Where-Object { [datetime]$_.EventTimeET -lt [datetime]$lastFail.EventTimeET } | Select-Object -Last 1
        $successAfterLastFail = $successes | Where-Object { [datetime]$_.EventTimeET -gt [datetime]$lastFail.EventTimeET } | Select-Object -First 1
        $consecutiveCount = if ($successBeforeLastFail) { @($fails | Where-Object { [datetime]$_.EventTimeET -gt [datetime]$successBeforeLastFail.EventTimeET }).Count } else { $fails.Count }

        if ($successAfterLastFail) {
            $recovered += New-SectionRow $lastFail 'Recovered In Window' 'RecoveredInWindow' $firstFail.EventTimeET $lastFail.EventTimeET $successAfterLastFail.EventTimeET $consecutiveCount
            if ($prevFailing.ContainsKey($key)) { $newRec += New-SectionRow $lastFail 'New Recovery' 'NewlyRecoveredThisCheck' $firstFail.EventTimeET $lastFail.EventTimeET $successAfterLastFail.EventTimeET $consecutiveCount }
            $state += [pscustomobject]@{ ObjectKey=$key; Cluster=$lastFail.Cluster; Environment=$lastFail.Environment; ProtectionGroup=$lastFail.ProtectionGroup; Host=$lastFail.Host; ObjectName=$lastFail.ObjectName; ObjectType=$lastFail.ObjectType; RunType=$lastFail.RunType; CurrentStatus='RecoveredInWindow'; FirstFailedET=$firstFail.EventTimeET; LastFailedET=$lastFail.EventTimeET; RecoveredET=$successAfterLastFail.EventTimeET; ConsecutiveFailureCount=$consecutiveCount; LastMessage=$lastFail.Message }
        } else {
            $status = if ($successBeforeLastFail) { 'ReFailed' } else { 'StillFailing' }
            $current += New-SectionRow $lastFail 'Current Still Failing' $status $firstFail.EventTimeET $lastFail.EventTimeET '' $consecutiveCount
            if (-not $prevFailing.ContainsKey($key)) { $newFail += New-SectionRow $lastFail 'New Failure' 'NewlyFailedThisCheck' $firstFail.EventTimeET $lastFail.EventTimeET '' $consecutiveCount }
            if ($consecutiveCount -gt 1) { $consec += New-SectionRow $lastFail 'Consecutive Failure' 'ConsecutiveFailure' $firstFail.EventTimeET $lastFail.EventTimeET '' $consecutiveCount }
            $state += [pscustomobject]@{ ObjectKey=$key; Cluster=$lastFail.Cluster; Environment=$lastFail.Environment; ProtectionGroup=$lastFail.ProtectionGroup; Host=$lastFail.Host; ObjectName=$lastFail.ObjectName; ObjectType=$lastFail.ObjectType; RunType=$lastFail.RunType; CurrentStatus=$status; FirstFailedET=$firstFail.EventTimeET; LastFailedET=$lastFail.EventTimeET; RecoveredET=''; ConsecutiveFailureCount=$consecutiveCount; LastMessage=$lastFail.Message }
        }
    }

    $failedKeys = @($Events | Where-Object EventType -eq 'Failed' | Select-Object -ExpandProperty ObjectKey -Unique)
    $clusters = @($Events | Where-Object Cluster | Select-Object -ExpandProperty Cluster -Unique)
    $envs = @($Events | Where-Object Environment | Select-Object -ExpandProperty Environment -Unique)
    $pgs = @($Events | Where-Object ProtectionGroup | Select-Object -ExpandProperty ProtectionGroup -Unique)

    $summary = @(
        [pscustomobject]@{ Metric='IncidentNumber'; Value=$Incident },
        [pscustomobject]@{ Metric='WindowKey'; Value=$Window.WindowKey },
        [pscustomobject]@{ Metric='WindowLabel'; Value=$Window.WindowLabel },
        [pscustomobject]@{ Metric='WindowStartET'; Value=FmtEt $Window.StartET },
        [pscustomobject]@{ Metric='WindowEndET'; Value=FmtEt $Window.EndET },
        [pscustomobject]@{ Metric='SnStartUtc'; Value=$Window.SnStartUtc },
        [pscustomobject]@{ Metric='SnEndUtc'; Value=$Window.SnEndUtc },
        [pscustomobject]@{ Metric='GeneratedAtET'; Value=FmtEt (Get-NowEt) },
        [pscustomobject]@{ Metric='TotalUniqueObjectsFailedInWindow'; Value=$failedKeys.Count },
        [pscustomobject]@{ Metric='RecoveredInWindow'; Value=$recovered.Count },
        [pscustomobject]@{ Metric='StillFailingAtLatestCheck'; Value=$current.Count },
        [pscustomobject]@{ Metric='NewFailuresSincePreviousRun'; Value=$newFail.Count },
        [pscustomobject]@{ Metric='NewRecoveriesSincePreviousRun'; Value=$newRec.Count },
        [pscustomobject]@{ Metric='ConsecutiveRepeatedFailures'; Value=$consec.Count },
        [pscustomobject]@{ Metric='RunningRunsSeen'; Value=$running.Count },
        [pscustomobject]@{ Metric='CancelledRunsSeen'; Value=$cancelled.Count },
        [pscustomobject]@{ Metric='ImpactedClusters'; Value=$clusters.Count },
        [pscustomobject]@{ Metric='ImpactedEnvironments'; Value=($envs -join '; ') },
        [pscustomobject]@{ Metric='ImpactedProtectionGroups'; Value=$pgs.Count },
        [pscustomobject]@{ Metric='WarningCount'; Value=$Warnings.Count }
    )

    [pscustomobject]@{
        Summary        = $summary
        CurrentFailing = $current
        Recovered      = $recovered
        NewFailures    = $newFail
        NewRecoveries  = $newRec
        Consecutive    = $consec
        CarryForward   = $current
        EventHistory   = @($Events | Sort-Object EventTimeET)
        RunEvidence    = $RunEvidence
        QuickView      = @($current + $recovered + $newFail + $newRec + $consec + $running + $cancelled)
        ObjectState    = $state
        Warnings       = $Warnings
    }
}

function Export-CsvEvidencePackage($CsvFolder, [System.Collections.IDictionary]$Sheets) {
    if (Test-Path $CsvFolder) { Remove-Item -Path $CsvFolder -Recurse -Force }
    New-Item -ItemType Directory -Path $CsvFolder -Force | Out-Null

    $created = @()
    foreach ($name in $Sheets.Keys) {
        $rows = @($Sheets[$name])
        if ($rows.Count -eq 0) { $rows = @([pscustomobject]@{ Info = 'No rows' }) }
        $csvPath = Join-Path $CsvFolder ("{0}.csv" -f (SafeName $name))
        $rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
        $created += $csvPath
    }

    $manifestPath = Join-Path $CsvFolder '00_Attach_These_CSV_Files.txt'
    @(
        'CSV evidence package generated because XLSX export was unavailable or ForceCsv was used.',
        'Attach these CSV files to the ServiceNow incident, or zip this folder and attach the zip.',
        '',
        'Files:',
        ($created | ForEach-Object { Split-Path $_ -Leaf })
    ) | Set-Content -Path $manifestPath -Encoding UTF8

    [pscustomobject]@{ Format = 'CSV'; Folder = $CsvFolder; Files = @($created + $manifestPath); EvidenceText = "CSV evidence folder: $(Split-Path $CsvFolder -Leaf)" }
}

function Export-XlsxUsingImportExcel($WorkbookPath, [System.Collections.IDictionary]$Sheets) {
    if (Test-Path $WorkbookPath) { Remove-Item -Path $WorkbookPath -Force }
    $first = $true
    foreach ($name in $Sheets.Keys) {
        $rows = @($Sheets[$name])
        if ($rows.Count -eq 0) { $rows = @([pscustomobject]@{ Info = 'No rows' }) }
        $sheetName = if ($name.Length -gt 31) { $name.Substring(0,31) } else { $name }
        $params = @{ Path=$WorkbookPath; WorksheetName=$sheetName; AutoSize=$true; FreezeTopRow=$true; BoldTopRow=$true }
        if (-not $first) { $params.Append = $true }
        $rows | Export-Excel @params
        $first = $false
    }
    [pscustomobject]@{ Format = 'XLSX'; WorkbookPath = $WorkbookPath; Files = @($WorkbookPath); EvidenceText = "Attachment: $(Split-Path $WorkbookPath -Leaf)" }
}

function Export-EvidencePackage($Folder, $Incident, [System.Collections.IDictionary]$Sheets) {
    $workbookPath = Join-Path $Folder ("{0}_BackupFailure_WindowSummary.xlsx" -f (SafeName $Incident))
    $csvFolder = Join-Path $Folder ("{0}_BackupFailure_CSV_Evidence" -f (SafeName $Incident))

    if (-not $ForceCsv -and (Get-Command Export-Excel -ErrorAction SilentlyContinue)) {
        try { return Export-XlsxUsingImportExcel $workbookPath $Sheets }
        catch {
            Write-Warning "XLSX export failed. Falling back to CSV. Error: $($_.Exception.Message)"
            return Export-CsvEvidencePackage $csvFolder $Sheets
        }
    }

    Export-CsvEvidencePackage $csvFolder $Sheets
}

function New-WorkNotes($Tables, $Window, $Incident, $EvidenceText) {
    $h = @{}
    foreach ($row in $Tables.Summary) { $h[$row.Metric] = $row.Value }

    @(
        'Backup Failure Window Summary','',
        "Incident: $Incident",
        "Locked Compute Window: $($Window.WindowLabel)",
        "SNOW Compare UTC: $($Window.SnStartUtc) to $($Window.SnEndUtc)",
        "Generated At: $(FmtEt (Get-NowEt)) ET",
        'Source: Cohesity Helios API / PowerShell Window Consolidator','',
        'Summary:',
        "- Total unique objects failed in this window: $($h['TotalUniqueObjectsFailedInWindow'])",
        "- Recovered within this window: $($h['RecoveredInWindow'])",
        "- Still failing at latest check within this window: $($h['StillFailingAtLatestCheck'])",
        "- New failures since previous check: $($h['NewFailuresSincePreviousRun'])",
        "- New recoveries since previous check: $($h['NewRecoveriesSincePreviousRun'])",
        "- Consecutive/repeated failures: $($h['ConsecutiveRepeatedFailures'])",
        "- Running backup runs seen: $($h['RunningRunsSeen'])",
        "- Cancelled backup runs seen: $($h['CancelledRunsSeen'])",
        "- Impacted clusters: $($h['ImpactedClusters'])",
        "- Impacted environments: $($h['ImpactedEnvironments'])",
        "- Impacted protection groups: $($h['ImpactedProtectionGroups'])",'',
        'Current Still Failing: See evidence tab/file 02_Current_Still_Failing',
        'Recovered During Window: See evidence tab/file 03_Recovered_In_Window',
        'Consecutive / Repeated Failures: See evidence tab/file 06_Consecutive_Failures',
        'Carry Forward Baseline: See evidence tab/file 07_Carry_Forward_Baseline','',
        'Note: Running runs are listed separately and are not treated as failed or recovered until they complete.','',
        $EvidenceText
    ) -join [Environment]::NewLine
}

try {
    $window = Get-DtWindow
    $mappingInfo = Resolve-Window $window
    $incident = $mappingInfo.Mapping.IncidentNumber
    $folder = $mappingInfo.Mapping.OutputFolder

    $statePath = Join-Path $folder ("{0}_State.json" -f (SafeName $incident))
    $previousState = Read-Json $statePath $null
    $headers = @{ accept='application/json'; apiKey=(Get-ApiKeyValue) }

    $collection = Collect-Events $headers $window $incident
    $tables = Build-Tables $collection.Events $previousState $window $incident $collection.RunEvidence $collection.Warnings

    $runStatus = @(
        [pscustomobject]@{ Field='ScriptResult'; Value='Success' },
        [pscustomobject]@{ Field='IncidentNumber'; Value=$incident },
        [pscustomobject]@{ Field='WindowKey'; Value=$window.WindowKey },
        [pscustomobject]@{ Field='WindowLabel'; Value=$window.WindowLabel },
        [pscustomobject]@{ Field='SnStartUtc'; Value=$window.SnStartUtc },
        [pscustomobject]@{ Field='SnEndUtc'; Value=$window.SnEndUtc },
        [pscustomobject]@{ Field='ProductionApiMode'; Value='GET-only' },
        [pscustomobject]@{ Field='WarningCount'; Value=$collection.Warnings.Count }
    )

    $metadata = @(
        [pscustomobject]@{ Field='HeliosBaseUrl'; Value=$HeliosBaseUrl },
        [pscustomobject]@{ Field='ApiKeyPath'; Value=$ApiKeyPath },
        [pscustomobject]@{ Field='OutputRoot'; Value=$OutputRoot },
        [pscustomobject]@{ Field='RegistryPath'; Value=$mappingInfo.RegistryPath },
        [pscustomobject]@{ Field='MaxRunsPerProtectionGroup'; Value=$MaxRunsPerProtectionGroup },
        [pscustomobject]@{ Field='XlsxAvailable'; Value=[bool](Get-Command Export-Excel -ErrorAction SilentlyContinue) },
        [pscustomobject]@{ Field='ForceCsv'; Value=[bool]$ForceCsv }
    )

    $warningRows = @($collection.Warnings | ForEach-Object { [pscustomobject]@{ Warning=$_ } })
    if ($warningRows.Count -eq 0) { $warningRows = @([pscustomobject]@{ Warning='No warnings' }) }

    $sheets = [ordered]@{
        '00_Run_Status'             = $runStatus
        '01_Summary'                = $tables.Summary
        '02_Current_Still_Failing'  = $tables.CurrentFailing
        '03_Recovered_In_Window'    = $tables.Recovered
        '04_New_Failures_Latest'    = $tables.NewFailures
        '05_New_Recoveries_Latest'  = $tables.NewRecoveries
        '06_Consecutive_Failures'   = $tables.Consecutive
        '07_Carry_Forward_Baseline' = $tables.CarryForward
        '08_Event_History'          = $tables.EventHistory
        '09_Run_Evidence'           = $tables.RunEvidence
        '10_Metadata'               = $metadata
        '11_Warnings'               = $warningRows
    }

    $evidence = Export-EvidencePackage $folder $incident $sheets
    $workNotesPath = Join-Path $folder ("{0}_WorkNotes_Paste.txt" -f (SafeName $incident))
    New-WorkNotes $tables $window $incident $evidence.EvidenceText | Set-Content -Path $workNotesPath -Encoding UTF8

    Write-Json ([pscustomobject]@{
        IncidentNumber = $incident
        WindowKey      = $window.WindowKey
        WindowLabel    = $window.WindowLabel
        SnStartUtc     = $window.SnStartUtc
        SnEndUtc       = $window.SnEndUtc
        WindowLocked   = $true
        WindowSource   = $window.Source
        LastRunET      = FmtEt (Get-NowEt)
        ReportFormat   = $evidence.Format
        EvidencePath   = $(if ($evidence.Format -eq 'XLSX') { $evidence.WorkbookPath } else { $evidence.Folder })
        WorkNotesPath  = $workNotesPath
        Objects        = $tables.ObjectState
        Summary        = $tables.Summary
    }) $statePath

    $h = @{}
    foreach ($row in $tables.Summary) { $h[$row.Metric] = $row.Value }

    Write-Host "`nIncident: $incident" -ForegroundColor Cyan
    Write-Host "Window  : $($window.WindowLabel)`n"
    Write-Host 'Summary:' -ForegroundColor Cyan
    Write-Host "Total Failed In Window       : $($h['TotalUniqueObjectsFailedInWindow'])"
    Write-Host "Recovered In Window          : $($h['RecoveredInWindow'])"
    Write-Host "Still Failing Now            : $($h['StillFailingAtLatestCheck'])"
    Write-Host "New Failures Since Last Run  : $($h['NewFailuresSincePreviousRun'])"
    Write-Host "New Recoveries Since Last Run: $($h['NewRecoveriesSincePreviousRun'])"
    Write-Host "Consecutive Failures         : $($h['ConsecutiveRepeatedFailures'])"
    Write-Host "Running Runs Seen            : $($h['RunningRunsSeen'])"
    Write-Host "Cancelled Runs Seen          : $($h['CancelledRunsSeen'])`n"

    if ($ShowGridView -and (Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
        if ($MultipleGridViews) {
            $tables.CurrentFailing | Out-GridView -Title "$incident - Current Still Failing"
            $tables.Recovered | Out-GridView -Title "$incident - Recovered In Window"
            $tables.Consecutive | Out-GridView -Title "$incident - Consecutive Failures"
        } else {
            $tables.QuickView | Out-GridView -Title "$incident - Backup Failure Window Quick View"
        }
    } else {
        $tables.QuickView | Select-Object Section,Cluster,Environment,ProtectionGroup,ObjectName,RunType,Status,LastFailedET,RecoveredET,ConsecutiveFailureCount | Format-Table -AutoSize
    }

    Write-Host 'Files Created:' -ForegroundColor Cyan
    if ($evidence.Format -eq 'XLSX') { Write-Host $evidence.WorkbookPath }
    else { Write-Host $evidence.Folder }
    Write-Host $workNotesPath
    Write-Host $statePath
    Write-Host ''
    Write-Host 'Next Step: Attach evidence files to incident and paste WorkNotes_Paste.txt into work_notes.' -ForegroundColor Yellow
}
catch {
    Write-Host ''
    Write-Host 'SCRIPT RESULT: FAILED' -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    throw
}
