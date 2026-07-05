<#
.SYNOPSIS
    Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
    Operator-focused PowerShell tool for consolidating backup failures inside a locked compute window.

    This is not a generic Cohesity report. It tracks lifecycle state for the current Dynatrace-style
    compute window and produces:
      - One XLSX workbook with tabs
      - One work_notes paste text file
      - One JSON state file
      - One consolidated Out-GridView quick view

    The engineer enters the incident number only once when a new compute window starts.
    Subsequent runs inside the same window reuse the locked incident mapping.

.NOTES
    GET-only Cohesity API usage.
    Default API key path follows existing Cohesity automation convention.
#>

[CmdletBinding()]
param(
    [string]$HeliosBaseUrl = "https://helios.cohesity.com",
    [string]$ApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",

    # Window defaults: 6-hour windows aligned to 00/06/12/18 ET.
    # Change if Dynatrace uses a different compute-window cadence.
    [int]$WindowDurationHours = 6,
    [int]$AnchorHourET = 0,

    # Optional override for review/testing. If omitted, script detects current ET window.
    [datetime]$WindowStartET,
    [datetime]$WindowEndET,

    # Optional. If not supplied for a new window, script prompts once and locks the value.
    [string]$IncidentNumber,

    [int]$MaxRunsPerProtectionGroup = 50,
    [int]$MaxClusters = 0,
    [int]$MaxProtectionGroupsPerCluster = 0,

    [bool]$ShowGridView = $true,
    [switch]$MultipleGridViews,
    [switch]$VerboseMode
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

# -----------------------------
# Core helpers
# -----------------------------
function Get-EasternTimeZone {
    try { return [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { return [System.TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}

function Get-NowET {
    $tz = Get-EasternTimeZone
    return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
}

function Format-ET([object]$Value) {
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { return "" }
    try {
        if ($Value -is [datetime]) { return $Value.ToString("yyyy-MM-dd HH:mm:ss") }
        return ([datetime]$Value).ToString("yyyy-MM-dd HH:mm:ss")
    } catch { return [string]$Value }
}

function Usecs-ToET([object]$Usecs) {
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return $null }
    try {
        $epoch = [datetime]::SpecifyKind([datetime]"1970-01-01T00:00:00Z", [DateTimeKind]::Utc)
        $utc = $epoch.AddSeconds(([double]$Usecs) / 1000000)
        $tz = Get-EasternTimeZone
        return [System.TimeZoneInfo]::ConvertTimeFromUtc($utc, $tz)
    } catch { return $null }
}

function DateTimeET-ToUsecs([datetime]$EtDateTime) {
    $tz = Get-EasternTimeZone
    $unspecified = [datetime]::SpecifyKind($EtDateTime, [DateTimeKind]::Unspecified)
    $utc = [System.TimeZoneInfo]::ConvertTimeToUtc($unspecified, $tz)
    $epoch = [datetime]::SpecifyKind([datetime]"1970-01-01T00:00:00Z", [DateTimeKind]::Utc)
    return [int64](($utc - $epoch).TotalSeconds * 1000000)
}

function Get-CurrentComputeWindow {
    param(
        [int]$DurationHours,
        [int]$AnchorHour,
        [datetime]$OverrideStart,
        [datetime]$OverrideEnd
    )

    if ($OverrideStart -and $OverrideEnd) {
        return [pscustomobject]@{
            StartET = $OverrideStart
            EndET   = $OverrideEnd
            Source  = "ManualOverride"
        }
    }

    $now = Get-NowET
    $baseDate = $now.Date.AddHours($AnchorHour)
    if ($now -lt $baseDate) { $baseDate = $baseDate.AddDays(-1) }

    $hoursFromAnchor = ($now - $baseDate).TotalHours
    $slot = [math]::Floor($hoursFromAnchor / $DurationHours)
    $start = $baseDate.AddHours($slot * $DurationHours)
    $end = $start.AddHours($DurationHours)

    return [pscustomobject]@{
        StartET = $start
        EndET   = $end
        Source  = "Detected"
    }
}

function Get-WindowKey([datetime]$StartET, [datetime]$EndET) {
    return ("{0}_{1}" -f $StartET.ToString("yyyyMMdd_HHmm"), $EndET.ToString("yyyyMMdd_HHmm"))
}

function Read-JsonFile($Path, $DefaultObject) {
    if (Test-Path $Path) {
        $raw = Get-Content -Path $Path -Raw
        if (-not [string]::IsNullOrWhiteSpace($raw)) {
            return ($raw | ConvertFrom-Json)
        }
    }
    return $DefaultObject
}

function Save-JsonFile($Object, $Path) {
    $folder = Split-Path -Path $Path -Parent
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 30 | Set-Content -Path $Path -Encoding UTF8
}

function Sanitize-Name([string]$Name) {
    if ([string]::IsNullOrWhiteSpace($Name)) { return "Unknown" }
    return (($Name -replace '[\\/:*?"<>|]', '_') -replace '\s+', '_')
}

function Ensure-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [System.Array]) { return @($Value) }
    return @($Value)
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
        $val = Get-Prop $Obj @($name)
        if ($null -ne $val) { return @(Ensure-Array $val) }
    }
    return @()
}

function Normalize-Status($Status) {
    if ($null -eq $Status) { return "Unknown" }
    $s = ([string]$Status).Trim()
    $s = $s -replace '^k', ''
    switch -Regex ($s) {
        'Succeeded|Success|SucceededWithWarning|Warning' { return "Succeeded" }
        'Fail|Failed|Failure|Error' { return "Failed" }
        'Cancel|Canceled|Cancelled' { return "Canceled" }
        'Running|Accepted|Started|InProgress' { return "Running" }
        default { return $s }
    }
}

function Is-SuccessStatus([string]$Status) {
    return (Normalize-Status $Status) -eq "Succeeded"
}

function Is-FailureStatus([string]$Status) {
    return (Normalize-Status $Status) -eq "Failed"
}

function RunType-Name($Value) {
    if ($null -eq $Value) { return "Unknown" }
    $s = ([string]$Value).Trim() -replace '^k', ''
    switch -Regex ($s) {
        'Full' { return "Full" }
        'Incremental|Regular' { return "Incremental" }
        'Log' { return "Log" }
        'System' { return "System" }
        default { return $s }
    }
}

function Environment-Name($Value) {
    if ($null -eq $Value) { return "Unknown" }
    $s = ([string]$Value).Trim() -replace '^k', ''
    switch -Regex ($s) {
        'Acropolis' { return "Nutanix" }
        'GenericNas' { return "NAS" }
        default { return $s }
    }
}

function Get-ErrorMessage($Obj) {
    if ($null -eq $Obj) { return "" }
    $fields = @('errorMessage','message','errorMsg','failureMessage','warningMessage','reason')
    foreach ($f in $fields) {
        $v = Get-Prop $Obj @($f)
        if ($v) { return [string]$v }
    }

    $err = Get-Prop $Obj @('error','errors')
    if ($err) {
        $arr = Ensure-Array $err
        $m = Get-Prop $arr[0] @('errorMsg','message','errorMessage')
        if ($m) { return [string]$m }
    }

    $warnings = Get-Prop $Obj @('warnings')
    if ($warnings) {
        $arr = Ensure-Array $warnings
        $m = Get-Prop $arr[0] @('errorMsg','message','errorMessage')
        if ($m) { return [string]$m }
    }

    return ""
}

function Get-RunTimeET($Run) {
    $usecs = Get-Prop $Run @('endTimeUsecs','endUsecs','runEndTimeUsecs')
    if (-not $usecs) {
        $local = Get-Prop $Run @('localBackupInfo')
        $usecs = Get-Prop $local @('endTimeUsecs','startTimeUsecs')
    }
    if (-not $usecs) { $usecs = Get-Prop $Run @('startTimeUsecs','runStartTimeUsecs') }
    return Usecs-ToET $usecs
}

function Get-RunStartET($Run) {
    $usecs = Get-Prop $Run @('startTimeUsecs','runStartTimeUsecs')
    if (-not $usecs) {
        $local = Get-Prop $Run @('localBackupInfo')
        $usecs = Get-Prop $local @('startTimeUsecs')
    }
    return Usecs-ToET $usecs
}

function Get-RunStatus($Run) {
    $status = Get-Prop $Run @('status','backupRunStatus','runStatus')
    if (-not $status) {
        $local = Get-Prop $Run @('localBackupInfo')
        $status = Get-Prop $local @('status')
    }
    return Normalize-Status $status
}

function Get-RunType($Run) {
    $runType = Get-Prop $Run @('runType','backupRunType')
    if (-not $runType) {
        $local = Get-Prop $Run @('localBackupInfo')
        $runType = Get-Prop $local @('runType')
    }
    return RunType-Name $runType
}

function Get-RunObjects($Run) {
    $objects = First-Array $Run @('objects','objectRuns','objectRunList','tasks','taskRuns')
    if ($objects.Count -eq 0) {
        $inner = Get-Prop $Run @('run','protectionRun','backupRun')
        if ($inner) { $objects = First-Array $inner @('objects','objectRuns','tasks','taskRuns') }
    }
    return $objects
}

function Get-ObjectName($Obj) {
    $object = Get-Prop $Obj @('object','entity','source')
    $name = Get-Prop $Obj @('name','objectName','displayName')
    if (-not $name) { $name = Get-Prop $object @('name','objectName','displayName') }
    if (-not $name) { $name = "UnknownObject" }
    return [string]$name
}

function Get-ObjectId($Obj) {
    $object = Get-Prop $Obj @('object','entity','source')
    $id = Get-Prop $Obj @('id','objectId','entityId','sourceId')
    if (-not $id) { $id = Get-Prop $object @('id','objectId','entityId','sourceId') }
    return [string]$id
}

function Get-ObjectType($Obj, [string]$Environment) {
    $object = Get-Prop $Obj @('object','entity','source')
    $type = Get-Prop $Obj @('type','objectType','entityType')
    if (-not $type) { $type = Get-Prop $object @('type','objectType','entityType') }
    if (-not $type) { $type = $Environment }
    return ([string]$type -replace '^k', '')
}

function Get-HostName($Obj) {
    $host = Get-Prop $Obj @('host','hostName','parentName','sourceName','registeredSourceName')
    if ($host) { return [string]$host }
    $object = Get-Prop $Obj @('object','entity','source')
    $host = Get-Prop $object @('parentName','hostName','sourceName','registeredSourceName')
    if ($host) { return [string]$host }
    return ""
}

function Get-ObjectStatus($Obj, [string]$RunStatus) {
    $status = Get-Prop $Obj @('status','runStatus','protectionStatus','backupStatus')
    if (-not $status) {
        $snap = Get-Prop $Obj @('localSnapshotInfo','snapshotInfo')
        $status = Get-Prop $snap @('status')
    }
    if (-not $status) { $status = $RunStatus }
    return Normalize-Status $status
}

function Get-ObjectKey($ClusterId, $Environment, $PgId, $PgName, $Obj) {
    $objectId = Get-ObjectId $Obj
    if (-not [string]::IsNullOrWhiteSpace($objectId)) {
        return "$ClusterId|$Environment|$PgId|$objectId"
    }
    $host = Get-HostName $Obj
    $name = Get-ObjectName $Obj
    return "$ClusterId|$Environment|$PgName|$host|$name"
}

# -----------------------------
# API helpers
# -----------------------------
function Get-ApiKey {
    param([string]$Path)
    if (-not (Test-Path $Path)) { throw "API key file not found: $Path" }
    $key = (Get-Content -Path $Path -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key file is empty: $Path" }
    return $key
}

function Invoke-CohesityGet {
    param(
        [string]$Path,
        [hashtable]$Headers,
        [hashtable]$Query
    )

    $base = $HeliosBaseUrl.TrimEnd('/')
    $uri = "$base$Path"
    if ($Query -and $Query.Count -gt 0) {
        $pairs = @()
        foreach ($k in $Query.Keys) {
            if ($null -ne $Query[$k] -and [string]$Query[$k] -ne "") {
                $pairs += ("{0}={1}" -f [uri]::EscapeDataString([string]$k), [uri]::EscapeDataString([string]$Query[$k]))
            }
        }
        if ($pairs.Count -gt 0) { $uri = "$uri?$(($pairs -join '&'))" }
    }

    $lastError = $null
    for ($i = 1; $i -le 3; $i++) {
        try {
            return Invoke-RestMethod -Method GET -Uri $uri -Headers $Headers -TimeoutSec 120
        } catch {
            $lastError = $_
            Start-Sleep -Seconds ([math]::Min(2 * $i, 6))
        }
    }
    throw $lastError
}

function Get-Clusters($Headers) {
    $json = Invoke-CohesityGet -Path "/v2/mcm/cluster-mgmt/info" -Headers $Headers -Query @{}
    $list = First-Array $json @('clusters','clusterInfo','clusterInfos','items','data')
    $clusters = @()
    foreach ($c in $list) {
        $id = Get-Prop $c @('clusterId','id','uuid')
        $name = Get-Prop $c @('clusterName','name','displayName','hostname')
        if ($id -or $name) {
            $clusters += [pscustomobject]@{ ClusterId = [string]$id; ClusterName = [string]$name }
        }
    }
    if ($MaxClusters -gt 0) { $clusters = @($clusters | Select-Object -First $MaxClusters) }
    return $clusters
}

function Get-ProtectionGroups($Headers) {
    $json = Invoke-CohesityGet -Path "/v2/data-protect/protection-groups" -Headers $Headers -Query @{
        isDeleted = "false"
        isActive = "true"
        includeLastRunInfo = "true"
    }
    $pgs = First-Array $json @('protectionGroups','protectionGroupInfos','items','data')
    if ($MaxProtectionGroupsPerCluster -gt 0) { $pgs = @($pgs | Select-Object -First $MaxProtectionGroupsPerCluster) }
    return $pgs
}

function Get-ProtectionGroupRuns($Headers, [string]$PgId) {
    $encoded = [uri]::EscapeDataString($PgId)
    $json = Invoke-CohesityGet -Path "/v2/data-protect/protection-groups/$encoded/runs" -Headers $Headers -Query @{
        numRuns = [string]$MaxRunsPerProtectionGroup
        includeObjectDetails = "true"
    }
    return First-Array $json @('runs','protectionRuns','items','data')
}

# -----------------------------
# Window registry
# -----------------------------
function Resolve-WindowMapping {
    param(
        [object]$Window,
        [string]$ProvidedIncident,
        [string]$Root
    )

    if (-not (Test-Path $Root)) { New-Item -ItemType Directory -Path $Root -Force | Out-Null }

    $registryPath = Join-Path $Root "BackupFailure_WindowRegistry.json"
    $default = [pscustomobject]@{
        TimeZone = "America/New_York"
        WindowMode = "DynatraceComputeWindow"
        WindowDurationHours = $WindowDurationHours
        AnchorHourET = $AnchorHourET
        Windows = [pscustomobject]@{}
    }
    $registry = Read-JsonFile -Path $registryPath -DefaultObject $default

    if (-not ($registry.PSObject.Properties.Name -contains 'Windows')) {
        $registry | Add-Member -MemberType NoteProperty -Name Windows -Value ([pscustomobject]@{})
    }

    $key = Get-WindowKey -StartET $Window.StartET -EndET $Window.EndET
    $existing = $null
    if ($registry.Windows.PSObject.Properties.Name -contains $key) {
        $existing = $registry.Windows.$key
    }

    if ($existing) {
        if ($ProvidedIncident -and $existing.IncidentNumber -and ($ProvidedIncident -ne $existing.IncidentNumber)) {
            throw "Window $key is already locked to incident $($existing.IncidentNumber). Do not use $ProvidedIncident for this locked window."
        }
        $existing.LastRunET = Format-ET (Get-NowET)
        Save-JsonFile -Object $registry -Path $registryPath
        return [pscustomobject]@{ Registry = $registry; RegistryPath = $registryPath; WindowKey = $key; Mapping = $existing; IsNew = $false }
    }

    $incident = $ProvidedIncident
    if (-not $incident) {
        Write-Host "New compute window detected: $((Format-ET $Window.StartET)) ET to $((Format-ET $Window.EndET)) ET" -ForegroundColor Yellow
        $incident = Read-Host "Enter incident number for this window"
    }
    if ([string]::IsNullOrWhiteSpace($incident)) { throw "Incident number is required for a new compute window." }

    # Find the most recent previous window for carry-forward reference.
    $previous = $null
    foreach ($prop in $registry.Windows.PSObject.Properties) {
        $w = $prop.Value
        try {
            $wEnd = [datetime]$w.WindowEndET
            if ($wEnd -le $Window.StartET) {
                if (-not $previous -or $wEnd -gt ([datetime]$previous.WindowEndET)) { $previous = $w }
            }
        } catch {}
    }

    $incidentFolder = Join-Path $Root (Sanitize-Name $incident)
    if (-not (Test-Path $incidentFolder)) { New-Item -ItemType Directory -Path $incidentFolder -Force | Out-Null }

    $mapping = [pscustomobject]@{
        IncidentNumber = $incident
        WindowStartET = Format-ET $Window.StartET
        WindowEndET = Format-ET $Window.EndET
        WindowLocked = $true
        WindowSource = $Window.Source
        FirstRunET = Format-ET (Get-NowET)
        LastRunET = Format-ET (Get-NowET)
        CarryForwardFromIncident = $(if ($previous) { $previous.IncidentNumber } else { "" })
        OutputFolder = $incidentFolder
    }

    $registry.Windows | Add-Member -MemberType NoteProperty -Name $key -Value $mapping
    Save-JsonFile -Object $registry -Path $registryPath

    return [pscustomobject]@{ Registry = $registry; RegistryPath = $registryPath; WindowKey = $key; Mapping = $mapping; IsNew = $true }
}

# -----------------------------
# Event collection and lifecycle
# -----------------------------
function New-EventRow {
    param(
        [string]$Incident,
        [datetime]$EventTime,
        [string]$ClusterId,
        [string]$Cluster,
        [string]$Environment,
        [string]$PgId,
        [string]$PgName,
        [string]$Host,
        [string]$ObjectName,
        [string]$ObjectType,
        [string]$ObjectId,
        [string]$ObjectKey,
        [string]$RunType,
        [string]$EventType,
        [string]$Message,
        [datetime]$RunStartET,
        [datetime]$RunEndET
    )

    return [pscustomobject]@{
        IncidentNumber = $Incident
        EventTimeET = Format-ET $EventTime
        ClusterId = $ClusterId
        Cluster = $Cluster
        Environment = $Environment
        ProtectionGroupId = $PgId
        ProtectionGroup = $PgName
        Host = $Host
        ObjectName = $ObjectName
        ObjectType = $ObjectType
        ObjectId = $ObjectId
        ObjectKey = $ObjectKey
        RunType = $RunType
        EventType = $EventType
        Message = $Message
        RunStartET = Format-ET $RunStartET
        RunEndET = Format-ET $RunEndET
    }
}

function Collect-WindowEvents {
    param(
        [hashtable]$BaseHeaders,
        [object]$Window,
        [string]$Incident
    )

    $events = @()
    $runEvidence = @()
    $warnings = @()

    $clusters = Get-Clusters -Headers $BaseHeaders
    foreach ($cluster in $clusters) {
        $clusterId = $cluster.ClusterId
        $clusterName = if ($cluster.ClusterName) { $cluster.ClusterName } else { $clusterId }
        $headers = @{}
        foreach ($k in $BaseHeaders.Keys) { $headers[$k] = $BaseHeaders[$k] }
        if ($clusterId) { $headers["accessClusterId"] = [string]$clusterId }

        try {
            $pgs = Get-ProtectionGroups -Headers $headers
        } catch {
            $warnings += "Cluster $clusterName protection group query failed: $($_.Exception.Message)"
            continue
        }

        foreach ($pg in $pgs) {
            $pgId = [string](Get-Prop $pg @('id','protectionGroupId','uid'))
            if (-not $pgId) { $pgId = [string](Get-Prop $pg @('name','protectionGroupName')) }
            $pgName = [string](Get-Prop $pg @('name','protectionGroupName'))
            if (-not $pgName) { $pgName = $pgId }
            $env = Environment-Name (Get-Prop $pg @('environment','env','protectionSourceEnvironment'))

            try {
                $runs = Get-ProtectionGroupRuns -Headers $headers -PgId $pgId
            } catch {
                $warnings += "Cluster $clusterName PG $pgName run query failed: $($_.Exception.Message)"
                continue
            }

            $oldestRunTime = $null
            foreach ($runRaw in $runs) {
                $run = $runRaw
                $innerRun = Get-Prop $runRaw @('run','protectionRun','backupRun')
                if ($innerRun) {
                    # keep wrapper fields available but prefer inner fields when present
                    $run = $runRaw
                }

                $runStart = Get-RunStartET $run
                $runEnd = Get-RunTimeET $run
                $eventTime = if ($runEnd) { $runEnd } else { $runStart }
                if (-not $eventTime) { continue }

                if (-not $oldestRunTime -or $eventTime -lt $oldestRunTime) { $oldestRunTime = $eventTime }

                # Window filter: include only runs/events inside the locked compute window.
                if ($eventTime -lt $Window.StartET -or $eventTime -ge $Window.EndET) { continue }

                $runStatus = Get-RunStatus $run
                $runType = Get-RunType $run
                $objects = Get-RunObjects $run

                $runEvidence += [pscustomobject]@{
                    IncidentNumber = $Incident
                    Cluster = $clusterName
                    Environment = $env
                    ProtectionGroup = $pgName
                    RunType = $runType
                    RunStatus = $runStatus
                    RunStartET = Format-ET $runStart
                    RunEndET = Format-ET $runEnd
                    ObjectDetailCount = $objects.Count
                    Message = Get-ErrorMessage $run
                }

                if ($objects.Count -eq 0) {
                    if ($runStatus -in @('Failed','Canceled','Running')) {
                        $key = "$clusterId|$env|$pgId|PG_LEVEL|$runType"
                        $eventType = switch ($runStatus) {
                            'Failed' { 'Failed' }
                            'Canceled' { 'CancelledRun' }
                            'Running' { 'RunningRun' }
                            default { $runStatus }
                        }
                        $events += New-EventRow -Incident $Incident -EventTime $eventTime -ClusterId $clusterId -Cluster $clusterName -Environment $env -PgId $pgId -PgName $pgName -Host "" -ObjectName $pgName -ObjectType "ProtectionGroup" -ObjectId "" -ObjectKey $key -RunType $runType -EventType $eventType -Message (Get-ErrorMessage $run) -RunStartET $runStart -RunEndET $runEnd
                    }
                    continue
                }

                foreach ($obj in $objects) {
                    $objStatus = Get-ObjectStatus $obj $runStatus
                    $objectName = Get-ObjectName $obj
                    $objectId = Get-ObjectId $obj
                    $objectType = Get-ObjectType $obj $env
                    $host = Get-HostName $obj
                    $key = Get-ObjectKey -ClusterId $clusterId -Environment $env -PgId $pgId -PgName $pgName -Obj $obj
                    $message = Get-ErrorMessage $obj
                    if (-not $message) { $message = Get-ErrorMessage $run }

                    $eventType = $null
                    if ($objStatus -eq 'Running' -or $runStatus -eq 'Running') { $eventType = 'RunningRun' }
                    elseif ($objStatus -eq 'Canceled' -or $runStatus -eq 'Canceled') { $eventType = 'CancelledRun' }
                    elseif (Is-FailureStatus $objStatus -or Is-FailureStatus $runStatus) { $eventType = 'Failed' }
                    elseif (Is-SuccessStatus $objStatus -or Is-SuccessStatus $runStatus) { $eventType = 'Succeeded' }

                    if ($eventType) {
                        $events += New-EventRow -Incident $Incident -EventTime $eventTime -ClusterId $clusterId -Cluster $clusterName -Environment $env -PgId $pgId -PgName $pgName -Host $host -ObjectName $objectName -ObjectType $objectType -ObjectId $objectId -ObjectKey $key -RunType $runType -EventType $eventType -Message $message -RunStartET $runStart -RunEndET $runEnd
                    }
                }
            }

            if ($oldestRunTime -and $oldestRunTime -gt $Window.StartET) {
                $warnings += "PG $pgName on $clusterName returned only $MaxRunsPerProtectionGroup runs; oldest returned run is after window start. Increase MaxRunsPerProtectionGroup if needed."
            }
        }
    }

    return [pscustomobject]@{
        Events = $events
        RunEvidence = $runEvidence
        Warnings = $warnings
    }
}

function Build-LifecycleTables {
    param(
        [object[]]$Events,
        [object]$PreviousState,
        [object]$Window,
        [string]$Incident,
        [object[]]$RunEvidence,
        [string[]]$Warnings
    )

    $previousFailingKeys = @{}
    if ($PreviousState -and ($PreviousState.PSObject.Properties.Name -contains 'Objects')) {
        foreach ($p in @($PreviousState.Objects)) {
            if ($p.CurrentStatus -eq 'StillFailing' -and $p.ObjectKey) { $previousFailingKeys[[string]$p.ObjectKey] = $p }
        }
    }

    $byKey = @{}
    foreach ($e in @($Events | Sort-Object EventTimeET)) {
        if (-not $byKey.ContainsKey($e.ObjectKey)) { $byKey[$e.ObjectKey] = @() }
        $byKey[$e.ObjectKey] += $e
    }

    $currentFailing = @()
    $recovered = @()
    $newFailures = @()
    $newRecoveries = @()
    $consecutive = @()
    $runningRows = @()
    $cancelledRows = @()
    $objectState = @()

    foreach ($key in $byKey.Keys) {
        $evs = @($byKey[$key] | Sort-Object EventTimeET)
        $failureEvents = @($evs | Where-Object { $_.EventType -eq 'Failed' })
        $successEvents = @($evs | Where-Object { $_.EventType -eq 'Succeeded' })
        $runningEvents = @($evs | Where-Object { $_.EventType -eq 'RunningRun' })
        $cancelledEvents = @($evs | Where-Object { $_.EventType -eq 'CancelledRun' })

        foreach ($r in $runningEvents) {
            $runningRows += Add-Section -Row $r -Section "Running Run" -Status "RunningAtLatestCheck" -ConsecutiveFailureCount 0 -RecoveredET ""
        }
        foreach ($c in $cancelledEvents) {
            $cancelledRows += Add-Section -Row $c -Section "Cancelled Run" -Status "CancelledInWindow" -ConsecutiveFailureCount 0 -RecoveredET ""
        }

        if ($failureEvents.Count -eq 0) { continue }

        $lastFailure = $failureEvents | Sort-Object EventTimeET | Select-Object -Last 1
        $laterSuccess = $successEvents | Where-Object { ([datetime]$_.EventTimeET) -gt ([datetime]$lastFailure.EventTimeET) } | Sort-Object EventTimeET | Select-Object -First 1

        # consecutive failures since last success before latest failure
        $successBeforeLastFailure = $successEvents | Where-Object { ([datetime]$_.EventTimeET) -lt ([datetime]$lastFailure.EventTimeET) } | Sort-Object EventTimeET | Select-Object -Last 1
        if ($successBeforeLastFailure) {
            $consecCount = @($failureEvents | Where-Object { ([datetime]$_.EventTimeET) -gt ([datetime]$successBeforeLastFailure.EventTimeET) }).Count
        } else {
            $consecCount = $failureEvents.Count
        }

        if ($laterSuccess) {
            $row = Add-Section -Row $lastFailure -Section "Recovered In Window" -Status "RecoveredInWindow" -ConsecutiveFailureCount $consecCount -RecoveredET $laterSuccess.EventTimeET
            $recovered += $row

            if ($previousFailingKeys.ContainsKey($key)) {
                $newRecoveries += Add-Section -Row $lastFailure -Section "New Recovery" -Status "NewlyRecoveredThisCheck" -ConsecutiveFailureCount $consecCount -RecoveredET $laterSuccess.EventTimeET
            }

            $objectState += New-StateObject -Row $lastFailure -CurrentStatus "RecoveredInWindow" -FirstFailedET ($failureEvents | Select-Object -First 1).EventTimeET -LastFailedET $lastFailure.EventTimeET -RecoveredET $laterSuccess.EventTimeET -ConsecutiveFailureCount $consecCount
        } else {
            $row = Add-Section -Row $lastFailure -Section "Current Still Failing" -Status "StillFailing" -ConsecutiveFailureCount $consecCount -RecoveredET ""
            $currentFailing += $row

            if (-not $previousFailingKeys.ContainsKey($key)) {
                $newFailures += Add-Section -Row $lastFailure -Section "New Failure" -Status "NewlyFailedThisCheck" -ConsecutiveFailureCount $consecCount -RecoveredET ""
            }

            if ($consecCount -gt 1) {
                $consecutive += Add-Section -Row $lastFailure -Section "Consecutive Failure" -Status "ConsecutiveFailure" -ConsecutiveFailureCount $consecCount -RecoveredET ""
            }

            $objectState += New-StateObject -Row $lastFailure -CurrentStatus "StillFailing" -FirstFailedET ($failureEvents | Select-Object -First 1).EventTimeET -LastFailedET $lastFailure.EventTimeET -RecoveredET "" -ConsecutiveFailureCount $consecCount
        }
    }

    $quickView = @()
    $quickView += $currentFailing
    $quickView += $recovered
    $quickView += $newFailures
    $quickView += $newRecoveries
    $quickView += $consecutive
    $quickView += $runningRows
    $quickView += $cancelledRows

    $clusters = @($Events | Where-Object { $_.Cluster } | Select-Object -ExpandProperty Cluster -Unique)
    $envs = @($Events | Where-Object { $_.Environment } | Select-Object -ExpandProperty Environment -Unique)
    $pgs = @($Events | Where-Object { $_.ProtectionGroup } | Select-Object -ExpandProperty ProtectionGroup -Unique)

    $summary = @(
        [pscustomobject]@{ Metric = "IncidentNumber"; Value = $Incident },
        [pscustomobject]@{ Metric = "WindowStartET"; Value = Format-ET $Window.StartET },
        [pscustomobject]@{ Metric = "WindowEndET"; Value = Format-ET $Window.EndET },
        [pscustomobject]@{ Metric = "GeneratedAtET"; Value = Format-ET (Get-NowET) },
        [pscustomobject]@{ Metric = "TotalUniqueObjectsFailedInWindow"; Value = $byKey.Keys.Count },
        [pscustomobject]@{ Metric = "RecoveredInWindow"; Value = $recovered.Count },
        [pscustomobject]@{ Metric = "StillFailingAtLatestCheck"; Value = $currentFailing.Count },
        [pscustomobject]@{ Metric = "NewFailuresSincePreviousRun"; Value = $newFailures.Count },
        [pscustomobject]@{ Metric = "NewRecoveriesSincePreviousRun"; Value = $newRecoveries.Count },
        [pscustomobject]@{ Metric = "ConsecutiveRepeatedFailures"; Value = $consecutive.Count },
        [pscustomobject]@{ Metric = "RunningRunsSeen"; Value = $runningRows.Count },
        [pscustomobject]@{ Metric = "CancelledRunsSeen"; Value = $cancelledRows.Count },
        [pscustomobject]@{ Metric = "ImpactedClusters"; Value = $clusters.Count },
        [pscustomobject]@{ Metric = "ImpactedEnvironments"; Value = ($envs -join "; ") },
        [pscustomobject]@{ Metric = "ImpactedProtectionGroups"; Value = $pgs.Count },
        [pscustomobject]@{ Metric = "WarningCount"; Value = $Warnings.Count }
    )

    return [pscustomobject]@{
        Summary = $summary
        CurrentFailing = $currentFailing
        Recovered = $recovered
        NewFailures = $newFailures
        NewRecoveries = $newRecoveries
        Consecutive = $consecutive
        CarryForward = $currentFailing
        EventHistory = @($Events | Sort-Object EventTimeET)
        RunEvidence = $RunEvidence
        QuickView = $quickView
        Running = $runningRows
        Cancelled = $cancelledRows
        ObjectState = $objectState
        Warnings = $Warnings
    }
}

function Add-Section {
    param($Row, [string]$Section, [string]$Status, [int]$ConsecutiveFailureCount, [string]$RecoveredET)
    return [pscustomobject]@{
        Section = $Section
        Status = $Status
        IncidentNumber = $Row.IncidentNumber
        Cluster = $Row.Cluster
        Environment = $Row.Environment
        ProtectionGroup = $Row.ProtectionGroup
        Host = $Row.Host
        ObjectName = $Row.ObjectName
        ObjectType = $Row.ObjectType
        RunType = $Row.RunType
        FirstFailedET = ""
        LastFailedET = $Row.EventTimeET
        RecoveredET = $RecoveredET
        ConsecutiveFailureCount = $ConsecutiveFailureCount
        Message = $Row.Message
        ObjectKey = $Row.ObjectKey
    }
}

function New-StateObject {
    param($Row, [string]$CurrentStatus, [string]$FirstFailedET, [string]$LastFailedET, [string]$RecoveredET, [int]$ConsecutiveFailureCount)
    return [pscustomobject]@{
        ObjectKey = $Row.ObjectKey
        Cluster = $Row.Cluster
        Environment = $Row.Environment
        ProtectionGroup = $Row.ProtectionGroup
        Host = $Row.Host
        ObjectName = $Row.ObjectName
        ObjectType = $Row.ObjectType
        RunType = $Row.RunType
        CurrentStatus = $CurrentStatus
        FirstFailedET = $FirstFailedET
        LastFailedET = $LastFailedET
        RecoveredET = $RecoveredET
        ConsecutiveFailureCount = $ConsecutiveFailureCount
        LastMessage = $Row.Message
    }
}

# -----------------------------
# Output helpers
# -----------------------------
function Export-XlsxWorkbook {
    param(
        [string]$Path,
        [hashtable]$Worksheets
    )

    $folder = Split-Path -Path $Path -Parent
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
    if (Test-Path $Path) { Remove-Item -Path $Path -Force }

    $exportExcel = Get-Command Export-Excel -ErrorAction SilentlyContinue
    if ($exportExcel) {
        $first = $true
        foreach ($name in $Worksheets.Keys) {
            $rows = @($Worksheets[$name])
            if ($rows.Count -eq 0) { $rows = @([pscustomobject]@{ Info = "No rows" }) }
            $safeName = Get-SafeWorksheetName $name
            $params = @{
                Path = $Path
                WorksheetName = $safeName
                AutoSize = $true
                FreezeTopRow = $true
                BoldTopRow = $true
                TableName = (Sanitize-Name $safeName)
            }
            if (-not $first) { $params.Append = $true }
            $rows | Export-Excel @params
            $first = $false
        }
        return
    }

    # Fallback to Excel COM if ImportExcel is unavailable.
    $excel = $null
    try {
        $excel = New-Object -ComObject Excel.Application
        $excel.Visible = $false
        $excel.DisplayAlerts = $false
        $wb = $excel.Workbooks.Add()

        # Remove extra sheets after creating required tabs.
        while ($wb.Worksheets.Count -gt 1) { $wb.Worksheets.Item(1).Delete() }

        $sheetIndex = 0
        foreach ($name in $Worksheets.Keys) {
            $sheetIndex++
            if ($sheetIndex -eq 1) { $ws = $wb.Worksheets.Item(1) }
            else { $ws = $wb.Worksheets.Add([Type]::Missing, $wb.Worksheets.Item($wb.Worksheets.Count)) }

            $ws.Name = Get-SafeWorksheetName $name
            $rows = @($Worksheets[$name])
            if ($rows.Count -eq 0) { $rows = @([pscustomobject]@{ Info = "No rows" }) }

            $headers = @($rows[0].PSObject.Properties.Name)
            for ($c = 0; $c -lt $headers.Count; $c++) {
                $cell = $ws.Cells.Item(1, $c + 1)
                $cell.Value2 = $headers[$c]
                $cell.Font.Bold = $true
            }
            for ($r = 0; $r -lt $rows.Count; $r++) {
                for ($c = 0; $c -lt $headers.Count; $c++) {
                    $v = $rows[$r].PSObject.Properties[$headers[$c]].Value
                    $ws.Cells.Item($r + 2, $c + 1).Value2 = [string]$v
                }
            }
            $ws.Columns.AutoFit() | Out-Null
        }
        $wb.SaveAs($Path, 51)
        $wb.Close($true)
    } catch {
        throw "XLSX export failed. Install ImportExcel module or run on a machine with Excel installed. Details: $($_.Exception.Message)"
    } finally {
        if ($excel) { $excel.Quit() | Out-Null }
    }
}

function Get-SafeWorksheetName([string]$Name) {
    $s = $Name -replace '[\\/\?\*\[\]:]', '_'
    if ($s.Length -gt 31) { $s = $s.Substring(0,31) }
    return $s
}

function New-WorkNotesText {
    param(
        [object]$Tables,
        [object]$Window,
        [string]$Incident,
        [string]$WorkbookName
    )

    $summaryHash = @{}
    foreach ($row in $Tables.Summary) { $summaryHash[$row.Metric] = $row.Value }

    $lines = @()
    $lines += "Backup Failure Window Summary"
    $lines += ""
    $lines += "Incident: $Incident"
    $lines += "Locked Compute Window: $(Format-ET $Window.StartET) ET to $(Format-ET $Window.EndET) ET"
    $lines += "Generated At: $(Format-ET (Get-NowET)) ET"
    $lines += "Source: Cohesity Helios API / PowerShell Window Consolidator"
    $lines += ""
    $lines += "Summary:"
    $lines += "- Total unique objects failed in this window: $($summaryHash['TotalUniqueObjectsFailedInWindow'])"
    $lines += "- Recovered within this window: $($summaryHash['RecoveredInWindow'])"
    $lines += "- Still failing at latest check within this window: $($summaryHash['StillFailingAtLatestCheck'])"
    $lines += "- New failures since previous check: $($summaryHash['NewFailuresSincePreviousRun'])"
    $lines += "- New recoveries since previous check: $($summaryHash['NewRecoveriesSincePreviousRun'])"
    $lines += "- Consecutive/repeated failures: $($summaryHash['ConsecutiveRepeatedFailures'])"
    $lines += "- Running backup runs seen: $($summaryHash['RunningRunsSeen'])"
    $lines += "- Cancelled backup runs seen: $($summaryHash['CancelledRunsSeen'])"
    $lines += "- Impacted clusters: $($summaryHash['ImpactedClusters'])"
    $lines += "- Impacted environments: $($summaryHash['ImpactedEnvironments'])"
    $lines += "- Impacted protection groups: $($summaryHash['ImpactedProtectionGroups'])"
    $lines += ""
    $lines += "Current Still Failing: See workbook tab 02_Current_Still_Failing"
    $lines += "Recovered During Window: See workbook tab 03_Recovered_In_Window"
    $lines += "Consecutive / Repeated Failures: See workbook tab 06_Consecutive_Failures"
    $lines += "Carry Forward Baseline: See workbook tab 07_Carry_Forward_Baseline"
    $lines += ""
    $lines += "Note: Running runs are listed separately and are not treated as failed or recovered until they complete."
    $lines += ""
    $lines += "Attachment: $WorkbookName"
    return ($lines -join [Environment]::NewLine)
}

function Show-OperatorOutput {
    param($Tables, $Window, $Incident, $WorkbookPath, $WorkNotesPath, $StatePath)

    $summaryHash = @{}
    foreach ($row in $Tables.Summary) { $summaryHash[$row.Metric] = $row.Value }

    Write-Host ""
    Write-Host "Incident: $Incident" -ForegroundColor Cyan
    Write-Host "Window  : $(Format-ET $Window.StartET) ET to $(Format-ET $Window.EndET) ET"
    Write-Host ""
    Write-Host "Summary:" -ForegroundColor Cyan
    Write-Host ("Total Failed In Window       : {0}" -f $summaryHash['TotalUniqueObjectsFailedInWindow'])
    Write-Host ("Recovered In Window          : {0}" -f $summaryHash['RecoveredInWindow'])
    Write-Host ("Still Failing Now            : {0}" -f $summaryHash['StillFailingAtLatestCheck'])
    Write-Host ("New Failures Since Last Run  : {0}" -f $summaryHash['NewFailuresSincePreviousRun'])
    Write-Host ("New Recoveries Since Last Run: {0}" -f $summaryHash['NewRecoveriesSincePreviousRun'])
    Write-Host ("Consecutive Failures         : {0}" -f $summaryHash['ConsecutiveRepeatedFailures'])
    Write-Host ("Running Runs Seen            : {0}" -f $summaryHash['RunningRunsSeen'])
    Write-Host ("Cancelled Runs Seen          : {0}" -f $summaryHash['CancelledRunsSeen'])
    Write-Host ""

    if ($ShowGridView -and (Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
        if ($MultipleGridViews) {
            $Tables.CurrentFailing | Out-GridView -Title "$Incident - Current Still Failing"
            $Tables.Recovered | Out-GridView -Title "$Incident - Recovered In Window"
            $Tables.Consecutive | Out-GridView -Title "$Incident - Consecutive Failures"
        } else {
            $Tables.QuickView | Out-GridView -Title "$Incident - Backup Failure Window Quick View"
        }
    } else {
        $Tables.QuickView | Select-Object Section,Cluster,Environment,ProtectionGroup,ObjectName,RunType,Status,LastFailedET,RecoveredET,ConsecutiveFailureCount | Format-Table -AutoSize
    }

    Write-Host "Files Created:" -ForegroundColor Cyan
    Write-Host $WorkbookPath
    Write-Host $WorkNotesPath
    Write-Host $StatePath
    Write-Host ""
    Write-Host "Next Step: Attach XLSX to incident and paste WorkNotes_Paste.txt into work_notes." -ForegroundColor Yellow
}

# -----------------------------
# Main
# -----------------------------
try {
    $window = Get-CurrentComputeWindow -DurationHours $WindowDurationHours -AnchorHour $AnchorHourET -OverrideStart $WindowStartET -OverrideEnd $WindowEndET
    $mappingInfo = Resolve-WindowMapping -Window $window -ProvidedIncident $IncidentNumber -Root $OutputRoot
    $mapping = $mappingInfo.Mapping
    $incident = $mapping.IncidentNumber
    $incidentFolder = $mapping.OutputFolder
    if (-not (Test-Path $incidentFolder)) { New-Item -ItemType Directory -Path $incidentFolder -Force | Out-Null }

    $statePath = Join-Path $incidentFolder ("{0}_State.json" -f (Sanitize-Name $incident))
    $previousState = Read-JsonFile -Path $statePath -DefaultObject $null

    $apiKey = Get-ApiKey -Path $ApiKeyPath
    $baseHeaders = @{ accept = "application/json"; apiKey = $apiKey }

    $collection = Collect-WindowEvents -BaseHeaders $baseHeaders -Window $window -Incident $incident
    $tables = Build-LifecycleTables -Events $collection.Events -PreviousState $previousState -Window $window -Incident $incident -RunEvidence $collection.RunEvidence -Warnings $collection.Warnings

    $runStatus = @(
        [pscustomobject]@{ Field = "ScriptResult"; Value = "Success" },
        [pscustomobject]@{ Field = "GeneratedAtET"; Value = Format-ET (Get-NowET) },
        [pscustomobject]@{ Field = "IncidentNumber"; Value = $incident },
        [pscustomobject]@{ Field = "WindowStartET"; Value = Format-ET $window.StartET },
        [pscustomobject]@{ Field = "WindowEndET"; Value = Format-ET $window.EndET },
        [pscustomobject]@{ Field = "WindowKey"; Value = $mappingInfo.WindowKey },
        [pscustomobject]@{ Field = "WindowLockStatus"; Value = "Locked" },
        [pscustomobject]@{ Field = "PreviousRunFound"; Value = [bool]$previousState },
        [pscustomobject]@{ Field = "WarningCount"; Value = $collection.Warnings.Count }
    )

    $metadata = @(
        [pscustomobject]@{ Field = "HeliosBaseUrl"; Value = $HeliosBaseUrl },
        [pscustomobject]@{ Field = "ApiKeyPath"; Value = $ApiKeyPath },
        [pscustomobject]@{ Field = "OutputRoot"; Value = $OutputRoot },
        [pscustomobject]@{ Field = "RegistryPath"; Value = $mappingInfo.RegistryPath },
        [pscustomobject]@{ Field = "MaxRunsPerProtectionGroup"; Value = $MaxRunsPerProtectionGroup },
        [pscustomobject]@{ Field = "WindowDurationHours"; Value = $WindowDurationHours },
        [pscustomobject]@{ Field = "AnchorHourET"; Value = $AnchorHourET }
    )

    $warningsTable = @($collection.Warnings | ForEach-Object { [pscustomobject]@{ Warning = $_ } })
    if ($warningsTable.Count -eq 0) { $warningsTable = @([pscustomobject]@{ Warning = "No warnings" }) }

    $workbookPath = Join-Path $incidentFolder ("{0}_BackupFailure_WindowSummary.xlsx" -f (Sanitize-Name $incident))
    $workNotesPath = Join-Path $incidentFolder ("{0}_WorkNotes_Paste.txt" -f (Sanitize-Name $incident))

    $worksheets = [ordered]@{
        "00_Run_Status" = $runStatus
        "01_Summary" = $tables.Summary
        "02_Current_Still_Failing" = $tables.CurrentFailing
        "03_Recovered_In_Window" = $tables.Recovered
        "04_New_Failures_Latest" = $tables.NewFailures
        "05_New_Recoveries_Latest" = $tables.NewRecoveries
        "06_Consecutive_Failures" = $tables.Consecutive
        "07_Carry_Forward_Baseline" = $tables.CarryForward
        "08_Event_History" = $tables.EventHistory
        "09_Run_Evidence" = $tables.RunEvidence
        "10_Metadata" = $metadata
        "11_Warnings" = $warningsTable
    }

    Export-XlsxWorkbook -Path $workbookPath -Worksheets $worksheets
    $workNotes = New-WorkNotesText -Tables $tables -Window $window -Incident $incident -WorkbookName (Split-Path $workbookPath -Leaf)
    $workNotes | Set-Content -Path $workNotesPath -Encoding UTF8

    $newState = [pscustomobject]@{
        IncidentNumber = $incident
        WindowKey = $mappingInfo.WindowKey
        WindowStartET = Format-ET $window.StartET
        WindowEndET = Format-ET $window.EndET
        LastRunET = Format-ET (Get-NowET)
        Objects = $tables.ObjectState
        Summary = $tables.Summary
        WorkNotesPath = $workNotesPath
        WorkbookPath = $workbookPath
    }
    Save-JsonFile -Object $newState -Path $statePath

    Show-OperatorOutput -Tables $tables -Window $window -Incident $incident -WorkbookPath $workbookPath -WorkNotesPath $workNotesPath -StatePath $statePath
}
catch {
    Write-Host ""
    Write-Host "SCRIPT RESULT: FAILED" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    throw
}
