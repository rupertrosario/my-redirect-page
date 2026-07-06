<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
GET-only Helios collector that locks one ServiceNow incident to one Dynatrace compute window.
The compute window matches compute_window.js: daily 18:00 ET to next-day 18:00 ET.
Outputs CSV, TXT, and JSON only. No Excel dependency.
#>

[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$ApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [int]$NumRuns = 60,
    [int]$MaxClusters = 0,
    [int]$MaxProtectionGroupsPerCluster = 0,
    [string]$ClusterName = "",
    [string]$IncidentNumber = "",
    [switch]$NoGridView,
    [switch]$VerboseMode
)

$ErrorActionPreference = "Stop"

function Get-EasternTimeZone {
    foreach ($id in @("America/New_York", "Eastern Standard Time")) {
        try { return [System.TimeZoneInfo]::FindSystemTimeZoneById($id) } catch { }
    }
    throw "Unable to resolve Eastern Time zone on this host."
}

$script:EasternTimeZone = Get-EasternTimeZone

function Write-TraceLine {
    param([string]$Message)
    if ($VerboseMode) { Write-Host $Message }
}

function Get-Value {
    param($Object, [string]$Name, $Default = $null)
    if ($null -eq $Object) { return $Default }
    $prop = $Object.PSObject.Properties[$Name]
    if ($null -eq $prop) { return $Default }
    if ($null -eq $prop.Value) { return $Default }
    return $prop.Value
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    if ($Value -is [System.Array]) { return @($Value) }
    return @($Value)
}

function Clean-Text {
    param($Value)
    if ($null -eq $Value) { return "" }
    if ($Value -is [System.Array]) { $Value = ($Value -join " | ") }
    $s = [string]$Value
    $s = $s.Replace("`r", " ").Replace("`n", " ").Replace('"', "'")
    while ($s.Contains("  ")) { $s = $s.Replace("  ", " ") }
    return $s.Trim()
}

function Convert-UsecsToEtString {
    param($Usecs)
    if (-not $Usecs) { return "" }
    $ms = [int64]([math]::Floor(([double]$Usecs) / 1000))
    $utc = [DateTimeOffset]::FromUnixTimeMilliseconds($ms).UtcDateTime
    $et = [System.TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EasternTimeZone)
    return $et.ToString("yyyy-MM-dd HH:mm:ss")
}

function Convert-EtToUsecs {
    param([datetime]$EtDateTime)
    $unspecified = [datetime]::SpecifyKind($EtDateTime, [DateTimeKind]::Unspecified)
    $utc = [System.TimeZoneInfo]::ConvertTimeToUtc($unspecified, $script:EasternTimeZone)
    $dto = [DateTimeOffset]::new($utc, [TimeSpan]::Zero)
    return [int64]($dto.ToUnixTimeMilliseconds() * 1000)
}

function Get-ComputeWindow {
    $nowUtc = (Get-Date).ToUniversalTime()
    $nowEt = [System.TimeZoneInfo]::ConvertTimeFromUtc($nowUtc, $script:EasternTimeZone)

    if ($nowEt.Hour -lt 18) {
        $startDate = $nowEt.Date.AddDays(-1)
    } else {
        $startDate = $nowEt.Date
    }

    $windowStartEt = $startDate.AddHours(18)
    $windowEndEt = $windowStartEt.AddDays(1)
    $startDateText = $windowStartEt.ToString("yyyy-MM-dd")
    $endDateText = $windowEndEt.ToString("yyyy-MM-dd")

    [pscustomobject][ordered]@{
        WindowKey = "$($startDateText)_1800ET"
        WindowLabel = "$startDateText 18:00 ET -> $endDateText 18:00 ET"
        WindowStartET = $windowStartEt.ToString("yyyy-MM-dd HH:mm:ss")
        WindowEndET = $windowEndEt.ToString("yyyy-MM-dd HH:mm:ss")
        WindowStartUsecs = Convert-EtToUsecs $windowStartEt
        WindowEndUsecs = Convert-EtToUsecs $windowEndEt
        GeneratedAtET = ([System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EasternTimeZone)).ToString("yyyy-MM-dd HH:mm:ss")
    }
}

function Read-JsonFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return $null }
    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    return $raw | ConvertFrom-Json
}

function Write-JsonFile {
    param($Object, [string]$Path)
    $dir = Split-Path -Parent $Path
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 80 | Set-Content -Path $Path -Encoding UTF8
}

function Get-Registry {
    param([string]$Path)
    $registry = Read-JsonFile -Path $Path
    if ($null -eq $registry) {
        $registry = [pscustomobject][ordered]@{
            TimeZone = "America/New_York"
            WindowMode = "DynatraceComputeWindow"
            WindowDuration = "Daily 18:00 ET to next-day 18:00 ET"
            WindowSource = "compute_window.js"
            Windows = [pscustomobject]@{}
        }
    }
    if ($null -eq $registry.PSObject.Properties["Windows"]) {
        $registry | Add-Member -MemberType NoteProperty -Name Windows -Value ([pscustomobject]@{}) -Force
    }
    return $registry
}

function Resolve-IncidentLock {
    param($Registry, $Window, [string]$RegistryPath, [string]$RequestedIncident, [string]$OutputRoot)

    $existing = $Registry.Windows.PSObject.Properties[$Window.WindowKey]
    if ($null -ne $existing) {
        $entry = $existing.Value
        if (-not [string]::IsNullOrWhiteSpace($RequestedIncident) -and $RequestedIncident -ne $entry.IncidentNumber) {
            Write-Warning "Window is already locked to $($entry.IncidentNumber). Ignoring supplied incident $RequestedIncident."
        }
        return $entry
    }

    $incident = $RequestedIncident
    if ([string]::IsNullOrWhiteSpace($incident)) {
        $incident = Read-Host "Enter incident number for this window"
    }
    $incident = $incident.Trim().ToUpper()
    if ($incident -notmatch '^INC[0-9A-Z]+$') {
        throw "Invalid incident number '$incident'. Expected format like INC1234567."
    }

    $previousWindow = $null
    $allWindows = @($Registry.Windows.PSObject.Properties | ForEach-Object { $_.Value })
    if ($allWindows.Count -gt 0) {
        $previousWindow = $allWindows | Where-Object { $_.WindowStartET -lt $Window.WindowStartET } | Sort-Object WindowStartET -Descending | Select-Object -First 1
    }

    $outputFolder = Join-Path $OutputRoot $incident
    $entry = [pscustomobject][ordered]@{
        IncidentNumber = $incident
        WindowKey = $Window.WindowKey
        WindowLabel = $Window.WindowLabel
        WindowStartET = $Window.WindowStartET
        WindowEndET = $Window.WindowEndET
        WindowLocked = $true
        WindowSource = "compute_window.js"
        FirstRunET = $Window.GeneratedAtET
        LastRunET = $Window.GeneratedAtET
        CarryForwardFromIncident = $(if ($previousWindow) { $previousWindow.IncidentNumber } else { "" })
        OutputFolder = $outputFolder
    }

    $Registry.Windows | Add-Member -MemberType NoteProperty -Name $Window.WindowKey -Value $entry -Force
    Write-JsonFile -Object $Registry -Path $RegistryPath
    return $entry
}

function New-QueryString {
    param([hashtable]$Params)
    $pairs = New-Object System.Collections.Generic.List[string]
    foreach ($key in $Params.Keys) {
        $val = $Params[$key]
        if ($null -eq $val) { continue }
        $pairs.Add(([uri]::EscapeDataString([string]$key) + "=" + [uri]::EscapeDataString([string]$val))) | Out-Null
    }
    return ($pairs -join "&")
}

function Invoke-CohesityGet {
    param([string]$Uri, [hashtable]$Headers)
    Write-TraceLine "GET $Uri"
    return Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers
}

function Get-ApiKey {
    param([string]$Path)
    if (-not (Test-Path $Path)) { throw "API key file not found: $Path" }
    $key = (Get-Content -Path $Path -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key file is empty: $Path" }
    return $key
}

function Get-EnvCode {
    param($ProtectionGroup)
    $env = Get-Value $ProtectionGroup "environment" ""
    if ([string]::IsNullOrWhiteSpace($env)) {
        $types = As-Array (Get-Value $ProtectionGroup "environmentTypes" @())
        if ($types.Count -gt 0) { $env = [string]$types[0] }
    }
    return $env
}

function Get-EnvLabel {
    param([string]$EnvCode)
    switch ($EnvCode) {
        "kOracle" { "Oracle"; break }
        "kSQL" { "SQL"; break }
        "kPhysical" { "Physical"; break }
        "kGenericNas" { "NAS"; break }
        "kIsilon" { "NAS"; break }
        "kHyperV" { "HyperV"; break }
        "kAcropolis" { "Acropolis"; break }
        "kRemoteAdapter" { "RemoteAdapter"; break }
        default { if ($EnvCode) { $EnvCode } else { "Unknown" } }
    }
}

function Test-IsFailedStatus { param([string]$Status) return ($Status -eq "Failed" -or $Status -eq "kFailed") }
function Test-IsSuccessStatus { param([string]$Status) return ($Status -eq "Succeeded" -or $Status -eq "SucceededWithWarning" -or $Status -eq "kSucceeded" -or $Status -eq "kSucceededWithWarning") }
function Test-IsRunningStatus { param([string]$Status) return ($Status -match "Running|Started|Accepted|InProgress") }
function Test-IsCancelledStatus { param([string]$Status) return ($Status -match "Cancel") }

function Get-EventKind {
    param([string]$Status)
    if (Test-IsFailedStatus $Status) { return "Failed" }
    if (Test-IsSuccessStatus $Status) { return "Success" }
    if (Test-IsRunningStatus $Status) { return "Running" }
    if (Test-IsCancelledStatus $Status) { return "Cancelled" }
    return "Unknown"
}

function Test-RelevantObject {
    param([string]$EnvCode, $Object)
    $objectType = Get-Value $Object "objectType" ""
    switch ($EnvCode) {
        "kOracle" { return ($objectType -eq "kDatabase" -or $objectType -eq "kHost") }
        "kSQL" { return ($objectType -eq "kDatabase" -or $objectType -eq "kHost") }
        "kPhysical" { return ($objectType -eq "kHost") }
        "kGenericNas" { return ($objectType -eq "kHost") }
        "kIsilon" { return ($objectType -eq "kHost") }
        "kHyperV" { return ($objectType -eq "kVirtualMachine") }
        "kAcropolis" { return ($objectType -eq "kVirtualMachine") }
        default { return $true }
    }
}

function Get-RemoteAdapterInfo {
    param($ProtectionGroup)
    $ra = Get-Value $ProtectionGroup "remoteAdapterParams" $null
    $hostName = ""
    $dbName = ""
    if ($null -ne $ra) {
        $hosts = Get-Value $ra "hosts" $null
        if ($null -eq $hosts) { $hosts = Get-Value $ra "host" $null }
        $first = $null
        $hostArray = As-Array $hosts
        if ($hostArray.Count -gt 0) { $first = $hostArray[0] }
        if ($null -ne $first) {
            $hostName = Get-Value $first "hostname" ""
            if ([string]::IsNullOrWhiteSpace($hostName)) { $hostName = Get-Value $first "hostName" "" }
            if ([string]::IsNullOrWhiteSpace($hostName)) { $hostName = Get-Value $first "name" "" }
            $scriptObj = Get-Value $first "incrementalBackupScript" $null
            if ($null -eq $scriptObj) { $scriptObj = Get-Value $first "backupScript" $null }
            $args = Get-Value $scriptObj "params" ""
            if ([string]::IsNullOrWhiteSpace($args)) { $args = Get-Value $scriptObj "arguments" "" }
            if ($args -is [System.Array]) { $args = ($args -join " ") }
            if ([string]$args -match '-o\s+(\S+)') { $dbName = $matches[1] }
        }
    }
    return [pscustomobject]@{ Host = $hostName; ObjectName = $(if ($dbName) { $dbName } else { $hostName }) }
}

function New-ObjectKey {
    param([string]$ClusterId, [string]$ClusterName, [string]$EnvCode, [string]$PgId, [string]$PgName, [string]$ObjectId, [string]$Host, [string]$ObjectName)
    if (-not [string]::IsNullOrWhiteSpace($ObjectId)) {
        return "$ClusterId|$EnvCode|$PgId|$ObjectId"
    }
    return "$ClusterName|$EnvCode|$PgName|$Host|$ObjectName"
}

function New-EventRow {
    param(
        $Window, [string]$Incident, [string]$ClusterId, [string]$Cluster, [string]$EnvCode, [string]$Environment,
        [string]$PgId, [string]$ProtectionGroup, [string]$Host, [string]$ObjectId, [string]$ObjectName,
        [string]$ObjectType, [string]$RunType, [string]$RunStatus, [int64]$StartUsecs, [int64]$EndUsecs,
        [string]$Message, [string]$RunId
    )

    $eventUsecs = $EndUsecs
    if (-not $eventUsecs -or $eventUsecs -le 0) { $eventUsecs = $StartUsecs }
    $objectKey = New-ObjectKey -ClusterId $ClusterId -ClusterName $Cluster -EnvCode $EnvCode -PgId $PgId -PgName $ProtectionGroup -ObjectId $ObjectId -Host $Host -ObjectName $ObjectName

    return [pscustomobject][ordered]@{
        IncidentNumber = $Incident
        WindowKey = $Window.WindowKey
        WindowLabel = $Window.WindowLabel
        ClusterId = $ClusterId
        Cluster = $Cluster
        EnvironmentCode = $EnvCode
        Environment = $Environment
        ProtectionGroupId = $PgId
        ProtectionGroup = $ProtectionGroup
        Host = $Host
        ObjectId = $ObjectId
        ObjectName = $ObjectName
        ObjectType = $ObjectType
        RunType = $RunType
        RunStatus = $RunStatus
        EventKind = Get-EventKind $RunStatus
        StartTimeET = Convert-UsecsToEtString $StartUsecs
        EndTimeET = Convert-UsecsToEtString $EndUsecs
        EventTimeUsecs = $eventUsecs
        EventTimeET = Convert-UsecsToEtString $eventUsecs
        Message = $Message
        ObjectKey = $objectKey
        RunId = $RunId
    }
}

function Get-FailedAttemptMessage {
    param($RunObject)
    $lsi = Get-Value $RunObject "localSnapshotInfo" $null
    $attempts = As-Array (Get-Value $lsi "failedAttempts" @())
    if ($attempts.Count -eq 0) { return "" }
    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($attempt in $attempts) {
        $msg = Clean-Text (Get-Value $attempt "message" "")
        if (-not [string]::IsNullOrWhiteSpace($msg)) { $parts.Add($msg) | Out-Null }
    }
    return ($parts -join " | ")
}

function Convert-RunToEvents {
    param($Window, [string]$Incident, $Cluster, $ProtectionGroup, $Run, $Info)

    $clusterId = [string](Get-Value $Cluster "clusterId" "")
    $clusterNameResolved = Get-Value $Cluster "name" ""
    if ([string]::IsNullOrWhiteSpace($clusterNameResolved)) { $clusterNameResolved = Get-Value $Cluster "clusterName" "" }
    if ([string]::IsNullOrWhiteSpace($clusterNameResolved)) { $clusterNameResolved = Get-Value $Cluster "displayName" "Unknown-$clusterId" }

    $pgId = [string](Get-Value $ProtectionGroup "id" "")
    $pgName = [string](Get-Value $ProtectionGroup "name" "Unknown PG")
    $envCode = Get-EnvCode $ProtectionGroup
    $envLabel = Get-EnvLabel $envCode

    $status = [string](Get-Value $Info "status" "Unknown")
    $runType = [string](Get-Value $Info "runType" "")
    $startUsecs = [int64](Get-Value $Info "startTimeUsecs" 0)
    $endUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
    $eventUsecs = $(if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs })

    if ($eventUsecs -lt $Window.WindowStartUsecs -or $eventUsecs -ge $Window.WindowEndUsecs) { return @() }

    $runId = [string](Get-Value $Run "id" "")
    if ([string]::IsNullOrWhiteSpace($runId)) { $runId = [string](Get-Value $Run "runId" "") }
    $runMsg = Clean-Text (Get-Value $Info "messages" "")

    if ($envCode -eq "kRemoteAdapter") {
        $ra = Get-RemoteAdapterInfo $ProtectionGroup
        return @(New-EventRow -Window $Window -Incident $Incident -ClusterId $clusterId -Cluster $clusterNameResolved -EnvCode $envCode -Environment $envLabel -PgId $pgId -ProtectionGroup $pgName -Host $ra.Host -ObjectId "" -ObjectName $ra.ObjectName -ObjectType "kRemoteAdapter" -RunType $runType -RunStatus $status -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $runMsg -RunId $runId)
    }

    $objects = As-Array (Get-Value $Run "objects" @())
    if ($objects.Count -eq 0) {
        return @(New-EventRow -Window $Window -Incident $Incident -ClusterId $clusterId -Cluster $clusterNameResolved -EnvCode $envCode -Environment $envLabel -PgId $pgId -ProtectionGroup $pgName -Host "" -ObjectId "" -ObjectName $pgName -ObjectType "ProtectionGroup" -RunType $runType -RunStatus $status -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $runMsg -RunId $runId)
    }

    $idToName = @{}
    foreach ($ro in $objects) {
        $obj = Get-Value $ro "object" $null
        $oid = [string](Get-Value $obj "id" "")
        $oname = [string](Get-Value $obj "name" "")
        if ($oid -and $oname) { $idToName[$oid] = $oname }
    }

    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($ro in $objects) {
        $obj = Get-Value $ro "object" $null
        if ($null -eq $obj) { continue }
        if (-not (Test-RelevantObject -EnvCode $envCode -Object $obj)) { continue }

        $objectType = [string](Get-Value $obj "objectType" "")
        $objectId = [string](Get-Value $obj "id" "")
        $objectName = [string](Get-Value $obj "name" "")
        $host = ""

        if (($envCode -eq "kOracle" -or $envCode -eq "kSQL") -and $objectType -eq "kHost") {
            $host = $objectName
            if (Test-IsFailedStatus $status) { $objectName = "No DBs Found (Host-Level Failure)" }
        } elseif (($envCode -eq "kOracle" -or $envCode -eq "kSQL") -and $objectType -eq "kDatabase") {
            $sourceId = [string](Get-Value $obj "sourceId" "")
            if ($sourceId -and $idToName.ContainsKey($sourceId)) { $host = $idToName[$sourceId] }
        }

        $msg = $runMsg
        if (Test-IsFailedStatus $status) {
            $failedMsg = Get-FailedAttemptMessage $ro
            if (-not [string]::IsNullOrWhiteSpace($failedMsg)) { $msg = $failedMsg }
        }

        $rows.Add((New-EventRow -Window $Window -Incident $Incident -ClusterId $clusterId -Cluster $clusterNameResolved -EnvCode $envCode -Environment $envLabel -PgId $pgId -ProtectionGroup $pgName -Host $host -ObjectId $objectId -ObjectName $objectName -ObjectType $objectType -RunType $runType -RunStatus $status -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -RunId $runId)) | Out-Null
    }

    if ($rows.Count -eq 0) {
        $rows.Add((New-EventRow -Window $Window -Incident $Incident -ClusterId $clusterId -Cluster $clusterNameResolved -EnvCode $envCode -Environment $envLabel -PgId $pgId -ProtectionGroup $pgName -Host "" -ObjectId "" -ObjectName $pgName -ObjectType "ProtectionGroup" -RunType $runType -RunStatus $status -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $runMsg -RunId $runId)) | Out-Null
    }

    return @($rows)
}

function New-LifecycleRow {
    param($Event, [string]$Section, [string]$Status, [string]$FirstFailedET, [string]$LastFailedET, [string]$RecoveredET, [int]$ConsecutiveFailureCount, [string]$CarryForwardStatus)
    return [pscustomobject][ordered]@{
        Section = $Section
        Status = $Status
        IncidentNumber = $Event.IncidentNumber
        WindowKey = $Event.WindowKey
        Cluster = $Event.Cluster
        Environment = $Event.Environment
        ProtectionGroup = $Event.ProtectionGroup
        Host = $Event.Host
        ObjectName = $Event.ObjectName
        ObjectType = $Event.ObjectType
        RunType = $Event.RunType
        FirstFailedET = $FirstFailedET
        LastFailedET = $LastFailedET
        RecoveredET = $RecoveredET
        ConsecutiveFailureCount = $ConsecutiveFailureCount
        CarryForwardStatus = $CarryForwardStatus
        Message = $Event.Message
        ObjectKey = $Event.ObjectKey
    }
}

function Export-ReportCsv {
    param([string]$Path, $Rows, [string[]]$Columns)
    $dir = Split-Path -Parent $Path
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $rowsArray = @($Rows)
    if ($rowsArray.Count -eq 0) {
        ($Columns -join ",") | Set-Content -Path $Path -Encoding UTF8
        return
    }
    $rowsArray | Select-Object $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
}

function Load-FailingKeysFromState {
    param($State)
    $map = @{}
    if ($null -eq $State) { return $map }
    foreach ($obj in (As-Array (Get-Value $State "Objects" @()))) {
        $status = [string](Get-Value $obj "CurrentStatus" "")
        $key = [string](Get-Value $obj "ObjectKey" "")
        if ($key -and ($status -eq "StillFailing" -or $status -eq "ReFailed" -or $status -eq "ConsecutiveFailure")) {
            $map[$key] = $obj
        }
    }
    return $map
}

$window = Get-ComputeWindow
$registryPath = Join-Path $OutputRoot "BackupFailure_WindowRegistry.json"
$registry = Get-Registry -Path $registryPath
$lock = Resolve-IncidentLock -Registry $registry -Window $window -RegistryPath $registryPath -RequestedIncident $IncidentNumber -OutputRoot $OutputRoot
$IncidentNumber = $lock.IncidentNumber
$outputFolder = $lock.OutputFolder
if (-not (Test-Path $outputFolder)) { New-Item -ItemType Directory -Path $outputFolder -Force | Out-Null }

$statePath = Join-Path $outputFolder ("{0}_State.json" -f $IncidentNumber)
$previousState = Read-JsonFile -Path $statePath
$previousFailing = Load-FailingKeysFromState -State $previousState
$previousStateExists = ($null -ne $previousState)

$carryForward = @{}
if (-not [string]::IsNullOrWhiteSpace($lock.CarryForwardFromIncident)) {
    foreach ($w in @($registry.Windows.PSObject.Properties | ForEach-Object { $_.Value })) {
        if ($w.IncidentNumber -eq $lock.CarryForwardFromIncident -and (Test-Path $w.OutputFolder)) {
            $priorStatePath = Join-Path $w.OutputFolder ("{0}_State.json" -f $w.IncidentNumber)
            $priorState = Read-JsonFile -Path $priorStatePath
            $carryForward = Load-FailingKeysFromState -State $priorState
            break
        }
    }
}

$apiKey = Get-ApiKey -Path $ApiKeyPath
$commonHeaders = @{ accept = "application/json"; apiKey = $apiKey }
$warnings = New-Object System.Collections.Generic.List[object]
$eventHistory = New-Object System.Collections.Generic.List[object]
$runEvidence = New-Object System.Collections.Generic.List[object]

try {
    $clusterData = Invoke-CohesityGet -Uri ("$BaseUrl/v2/mcm/cluster-mgmt/info") -Headers $commonHeaders
    $clusters = @($clusterData.cohesityClusters)
} catch {
    throw "Failed to collect Helios cluster list: $($_.Exception.Message)"
}

if (-not [string]::IsNullOrWhiteSpace($ClusterName)) {
    $clusters = @($clusters | Where-Object {
        (Get-Value $_ "name" "") -eq $ClusterName -or
        (Get-Value $_ "clusterName" "") -eq $ClusterName -or
        (Get-Value $_ "displayName" "") -eq $ClusterName
    })
}
if ($MaxClusters -gt 0) { $clusters = @($clusters | Select-Object -First $MaxClusters) }

foreach ($cluster in $clusters) {
    $clusterId = [string](Get-Value $cluster "clusterId" "")
    $clusterDisplay = Get-Value $cluster "name" ""
    if ([string]::IsNullOrWhiteSpace($clusterDisplay)) { $clusterDisplay = Get-Value $cluster "clusterName" "Unknown-$clusterId" }
    $headers = @{ accept = "application/json"; apiKey = $apiKey; accessClusterId = $clusterId }

    try {
        $pgQuery = New-QueryString @{ isDeleted = "false"; isPaused = "false"; isActive = "true" }
        $pgData = Invoke-CohesityGet -Uri ("$BaseUrl/v2/data-protect/protection-groups?$pgQuery") -Headers $headers
        $pgs = @($pgData.protectionGroups)
    } catch {
        $warnings.Add([pscustomobject]@{ Scope = "ProtectionGroups"; Cluster = $clusterDisplay; ProtectionGroup = ""; Warning = $_.Exception.Message }) | Out-Null
        continue
    }

    if ($MaxProtectionGroupsPerCluster -gt 0) { $pgs = @($pgs | Select-Object -First $MaxProtectionGroupsPerCluster) }

    foreach ($pg in $pgs) {
        $pgId = [string](Get-Value $pg "id" "")
        $pgName = [string](Get-Value $pg "name" "Unknown PG")
        try {
            $runQuery = New-QueryString @{ numRuns = $NumRuns; excludeNonRestorableRuns = "false"; includeObjectDetails = "true" }
            $runData = Invoke-CohesityGet -Uri ("$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgId))/runs?$runQuery") -Headers $headers
            $runs = @($runData.runs)
        } catch {
            $warnings.Add([pscustomobject]@{ Scope = "Runs"; Cluster = $clusterDisplay; ProtectionGroup = $pgName; Warning = $_.Exception.Message }) | Out-Null
            continue
        }

        foreach ($run in $runs) {
            foreach ($info in (As-Array (Get-Value $run "localBackupInfo" @()))) {
                $events = Convert-RunToEvents -Window $window -Incident $IncidentNumber -Cluster $cluster -ProtectionGroup $pg -Run $run -Info $info
                foreach ($ev in $events) { $eventHistory.Add($ev) | Out-Null }

                $status = [string](Get-Value $info "status" "Unknown")
                $startUsecs = [int64](Get-Value $info "startTimeUsecs" 0)
                $endUsecs = [int64](Get-Value $info "endTimeUsecs" 0)
                $eventUsecs = $(if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs })
                if ($eventUsecs -ge $window.WindowStartUsecs -and $eventUsecs -lt $window.WindowEndUsecs) {
                    $runEvidence.Add([pscustomobject][ordered]@{
                        IncidentNumber = $IncidentNumber
                        WindowKey = $window.WindowKey
                        Cluster = $clusterDisplay
                        ProtectionGroup = $pgName
                        Environment = Get-EnvLabel (Get-EnvCode $pg)
                        RunType = [string](Get-Value $info "runType" "")
                        Status = $status
                        StartTimeET = Convert-UsecsToEtString $startUsecs
                        EndTimeET = Convert-UsecsToEtString $endUsecs
                        Message = Clean-Text (Get-Value $info "messages" "")
                    }) | Out-Null
                }
            }
        }
    }
}

$eventRows = @($eventHistory | Sort-Object Cluster, ProtectionGroup, ObjectName, EventTimeUsecs)
$currentStillFailing = New-Object System.Collections.Generic.List[object]
$recoveredInWindow = New-Object System.Collections.Generic.List[object]
$consecutiveFailures = New-Object System.Collections.Generic.List[object]
$objectState = New-Object System.Collections.Generic.List[object]

foreach ($group in ($eventRows | Group-Object ObjectKey)) {
    $events = @($group.Group | Sort-Object EventTimeUsecs)
    $failed = @($events | Where-Object { $_.EventKind -eq "Failed" })
    $success = @($events | Where-Object { $_.EventKind -eq "Success" })
    if ($failed.Count -eq 0 -and $success.Count -eq 0) { continue }

    $carryStatus = $(if ($carryForward.ContainsKey($group.Name)) { "CarriedForward" } else { "NewInThisWindow" })

    if ($failed.Count -eq 0 -and $success.Count -gt 0 -and $carryForward.ContainsKey($group.Name)) {
        $recovery = $success | Sort-Object EventTimeUsecs | Select-Object -Last 1
        $row = New-LifecycleRow -Event $recovery -Section "Recovered In Window" -Status "CarriedForwardRecovered" -FirstFailedET "" -LastFailedET "" -RecoveredET $recovery.EventTimeET -ConsecutiveFailureCount 0 -CarryForwardStatus "CarriedForwardRecovered"
        $recoveredInWindow.Add($row) | Out-Null
        $objectState.Add([pscustomobject][ordered]@{ ObjectKey = $group.Name; CurrentStatus = "CarriedForwardRecovered"; Cluster = $recovery.Cluster; Environment = $recovery.Environment; ProtectionGroup = $recovery.ProtectionGroup; Host = $recovery.Host; ObjectName = $recovery.ObjectName; LastFailedET = ""; RecoveredET = $recovery.EventTimeET }) | Out-Null
        continue
    }

    if ($failed.Count -eq 0) { continue }

    $firstFailure = $failed | Sort-Object EventTimeUsecs | Select-Object -First 1
    $lastFailure = $failed | Sort-Object EventTimeUsecs | Select-Object -Last 1
    $laterSuccess = @($success | Where-Object { $_.EventTimeUsecs -gt $lastFailure.EventTimeUsecs } | Sort-Object EventTimeUsecs | Select-Object -Last 1)

    $countSinceLastSuccess = 0
    foreach ($ev in $events) {
        if ($ev.EventKind -eq "Success") { $countSinceLastSuccess = 0 }
        elseif ($ev.EventKind -eq "Failed") { $countSinceLastSuccess++ }
    }

    if ($laterSuccess.Count -gt 0) {
        $recovery = $laterSuccess[0]
        $row = New-LifecycleRow -Event $lastFailure -Section "Recovered In Window" -Status "RecoveredInWindow" -FirstFailedET $firstFailure.EventTimeET -LastFailedET $lastFailure.EventTimeET -RecoveredET $recovery.EventTimeET -ConsecutiveFailureCount 0 -CarryForwardStatus $(if ($carryForward.ContainsKey($group.Name)) { "CarriedForwardRecovered" } else { $carryStatus })
        $recoveredInWindow.Add($row) | Out-Null
        $objectState.Add([pscustomobject][ordered]@{ ObjectKey = $group.Name; CurrentStatus = "RecoveredInWindow"; Cluster = $lastFailure.Cluster; Environment = $lastFailure.Environment; ProtectionGroup = $lastFailure.ProtectionGroup; Host = $lastFailure.Host; ObjectName = $lastFailure.ObjectName; LastFailedET = $lastFailure.EventTimeET; RecoveredET = $recovery.EventTimeET }) | Out-Null
    } else {
        $hadEarlierSuccess = @($success | Where-Object { $_.EventTimeUsecs -lt $lastFailure.EventTimeUsecs }).Count -gt 0
        $status = $(if ($hadEarlierSuccess) { "ReFailed" } elseif ($countSinceLastSuccess -gt 1) { "ConsecutiveFailure" } else { "StillFailing" })
        $cfStatus = $(if ($carryForward.ContainsKey($group.Name)) { "CarriedForwardAndStillFailing" } else { $carryStatus })
        $row = New-LifecycleRow -Event $lastFailure -Section "Current Still Failing" -Status $status -FirstFailedET $firstFailure.EventTimeET -LastFailedET $lastFailure.EventTimeET -RecoveredET "" -ConsecutiveFailureCount $countSinceLastSuccess -CarryForwardStatus $cfStatus
        $currentStillFailing.Add($row) | Out-Null
        if ($countSinceLastSuccess -gt 1) {
            $consecutiveFailures.Add((New-LifecycleRow -Event $lastFailure -Section "Consecutive Failure" -Status "ConsecutiveFailure" -FirstFailedET $firstFailure.EventTimeET -LastFailedET $lastFailure.EventTimeET -RecoveredET "" -ConsecutiveFailureCount $countSinceLastSuccess -CarryForwardStatus $cfStatus)) | Out-Null
        }
        $objectState.Add([pscustomobject][ordered]@{ ObjectKey = $group.Name; CurrentStatus = $status; Cluster = $lastFailure.Cluster; Environment = $lastFailure.Environment; ProtectionGroup = $lastFailure.ProtectionGroup; Host = $lastFailure.Host; ObjectName = $lastFailure.ObjectName; LastFailedET = $lastFailure.EventTimeET; RecoveredET = "" }) | Out-Null
    }
}

$currentFailingKeys = @{}
foreach ($row in $currentStillFailing) { $currentFailingKeys[$row.ObjectKey] = $row }

$newFailures = New-Object System.Collections.Generic.List[object]
foreach ($row in $currentStillFailing) {
    if (-not $previousStateExists -or -not $previousFailing.ContainsKey($row.ObjectKey)) {
        $newFailures.Add((New-LifecycleRow -Event $row -Section "New Failure" -Status "NewlyFailedThisCheck" -FirstFailedET $row.FirstFailedET -LastFailedET $row.LastFailedET -RecoveredET "" -ConsecutiveFailureCount $row.ConsecutiveFailureCount -CarryForwardStatus $row.CarryForwardStatus)) | Out-Null
    }
}

$newRecoveries = New-Object System.Collections.Generic.List[object]
foreach ($row in $recoveredInWindow) {
    if ($previousFailing.ContainsKey($row.ObjectKey) -or $row.CarryForwardStatus -eq "CarriedForwardRecovered") {
        $newRecoveries.Add((New-LifecycleRow -Event $row -Section "New Recovery" -Status "NewlyRecoveredThisCheck" -FirstFailedET $row.FirstFailedET -LastFailedET $row.LastFailedET -RecoveredET $row.RecoveredET -ConsecutiveFailureCount 0 -CarryForwardStatus $row.CarryForwardStatus)) | Out-Null
    }
}

$runningRows = @($eventRows | Where-Object { $_.EventKind -eq "Running" } | ForEach-Object { New-LifecycleRow -Event $_ -Section "Running Run" -Status "RunningAtLatestCheck" -FirstFailedET "" -LastFailedET "" -RecoveredET "" -ConsecutiveFailureCount 0 -CarryForwardStatus "" })
$cancelledRows = @($eventRows | Where-Object { $_.EventKind -eq "Cancelled" } | ForEach-Object { New-LifecycleRow -Event $_ -Section "Cancelled Run" -Status "CancelledInWindow" -FirstFailedET "" -LastFailedET $_.EventTimeET -RecoveredET "" -ConsecutiveFailureCount 0 -CarryForwardStatus "" })

$summary = [pscustomobject][ordered]@{
    IncidentNumber = $IncidentNumber
    WindowKey = $window.WindowKey
    WindowLabel = $window.WindowLabel
    WindowStartET = $window.WindowStartET
    WindowEndET = $window.WindowEndET
    GeneratedAtET = $window.GeneratedAtET
    TotalFailedInWindow = @($eventRows | Where-Object { $_.EventKind -eq "Failed" } | Select-Object -ExpandProperty ObjectKey -Unique).Count
    RecoveredInWindow = @($recoveredInWindow).Count
    StillFailingNow = @($currentStillFailing).Count
    NewFailuresSinceLastRun = @($newFailures).Count
    NewRecoveriesSinceLastRun = @($newRecoveries).Count
    ConsecutiveFailures = @($consecutiveFailures).Count
    RunningRunsSeen = @($runningRows).Count
    CancelledRunsSeen = @($cancelledRows).Count
    ImpactedClusters = @($eventRows | Where-Object { $_.EventKind -eq "Failed" } | Select-Object -ExpandProperty Cluster -Unique).Count
    ImpactedEnvironments = ((@($eventRows | Where-Object { $_.EventKind -eq "Failed" } | Select-Object -ExpandProperty Environment -Unique) | Sort-Object) -join ", ")
    ImpactedProtectionGroups = @($eventRows | Where-Object { $_.EventKind -eq "Failed" } | Select-Object -ExpandProperty ProtectionGroup -Unique).Count
}

$base = Join-Path $outputFolder $IncidentNumber
$lifecycleColumns = @("Section","Status","IncidentNumber","WindowKey","Cluster","Environment","ProtectionGroup","Host","ObjectName","ObjectType","RunType","FirstFailedET","LastFailedET","RecoveredET","ConsecutiveFailureCount","CarryForwardStatus","Message","ObjectKey")
$eventColumns = @("IncidentNumber","WindowKey","WindowLabel","Cluster","Environment","ProtectionGroup","Host","ObjectName","ObjectType","RunType","RunStatus","EventKind","StartTimeET","EndTimeET","EventTimeET","Message","ObjectKey","RunId")
$summaryColumns = @("IncidentNumber","WindowKey","WindowLabel","WindowStartET","WindowEndET","GeneratedAtET","TotalFailedInWindow","RecoveredInWindow","StillFailingNow","NewFailuresSinceLastRun","NewRecoveriesSinceLastRun","ConsecutiveFailures","RunningRunsSeen","CancelledRunsSeen","ImpactedClusters","ImpactedEnvironments","ImpactedProtectionGroups")
$warningColumns = @("Scope","Cluster","ProtectionGroup","Warning")

Export-ReportCsv -Path "${base}_00_Run_Status.csv" -Rows @([pscustomobject]@{ Status = "ScriptSucceeded"; IncidentNumber = $IncidentNumber; WindowKey = $window.WindowKey; WindowLabel = $window.WindowLabel; GeneratedAtET = $window.GeneratedAtET; Note = "Script success means data was collected/consolidated. It does not mean backups succeeded." }) -Columns @("Status","IncidentNumber","WindowKey","WindowLabel","GeneratedAtET","Note")
Export-ReportCsv -Path "${base}_01_Summary.csv" -Rows @($summary) -Columns $summaryColumns
Export-ReportCsv -Path "${base}_02_Current_Still_Failing.csv" -Rows @($currentStillFailing) -Columns $lifecycleColumns
Export-ReportCsv -Path "${base}_03_Recovered_In_Window.csv" -Rows @($recoveredInWindow) -Columns $lifecycleColumns
Export-ReportCsv -Path "${base}_04_New_Failures_Latest.csv" -Rows @($newFailures) -Columns $lifecycleColumns
Export-ReportCsv -Path "${base}_05_New_Recoveries_Latest.csv" -Rows @($newRecoveries) -Columns $lifecycleColumns
Export-ReportCsv -Path "${base}_06_Consecutive_Failures.csv" -Rows @($consecutiveFailures) -Columns $lifecycleColumns
Export-ReportCsv -Path "${base}_07_Carry_Forward_Baseline.csv" -Rows @($currentStillFailing) -Columns $lifecycleColumns
Export-ReportCsv -Path "${base}_08_Event_History.csv" -Rows @($eventRows) -Columns $eventColumns
Export-ReportCsv -Path "${base}_09_Run_Evidence.csv" -Rows @($runEvidence) -Columns @("IncidentNumber","WindowKey","Cluster","ProtectionGroup","Environment","RunType","Status","StartTimeET","EndTimeET","Message")
Export-ReportCsv -Path "${base}_10_Metadata.csv" -Rows @([pscustomobject]@{ BaseUrl = $BaseUrl; ApiKeyPath = $ApiKeyPath; OutputRoot = $OutputRoot; TimeZone = "America/New_York"; WindowSource = "compute_window.js"; NumRuns = $NumRuns; MaxClusters = $MaxClusters; MaxProtectionGroupsPerCluster = $MaxProtectionGroupsPerCluster; ClusterName = $ClusterName; GetOnly = $true; ExcelUsed = $false }) -Columns @("BaseUrl","ApiKeyPath","OutputRoot","TimeZone","WindowSource","NumRuns","MaxClusters","MaxProtectionGroupsPerCluster","ClusterName","GetOnly","ExcelUsed")
Export-ReportCsv -Path "${base}_11_Warnings.csv" -Rows @($warnings) -Columns $warningColumns

$quickView = @($currentStillFailing) + @($recoveredInWindow) + @($newFailures) + @($newRecoveries) + @($consecutiveFailures) + @($runningRows) + @($cancelledRows)
Export-ReportCsv -Path "${base}_QuickView.csv" -Rows $quickView -Columns $lifecycleColumns

$workNotesPath = "${base}_WorkNotes_Paste.txt"
$workNotes = @"
Backup Failure Window Summary

Incident: $IncidentNumber
Locked Compute Window: $($window.WindowLabel)
Generated At: $($window.GeneratedAtET) ET
Source: Cohesity Helios API / PowerShell Window Consolidator
Window Source: compute_window.js

Summary:
- Total unique objects failed in this window: $($summary.TotalFailedInWindow)
- Recovered within this window: $($summary.RecoveredInWindow)
- Still failing at latest check within this window: $($summary.StillFailingNow)
- New failures since previous check: $($summary.NewFailuresSinceLastRun)
- New recoveries since previous check: $($summary.NewRecoveriesSinceLastRun)
- Consecutive/repeated failures: $($summary.ConsecutiveFailures)
- Running backup runs seen: $($summary.RunningRunsSeen)
- Cancelled backup runs seen: $($summary.CancelledRunsSeen)
- Impacted clusters: $($summary.ImpactedClusters)
- Impacted environments: $($summary.ImpactedEnvironments)
- Impacted protection groups: $($summary.ImpactedProtectionGroups)

Current Still Failing:
See CSV: $($IncidentNumber)_02_Current_Still_Failing.csv

Recovered During Window:
See CSV: $($IncidentNumber)_03_Recovered_In_Window.csv

New Failures / Recoveries Since Previous Check:
See CSV: $($IncidentNumber)_04_New_Failures_Latest.csv
See CSV: $($IncidentNumber)_05_New_Recoveries_Latest.csv

Consecutive / Repeated Failures:
See CSV: $($IncidentNumber)_06_Consecutive_Failures.csv

Carry Forward Baseline:
See CSV: $($IncidentNumber)_07_Carry_Forward_Baseline.csv

Note:
Running runs are listed separately and are not treated as failed or recovered until they complete.
Script success means the script collected and consolidated the window correctly; it does not mean backups succeeded.

Attachments:
Attach the generated CSV/TXT/JSON files from:
$outputFolder
"@
$workNotes | Set-Content -Path $workNotesPath -Encoding UTF8

$state = [pscustomobject][ordered]@{
    IncidentNumber = $IncidentNumber
    WindowKey = $window.WindowKey
    WindowLabel = $window.WindowLabel
    WindowStartET = $window.WindowStartET
    WindowEndET = $window.WindowEndET
    LastRunET = $window.GeneratedAtET
    SnowSysId = ""
    SnowWorkNotesReadEnabled = $false
    WindowSource = "compute_window.js"
    WindowLocked = $true
    CarryForwardFromIncident = $lock.CarryForwardFromIncident
    OutputFolder = $outputFolder
    Objects = @($objectState)
}
Write-JsonFile -Object $state -Path $statePath

$registry = Get-Registry -Path $registryPath
$currentEntry = $registry.Windows.PSObject.Properties[$window.WindowKey].Value
$currentEntry.LastRunET = $window.GeneratedAtET
$currentEntry.OutputFolder = $outputFolder
Write-JsonFile -Object $registry -Path $registryPath

if (-not $NoGridView) {
    $ogv = Get-Command Out-GridView -ErrorAction SilentlyContinue
    if ($null -ne $ogv -and $quickView.Count -gt 0) {
        $quickView | Out-GridView -Title "$IncidentNumber - Backup Failure Window Quick View"
    }
}

Write-Host ""
Write-Host "Incident: $IncidentNumber"
Write-Host "Window  : $($window.WindowLabel)"
Write-Host ""
Write-Host "Summary:"
Write-Host ("Total Failed In Window       : {0}" -f $summary.TotalFailedInWindow)
Write-Host ("Recovered In Window          : {0}" -f $summary.RecoveredInWindow)
Write-Host ("Still Failing Now            : {0}" -f $summary.StillFailingNow)
Write-Host ("New Failures Since Last Run  : {0}" -f $summary.NewFailuresSinceLastRun)
Write-Host ("New Recoveries Since Last Run: {0}" -f $summary.NewRecoveriesSinceLastRun)
Write-Host ("Consecutive Failures         : {0}" -f $summary.ConsecutiveFailures)
Write-Host ("Running Runs Seen            : {0}" -f $summary.RunningRunsSeen)
Write-Host ("Cancelled Runs Seen          : {0}" -f $summary.CancelledRunsSeen)
Write-Host ""
Write-Host "Files Created:"
Write-Host "${base}_01_Summary.csv"
Write-Host "${base}_02_Current_Still_Failing.csv"
Write-Host "${base}_03_Recovered_In_Window.csv"
Write-Host "${base}_04_New_Failures_Latest.csv"
Write-Host "${base}_05_New_Recoveries_Latest.csv"
Write-Host "${base}_06_Consecutive_Failures.csv"
Write-Host "${base}_07_Carry_Forward_Baseline.csv"
Write-Host "${base}_08_Event_History.csv"
Write-Host "${base}_09_Run_Evidence.csv"
Write-Host $workNotesPath
Write-Host $statePath
Write-Host ""
Write-Host "Next Step: Attach CSV/TXT/JSON evidence and paste WorkNotes_Paste.txt into work_notes."
