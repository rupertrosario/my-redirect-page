param(
    [int]$MaxRunsPerProtectionGroup = 20,
    [switch]$NoCsv
)

$ErrorActionPreference = "Stop"

$root = "X:\PowerShell\Cohesity_API_Scripts"
$keyCheckPath = Join-Path $root "DO_NOT_Delete\apikey.txt"
$helperPath = Join-Path $root "Common\ApiKeyAesHelper.ps1"
$encryptedFile = Join-Path $root "Common\Secure\cohesity_apikey.enc"
$outputDir = "X:\PowerShell\Data\Cohesity\BackupFailures"
$baseUrl = "https://helios.cohesity.com"

if (-not (Test-Path $keyCheckPath)) { throw "Required key check file not found at $keyCheckPath" }
if (-not (Test-Path $helperPath)) { throw "Required helper file not found at $helperPath" }
if (-not (Test-Path $encryptedFile)) { throw "Required encrypted key file not found at $encryptedFile" }

. $helperPath
$cohesityToken = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedFile

$commonHeaders = @{
    "apiKey" = $cohesityToken
    "accept" = "application/json"
}

function Invoke-GetJson {
    param([string]$Uri, [hashtable]$Headers)

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method GET -Uri $Uri -Headers $Headers -UseBasicParsing
    }
    else {
        $r = Invoke-WebRequest -Method GET -Uri $Uri -Headers $Headers
    }

    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function Convert-UsecsToUtc {
    param($Usecs)

    if ($null -eq $Usecs -or $Usecs -eq 0) { return $null }

    try {
        return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000)).UtcDateTime
    }
    catch {
        return $null
    }
}

function Convert-UtcToEtText {
    param([datetime]$Utc)

    if ($null -eq $Utc) { return "" }

    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    return ([System.TimeZoneInfo]::ConvertTimeFromUtc($Utc, $tz)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Get-Window {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    $nowEt = [System.TimeZoneInfo]::ConvertTimeFromUtc([datetime]::UtcNow, $tz)

    if ($nowEt.Hour -lt 18) {
        $startEt = $nowEt.Date.AddDays(-1).AddHours(18)
    }
    else {
        $startEt = $nowEt.Date.AddHours(18)
    }

    $endEt = $startEt.AddDays(1)

    return [pscustomobject]@{
        Key      = $startEt.ToString("yyyy-MM-dd_1800ET")
        Label    = "{0} ET -> {1} ET" -f $startEt.ToString("yyyy-MM-dd HH:mm"), $endEt.ToString("yyyy-MM-dd HH:mm")
        StartUtc = [System.TimeZoneInfo]::ConvertTimeToUtc($startEt, $tz)
        EndUtc   = [System.TimeZoneInfo]::ConvertTimeToUtc($endEt, $tz)
    }
}

function CleanText {
    param($Value)

    if ($null -eq $Value) { return "" }

    if ($Value -is [System.Array]) {
        $Value = ($Value | ForEach-Object { "$_" }) -join " | "
    }

    return (([string]$Value -replace "[`r`n]+", " ") -replace "\s+", " ").Trim()
}

function Get-FirstInfo {
    param($Run)

    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }

    $arr = @($Run.localBackupInfo)
    if ($arr.Count -eq 0) { return $null }

    return $arr[0]
}

function Get-ClusterName {
    param($Cluster)

    if ($Cluster.clusterName) { return [string]$Cluster.clusterName }
    if ($Cluster.name) { return [string]$Cluster.name }
    if ($Cluster.displayName) { return [string]$Cluster.displayName }
    return "Unknown-$($Cluster.clusterId)"
}

function Get-PgEnvironment {
    param($Pg)

    if ($Pg.environment) { return [string]$Pg.environment }
    if ($Pg.environmentTypes) {
        $envs = @($Pg.environmentTypes)
        if ($envs.Count -gt 0) { return [string]$envs[0] }
    }

    return "Unknown"
}

function Get-ObjectKey {
    param($ObjectWrapper)

    if ($null -eq $ObjectWrapper -or $null -eq $ObjectWrapper.object) { return "" }

    $obj = $ObjectWrapper.object

    if ($null -ne $obj.id -and "" -ne [string]$obj.id) {
        return [string]$obj.id
    }

    $sourceId = ""
    if ($null -ne $obj.sourceId) { $sourceId = [string]$obj.sourceId }

    return "{0}|{1}|{2}|{3}" -f $obj.environment, $obj.objectType, $obj.name, $sourceId
}

function Get-TargetRole {
    param($Environment, $Object)

    if ($null -eq $Object) { return "" }

    $type = [string]$Object.objectType
    $objEnv = [string]$Object.environment

    switch ($Environment) {
        "kOracle" {
            if ($type -eq "kDatabase") { return "Database" }
            if ($type -eq "kHost" -or $objEnv -eq "kPhysical") { return "HostLevel" }
        }
        "kSQL" {
            if ($type -eq "kDatabase") { return "Database" }
            if ($type -eq "kHost" -or $objEnv -eq "kPhysical") { return "HostLevel" }
        }
        "kPhysical" {
            if ($type -eq "kHost") { return "Object" }
        }
        "kGenericNas" {
            if ($type -eq "kHost") { return "Object" }
        }
        "kIsilon" {
            if ($type -eq "kHost") { return "Object" }
        }
        "kHyperV" {
            if ($type -eq "kVirtualMachine") { return "Object" }
        }
        "kAcropolis" {
            if ($type -eq "kVirtualMachine") { return "Object" }
        }
        default {
            if (-not [string]::IsNullOrWhiteSpace($type)) { return "Object" }
        }
    }

    return ""
}

function Find-HostNameInObjects {
    param($Objects, $SourceId)

    if ($null -eq $SourceId) { return "" }

    $sid = [string]$SourceId

    foreach ($item in @($Objects)) {
        if ($null -eq $item.object) { continue }
        if ($null -eq $item.object.id) { continue }

        if ([string]$item.object.id -eq $sid) {
            return [string]$item.object.name
        }
    }

    return ""
}

function Get-FailedMessage {
    param($Attempts)

    $messages = @()

    foreach ($a in @($Attempts)) {
        if ($null -eq $a) { continue }
        $msg = CleanText $a.message
        if (-not [string]::IsNullOrWhiteSpace($msg)) {
            $messages += $msg
        }
    }

    return ($messages -join " | ")
}

function New-OutputRow {
    param(
        $Window,
        [string]$ClusterName,
        [string]$ClusterId,
        [string]$Environment,
        $ProtectionGroup,
        [string]$RunType,
        [string]$RunStatus,
        [string]$FailureType,
        $StartUtc,
        $EndUtc,
        [string]$ObjectType,
        [string]$HostName,
        [string]$ObjectName,
        [string]$DatabaseName,
        [string]$FailedMessage,
        $StartUsecs,
        $EndUsecs
    )

    return [pscustomobject]@{
        WindowKey         = $Window.Key
        WindowLabel       = $Window.Label
        Cluster           = $ClusterName
        ClusterId         = $ClusterId
        Environment       = $Environment
        ProtectionGroup   = [string]$ProtectionGroup.name
        ProtectionGroupId = [string]$ProtectionGroup.id
        RunType           = $RunType
        RunStatus         = $RunStatus
        FailureType       = $FailureType
        StartTimeET       = Convert-UtcToEtText $StartUtc
        EndTimeET         = Convert-UtcToEtText $EndUtc
        ObjectType        = $ObjectType
        Host              = $HostName
        ObjectName        = $ObjectName
        DatabaseName      = $DatabaseName
        FailedMessage     = $FailedMessage
        StartTimeUsecs    = $StartUsecs
        EndTimeUsecs      = $EndUsecs
        EndUtc            = $EndUtc
    }
}

function Get-LatestUnclearedFailuresForPg {
    param(
        $Window,
        [string]$ClusterName,
        [string]$ClusterId,
        $ProtectionGroup,
        $Runs
    )

    $environment = Get-PgEnvironment $ProtectionGroup
    $latestByKey = @{}
    $cleared = [System.Collections.Generic.HashSet[string]]::new()
    $runLevelByType = @{}
    $runTypeCleared = [System.Collections.Generic.HashSet[string]]::new()

    $runsWithInfo = @()

    foreach ($run in @($Runs)) {
        $info = Get-FirstInfo $run
        if ($null -eq $info) { continue }

        $endUsecs = 0
        if ($null -ne $info.endTimeUsecs) { $endUsecs = [int64]$info.endTimeUsecs }

        $runsWithInfo += [pscustomobject]@{
            Run      = $run
            Info     = $info
            EndUsecs = $endUsecs
        }
    }

    $runsWithInfo = @($runsWithInfo | Sort-Object EndUsecs -Descending)

    foreach ($item in $runsWithInfo) {
        $run = $item.Run
        $info = $item.Info

        $runType = [string]$info.runType
        if ([string]::IsNullOrWhiteSpace($runType)) { $runType = "UNKNOWN" }

        $runStatus = [string]$info.status
        $startUtc = Convert-UsecsToUtc $info.startTimeUsecs
        $endUtc = Convert-UsecsToUtc $info.endTimeUsecs
        $objects = @($run.objects)

        if ($runStatus -eq "Succeeded" -or $runStatus -eq "SucceededWithWarning") {
            [void]$runTypeCleared.Add($runType)
        }

        if ($objects.Count -eq 0) {
            if ($runStatus -eq "Failed" -and -not $runTypeCleared.Contains($runType) -and -not $runLevelByType.ContainsKey($runType)) {
                $msg = CleanText $info.messages
                if ([string]::IsNullOrWhiteSpace($msg)) {
                    $msg = "Run marked Failed but no object details were returned."
                }

                $runLevelByType[$runType] = New-OutputRow `
                    -Window $Window `
                    -ClusterName $ClusterName `
                    -ClusterId $ClusterId `
                    -Environment $environment `
                    -ProtectionGroup $ProtectionGroup `
                    -RunType $runType `
                    -RunStatus $runStatus `
                    -FailureType "RunLevelFailedNoObjectDetails" `
                    -StartUtc $startUtc `
                    -EndUtc $endUtc `
                    -ObjectType "" `
                    -HostName "" `
                    -ObjectName "" `
                    -DatabaseName "" `
                    -FailedMessage $msg `
                    -StartUsecs $info.startTimeUsecs `
                    -EndUsecs $info.endTimeUsecs
            }

            continue
        }

        # Pass 1: newer object successes clear older object failures.
        foreach ($ob in $objects) {
            if ($null -eq $ob.object) { continue }

            $role = Get-TargetRole -Environment $environment -Object $ob.object
            if ([string]::IsNullOrWhiteSpace($role)) { continue }

            $key = Get-ObjectKey $ob
            if ([string]::IsNullOrWhiteSpace($key)) { continue }

            $attempts = @()
            if ($ob.localSnapshotInfo -and $ob.localSnapshotInfo.failedAttempts) {
                $attempts = @($ob.localSnapshotInfo.failedAttempts)
            }

            if ($ob.localSnapshotInfo -and $attempts.Count -eq 0 -and -not $latestByKey.ContainsKey($key)) {
                [void]$cleared.Add($key)
            }
        }

        # Pass 2: capture only the first failure seen while walking newest to oldest.
        foreach ($ob in $objects) {
            if ($null -eq $ob.object) { continue }

            $role = Get-TargetRole -Environment $environment -Object $ob.object
            if ([string]::IsNullOrWhiteSpace($role)) { continue }

            $key = Get-ObjectKey $ob
            if ([string]::IsNullOrWhiteSpace($key)) { continue }
            if ($cleared.Contains($key)) { continue }
            if ($latestByKey.ContainsKey($key)) { continue }

            $attempts = @()
            if ($ob.localSnapshotInfo -and $ob.localSnapshotInfo.failedAttempts) {
                $attempts = @($ob.localSnapshotInfo.failedAttempts)
            }

            if (-not $attempts -or $attempts.Count -eq 0) {
                if ($environment -eq "kPhysical" -and $runStatus -eq "Failed") {
                    $attempts = @([pscustomobject]@{ message = "No failedAttempts[] details found - run marked Failed" })
                }
                else {
                    continue
                }
            }

            $objectType = [string]$ob.object.objectType
            $objectName = [string]$ob.object.name
            $hostName = ""
            $databaseName = ""
            $failureType = "LatestUnclearedObjectFailure"

            if ($role -eq "Database") {
                $databaseName = $objectName
                $objectName = ""
                $hostName = Find-HostNameInObjects -Objects $objects -SourceId $ob.object.sourceId
            }
            elseif ($role -eq "HostLevel") {
                $hostName = $objectName
                $objectName = ""
                $databaseName = "No DBs Found (Host-Level Failure)"
                $failureType = "LatestUnclearedHostLevelFailure"
            }

            $msg = Get-FailedMessage $attempts
            if ([string]::IsNullOrWhiteSpace($msg)) { continue }

            $latestByKey[$key] = New-OutputRow `
                -Window $Window `
                -ClusterName $ClusterName `
                -ClusterId $ClusterId `
                -Environment $environment `
                -ProtectionGroup $ProtectionGroup `
                -RunType $runType `
                -RunStatus $runStatus `
                -FailureType $failureType `
                -StartUtc $startUtc `
                -EndUtc $endUtc `
                -ObjectType $objectType `
                -HostName $hostName `
                -ObjectName $objectName `
                -DatabaseName $databaseName `
                -FailedMessage $msg `
                -StartUsecs $info.startTimeUsecs `
                -EndUsecs $info.endTimeUsecs
        }
    }

    $out = @()

    foreach ($row in $latestByKey.Values) {
        if ($null -ne $row.EndUtc -and $row.EndUtc -ge $Window.StartUtc -and $row.EndUtc -lt $Window.EndUtc) {
            $out += $row
        }
    }

    foreach ($row in $runLevelByType.Values) {
        if ($null -ne $row.EndUtc -and $row.EndUtc -ge $Window.StartUtc -and $row.EndUtc -lt $Window.EndUtc) {
            $out += $row
        }
    }

    return $out
}

$window = Get-Window

Write-Host ""
Write-Host "Cohesity Backup Failures - Object-Level Latest Uncleared" -ForegroundColor Cyan
Write-Host "Mode      : GET only"
Write-Host "Window    : $($window.Label)"
Write-Host "WindowKey : $($window.Key)"
Write-Host ""

$clusterResp = Invoke-GetJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
$rawClusters = @($clusterResp.cohesityClusters)
if (-not $rawClusters -or $rawClusters.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = for ($i = 0; $i -lt $rawClusters.Count; $i++) {
    [pscustomobject]@{
        Index       = $i + 1
        ClusterName = Get-ClusterName $rawClusters[$i]
        ClusterId   = [string]$rawClusters[$i].clusterId
    }
}

$clusters = @($clusters | Sort-Object ClusterName)
for ($i = 0; $i -lt $clusters.Count; $i++) { $clusters[$i].Index = $i + 1 }

Write-Host "Available clusters:" -ForegroundColor Cyan
$clusters | Format-Table -AutoSize
Write-Host "[0] All clusters"
Write-Host "[X] Exit"
Write-Host ""

while ($true) {
    $choice = Read-Host "Choose cluster number"
    if ($choice -match '^(x|X|q|Q)$') { return }

    [int]$n = 0
    if ([int]::TryParse($choice, [ref]$n) -and $n -ge 0 -and $n -le $clusters.Count) { break }

    Write-Host "Invalid choice." -ForegroundColor Red
}

if ($n -eq 0) {
    $selectedClusters = @($clusters)
    $label = "ALL"
}
else {
    $selectedClusters = @($clusters | Where-Object { $_.Index -eq $n })
    $label = $selectedClusters[0].ClusterName
}

$rows = @()
$summary = @()

foreach ($c in $selectedClusters) {
    Write-Host "Processing $($c.ClusterName)" -ForegroundColor Cyan

    $headers = @{
        "apiKey"          = $cohesityToken
        "accept"          = "application/json"
        "accessClusterId" = $c.ClusterId
    }

    try {
        $pgResp = Invoke-GetJson -Uri "$baseUrl/v2/data-protect/protection-groups?isDeleted=false&isPaused=false&isActive=true&includeLastRunInfo=true" -Headers $headers
        $pgs = @($pgResp.protectionGroups | Sort-Object name)
    }
    catch {
        Write-Host "  Failed PG query: $($_.Exception.Message)" -ForegroundColor Yellow
        continue
    }

    $clusterRowCount = 0
    $i = 0

    foreach ($pg in $pgs) {
        $i++
        Write-Host ("  [{0}/{1}] {2}" -f $i, $pgs.Count, $pg.name)

        try {
            $runUri = "$baseUrl/v2/data-protect/protection-groups/$($pg.id)/runs?numRuns=$MaxRunsPerProtectionGroup&excludeNonRestorableRuns=false&includeObjectDetails=true"
            $runResp = Invoke-GetJson -Uri $runUri -Headers $headers
            $runs = @($runResp.runs)
        }
        catch {
            Write-Host "    Run query failed." -ForegroundColor Yellow
            continue
        }

        if (-not $runs -or $runs.Count -eq 0) { continue }

        $pgRows = @(Get-LatestUnclearedFailuresForPg -Window $window -ClusterName $c.ClusterName -ClusterId $c.ClusterId -ProtectionGroup $pg -Runs $runs)

        if ($pgRows.Count -gt 0) {
            $rows += $pgRows
            $clusterRowCount += $pgRows.Count
        }
    }

    $summary += [pscustomobject]@{
        Cluster                 = $c.ClusterName
        ProtectionGroupsChecked = $pgs.Count
        LatestUnclearedRows     = $clusterRowCount
    }
}

Write-Host ""
Write-Host "Summary" -ForegroundColor Cyan
$summary | Format-Table -AutoSize

Write-Host ""
Write-Host "Latest uncleared failure rows in window: $($rows.Count)" -ForegroundColor Green

if ($rows.Count -gt 0) {
    $preview = $rows |
        Sort-Object EndTimeUsecs -Descending |
        Select-Object -First 25 Cluster, Environment, ProtectionGroup, RunType, RunStatus, FailureType, EndTimeET, Host, ObjectName, DatabaseName, FailedMessage

    $preview | Format-Table -AutoSize

    if (-not $NoCsv) {
        if (-not (Test-Path $outputDir)) { New-Item -Path $outputDir -ItemType Directory | Out-Null }

        $safeLabel = (($label -replace '[^a-zA-Z0-9_.-]', '_'))
        $csv = Join-Path $outputDir ("BackupFailures_{0}_{1}_{2}.csv" -f $safeLabel, $window.Key, (Get-Date -Format "yyyyMMdd_HHmmss"))

        $rows |
            Sort-Object Cluster, ProtectionGroup, EndTimeUsecs -Descending |
            Select-Object WindowKey, WindowLabel, Cluster, ClusterId, Environment, ProtectionGroup, ProtectionGroupId, RunType, RunStatus, FailureType, StartTimeET, EndTimeET, ObjectType, Host, ObjectName, DatabaseName, FailedMessage, StartTimeUsecs, EndTimeUsecs |
            Export-Csv -Path $csv -NoTypeInformation -Encoding UTF8

        Write-Host "CSV saved: $csv" -ForegroundColor Green
    }
}
else {
    Write-Host "No latest uncleared object-level failure rows found in the current compute window." -ForegroundColor Yellow
}

Write-Host "Done. No registry/state/SNOW updates created." -ForegroundColor Cyan
