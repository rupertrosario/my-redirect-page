param(
    [int]$MaxProtectionGroups = 0,
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
    } else {
        $r = Invoke-WebRequest -Method GET -Uri $Uri -Headers $Headers
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function Usecs-ToUtc {
    param($Usecs)
    if ($null -eq $Usecs -or $Usecs -eq 0) { return $null }
    try { return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000)).UtcDateTime } catch { return $null }
}

function Utc-ToEtText {
    param([datetime]$Utc)
    if ($null -eq $Utc) { return "" }
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    return ([System.TimeZoneInfo]::ConvertTimeFromUtc($Utc, $tz)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Get-Window {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    $nowEt = [System.TimeZoneInfo]::ConvertTimeFromUtc([datetime]::UtcNow, $tz)
    if ($nowEt.Hour -lt 18) { $startEt = $nowEt.Date.AddDays(-1).AddHours(18) } else { $startEt = $nowEt.Date.AddHours(18) }
    $endEt = $startEt.AddDays(1)
    [pscustomobject]@{
        Key = $startEt.ToString("yyyy-MM-dd_1800ET")
        Label = "{0} ET -> {1} ET" -f $startEt.ToString("yyyy-MM-dd HH:mm"), $endEt.ToString("yyyy-MM-dd HH:mm")
        StartUtc = [System.TimeZoneInfo]::ConvertTimeToUtc($startEt, $tz)
        EndUtc = [System.TimeZoneInfo]::ConvertTimeToUtc($endEt, $tz)
    }
}

function CleanText {
    param($Value)
    if ($null -eq $Value) { return "" }
    if ($Value -is [System.Array]) { $Value = ($Value | ForEach-Object { "$_" }) -join " | " }
    return (([string]$Value -replace "[`r`n]+", " ") -replace "\s+", " ").Trim()
}

function FirstInfo {
    param($Run)
    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    return @($Run.localBackupInfo)[0]
}

function ClusterName {
    param($Cluster)
    if ($Cluster.clusterName) { return [string]$Cluster.clusterName }
    if ($Cluster.name) { return [string]$Cluster.name }
    if ($Cluster.displayName) { return [string]$Cluster.displayName }
    return "Unknown-$($Cluster.clusterId)"
}

function PgEnv {
    param($Pg)
    if ($Pg.environment) { return [string]$Pg.environment }
    if ($Pg.environmentTypes) { return [string]@($Pg.environmentTypes)[0] }
    return "Unknown"
}

function FailedMessage {
    param($Attempts)
    $m = @()
    foreach ($a in @($Attempts)) {
        if ($a -and $a.message) { $m += (CleanText $a.message) }
    }
    return ($m -join " | ")
}

$window = Get-Window

Write-Host ""
Write-Host "Cohesity Backup Failures - Simple Cluster Run" -ForegroundColor Cyan
Write-Host "Mode      : GET only"
Write-Host "Window    : $($window.Label)"
Write-Host "WindowKey : $($window.Key)"
Write-Host ""

$clusterResp = Invoke-GetJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
$rawClusters = @($clusterResp.cohesityClusters)
if (-not $rawClusters -or $rawClusters.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = for ($i = 0; $i -lt $rawClusters.Count; $i++) {
    [pscustomobject]@{ Index = $i + 1; ClusterName = ClusterName $rawClusters[$i]; ClusterId = [string]$rawClusters[$i].clusterId }
}
$clusters = @($clusters | Sort-Object ClusterName)
for ($i = 0; $i -lt $clusters.Count; $i++) { $clusters[$i].Index = $i + 1 }

Write-Host "Available clusters:" -ForegroundColor Cyan
$clusters | Format-Table -AutoSize
Write-Host "[0] All clusters"
Write-Host "[X] Exit"

while ($true) {
    $choice = Read-Host "Choose cluster number"
    if ($choice -match '^(x|X|q|Q)$') { return }
    [int]$n = 0
    if ([int]::TryParse($choice, [ref]$n) -and $n -ge 0 -and $n -le $clusters.Count) { break }
    Write-Host "Invalid choice." -ForegroundColor Red
}

if ($n -eq 0) { $selectedClusters = @($clusters); $label = "ALL" } else { $selectedClusters = @($clusters | Where-Object { $_.Index -eq $n }); $label = $selectedClusters[0].ClusterName }

$rows = @()
$summary = @()

foreach ($c in $selectedClusters) {
    Write-Host "Processing $($c.ClusterName)" -ForegroundColor Cyan
    $headers = @{ "apiKey" = $cohesityToken; "accept" = "application/json"; "accessClusterId" = $c.ClusterId }

    try {
        $pgResp = Invoke-GetJson -Uri "$baseUrl/v2/data-protect/protection-groups?isDeleted=false&isPaused=false&isActive=true&includeLastRunInfo=true" -Headers $headers
        $pgs = @($pgResp.protectionGroups | Sort-Object name)
    } catch {
        Write-Host "  Failed PG query: $($_.Exception.Message)" -ForegroundColor Yellow
        continue
    }

    if ($MaxProtectionGroups -gt 0) { $pgs = @($pgs | Select-Object -First $MaxProtectionGroups) }
    $failCount = 0
    $i = 0

    foreach ($pg in $pgs) {
        $i++
        Write-Host ("  [{0}/{1}] {2}" -f $i, $pgs.Count, $pg.name)
        $env = PgEnv $pg

        try {
            $runResp = Invoke-GetJson -Uri "$baseUrl/v2/data-protect/protection-groups/$($pg.id)/runs?numRuns=$MaxRunsPerProtectionGroup&excludeNonRestorableRuns=false&includeObjectDetails=true" -Headers $headers
            $runs = @($runResp.runs)
        } catch {
            continue
        }

        foreach ($run in $runs) {
            $info = FirstInfo $run
            if ($null -eq $info) { continue }
            $endUtc = Usecs-ToUtc $info.endTimeUsecs
            $startUtc = Usecs-ToUtc $info.startTimeUsecs
            if ($null -eq $endUtc -or $endUtc -lt $window.StartUtc -or $endUtc -ge $window.EndUtc) { continue }

            $objects = @($run.objects)
            $hadObjectFailure = $false

            foreach ($ob in $objects) {
                if ($null -eq $ob.object) { continue }
                $attempts = @()
                if ($ob.localSnapshotInfo -and $ob.localSnapshotInfo.failedAttempts) { $attempts = @($ob.localSnapshotInfo.failedAttempts) }
                if (-not $attempts -or $attempts.Count -eq 0) { continue }

                $hadObjectFailure = $true
                $rows += [pscustomobject]@{
                    WindowKey = $window.Key
                    Cluster = $c.ClusterName
                    Environment = $env
                    ProtectionGroup = $pg.name
                    RunType = $info.runType
                    RunStatus = $info.status
                    FailureType = "ObjectFailedAttempt"
                    StartTimeET = Utc-ToEtText $startUtc
                    EndTimeET = Utc-ToEtText $endUtc
                    ObjectType = $ob.object.objectType
                    ObjectName = $ob.object.name
                    FailedMessage = FailedMessage $attempts
                    EndTimeUsecs = $info.endTimeUsecs
                }
                $failCount++
            }

            if ($info.status -eq "Failed" -and -not $hadObjectFailure) {
                $rows += [pscustomobject]@{
                    WindowKey = $window.Key
                    Cluster = $c.ClusterName
                    Environment = $env
                    ProtectionGroup = $pg.name
                    RunType = $info.runType
                    RunStatus = $info.status
                    FailureType = "RunLevelFailedNoObjectDetails"
                    StartTimeET = Utc-ToEtText $startUtc
                    EndTimeET = Utc-ToEtText $endUtc
                    ObjectType = ""
                    ObjectName = ""
                    FailedMessage = CleanText $info.messages
                    EndTimeUsecs = $info.endTimeUsecs
                }
                $failCount++
            }
        }
    }

    $summary += [pscustomobject]@{ Cluster = $c.ClusterName; ProtectionGroupsChecked = $pgs.Count; FailureRowsInWindow = $failCount }
}

Write-Host ""
Write-Host "Summary" -ForegroundColor Cyan
$summary | Format-Table -AutoSize

Write-Host ""
Write-Host "Failure rows in window: $($rows.Count)" -ForegroundColor Green

if ($rows.Count -gt 0) {
    $rows | Sort-Object EndTimeUsecs -Descending | Select-Object -First 25 Cluster,Environment,ProtectionGroup,RunType,RunStatus,FailureType,EndTimeET,ObjectName,FailedMessage | Format-Table -AutoSize

    if (-not $NoCsv) {
        if (-not (Test-Path $outputDir)) { New-Item -Path $outputDir -ItemType Directory | Out-Null }
        $safeLabel = (($label -replace '[^a-zA-Z0-9_.-]', '_'))
        $csv = Join-Path $outputDir ("BackupFailures_{0}_{1}_{2}.csv" -f $safeLabel, $window.Key, (Get-Date -Format "yyyyMMdd_HHmmss"))
        $rows | Sort-Object Cluster,ProtectionGroup,EndTimeUsecs -Descending | Export-Csv -Path $csv -NoTypeInformation -Encoding UTF8
        Write-Host "CSV saved: $csv" -ForegroundColor Green
    }
} else {
    Write-Host "No failure rows found in the current compute window." -ForegroundColor Yellow
}

Write-Host "Done. No registry/state/SNOW updates created." -ForegroundColor Cyan
