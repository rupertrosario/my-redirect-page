# -------------------------------------------------------------
# Cohesity Oracle – Latest Failures (Sorted by PG + EndTime)
# -------------------------------------------------------------
# ✅ Single cluster
# ✅ Checks latest localBackupInfo per runType
# ✅ Shows DB + Host names for failures
# ✅ Skips successful runTypes
# ✅ Sorted by ProtectionGroup + EndTime descending
# ✅ Console-only output (no CSV)
# -------------------------------------------------------------

$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"
$apiKeyPath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$ErrorActionPreference = 'Stop'

# --- Load API Key ---
if (-not (Test-Path $apiKeyPath)) { throw "API key file not found: $apiKeyPath" }
$apiKey   = (Get-Content -Path $apiKeyPath -Raw).Trim()
$headers  = @{ apiKey = $apiKey; accessClusterId = $cluster_id }

# --- Helper: Epoch → UTC ---
function Convert-ToUtcFromEpoch($v) {
    if ($null -eq $v -or $v -eq 0) { return $null }
    try { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}
$tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

Write-Host "`n🔍 Fetching Oracle protection groups from cluster: $cluster_name..." -ForegroundColor Cyan
$pgResp = Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups" -Headers $headers -Body @{
    environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True"
} -Method Get
$pgs = ($pgResp.Content | ConvertFrom-Json).protectionGroups
if (-not $pgs) { throw "No Oracle protection groups found on $cluster_name!" }

$globalFailures = @()

# =============================================================
# MAIN LOOP
# =============================================================
foreach ($pg in $pgs) {
    $pgId   = $pg.id
    $pgName = $pg.name
    Write-Host "`n📦 Checking PG: $pgName" -ForegroundColor Yellow

    $runUrl = "$baseUrl/v2/data-protect/protection-groups/$pgId/runs"
    $runBody = @{
        environments             = "kOracle"
        isDeleted                = "False"
        isPaused                 = "False"
        isActive                 = "True"
        numRuns                  = "10"
        excludeNonRestorableRuns = "False"
        includeObjectDetails     = "True"
    }

    $runResp = Invoke-WebRequest -Uri $runUrl -Headers $headers -Body $runBody -Method Get
    $json    = $runResp | ConvertFrom-Json
    if (-not $json.runs) { continue }

    # Sort by start time
    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

    # --- Group by runType (kFull, kLog, etc.) ---
    $runTypes = $runs.localBackupInfo.runType | Select-Object -Unique
    foreach ($rType in $runTypes) {
        $latestRun = $runs | Where-Object { $_.localBackupInfo[0].runType -eq $rType } |
            Sort-Object { $_.localBackupInfo[0].endTimeUsecs } -Descending | Select-Object -First 1
        if (-not $latestRun) { continue }

        $info = $latestRun.localBackupInfo[0]
        $status = $info.status
        $runType = $info.runType
        $runStartUs = [int64]$info.startTimeUsecs
        $runEndUs   = [int64]$info.endTimeUsecs
        $startLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $runStartUs), $tz)
        $endLocal   = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $runEndUs),   $tz)

        if ($status -eq "Succeeded" -or $status -eq "SucceededWithWarning") {
            Write-Host "✅ PG: $pgName [$rType] – Latest run succeeded" -ForegroundColor Green
            continue
        }

        Write-Host "❌ PG: $pgName [$rType] – Latest run failed, collecting details..." -ForegroundColor Red

        # =========================================================
        # Collect DB and Host failures
        # =========================================================
        $run = $latestRun
        if ($run.objects) {
            $dbObjs   = $run.objects | Where-Object { $_.object.objectType  -eq 'kDatabase' }
            $hostObjs = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }

            # --- DB-level failures ---
            foreach ($db in $dbObjs) {
                $attempts = $db.localSnapshotInfo.failedAttempts
                if ($attempts) {
                    foreach ($fa in $attempts) {
                        $msgClean = ($fa.message -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()
                        $parentHost = $hostObjs | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                        $hostName = if ($parentHost) { $parentHost.object.name } else { "N/A" }

                        $globalFailures += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            RunType         = $runType
                            Hosts           = $hostName
                            DatabaseName    = $db.object.name
                            StartTime       = $startLocal
                            EndTime         = $endLocal
                            FailedMessage   = $msgClean
                        }
                    }
                }
            }

            # --- Host-level failures ---
            foreach ($phy in $hostObjs) {
                $attempts = $phy.localSnapshotInfo.failedAttempts
                if ($attempts) {
                    foreach ($fa in $attempts) {
                        $msgClean = ($fa.message -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()
                        $globalFailures += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            RunType         = $runType
                            Hosts           = $phy.object.name
                            DatabaseName    = "No DBs Discovered"
                            StartTime       = $startLocal
                            EndTime         = $endLocal
                            FailedMessage   = $msgClean
                        }
                    }
                }
            }
        }
    }
}

# =============================================================
# OUTPUT SECTION
# =============================================================
if ($globalFailures.Count -gt 0) {
    Write-Host "`n🔥 Latest Failed Oracle Runs (Cluster: $cluster_name):`n" -ForegroundColor Cyan

    # --- Sort by PG + EndTime (Descending) ---
    $sorted = $globalFailures | Sort-Object ProtectionGroup, EndTime -Descending

    $sorted | Format-Table ProtectionGroup, RunType, Hosts, DatabaseName, StartTime, EndTime, FailedMessage -AutoSize
} else {
    Write-Host "`n✅ All latest Oracle runs succeeded on $cluster_name." -ForegroundColor Green
}

# Reference CSV path (disabled for now)
$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = "X:\PowerShell\Data\Choesity\BackupFailures\BackupFailures_Oracle_AllClusters_$timestamp.csv"
Write-Host "`n📂 (CSV path ready if needed later): $csvPath" -ForegroundColor Gray
