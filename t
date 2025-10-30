# -------------------------------------------------------------
# Cohesity Oracle – Unresolved Failures (Host + DB level)
# -------------------------------------------------------------
# Detects:
#   1. Host discovery failures ("Failed to discover databases")
#   2. Host or DB backup failures via failedAttempts[].message
# Filters:
#   • Excludes runs that have a later successful backup of same type
# -------------------------------------------------------------

# --- CONFIG ---
$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"
$apiKeyPath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$ErrorActionPreference = 'Stop'

# --- API Key Load ---
if (-not (Test-Path $apiKeyPath)) { throw "API key file not found: $apiKeyPath" }
$apiKey   = (Get-Content -Path $apiKeyPath -Raw).Trim()
$headers  = @{ apiKey = $apiKey; accessClusterId = $cluster_id }

# --- Time Conversion ---
function Convert-ToUtcFromEpoch($v) {
    if ($null -eq $v -or $v -eq 0) { return $null }
    try { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}
$tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

# --- Get Oracle Protection Groups ---
Write-Host "`n🔍 Fetching Oracle protection groups..." -ForegroundColor Cyan
$pgResp = Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups" -Headers $headers -Body @{
    environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True"
} -Method Get
$pgs = ($pgResp.Content | ConvertFrom-Json).protectionGroups
if (-not $pgs) { throw "No Oracle protection groups found!" }

$globalFailures = @()

# =============================================================
# MAIN LOOP: Go through each protection group
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

    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

    # =========================================================
    # Loop through each run and its localBackupInfo[]
    # =========================================================
    foreach ($run in $runs) {
        foreach ($info in $run.localBackupInfo) {

            if ($info.status -ne "Failed") { continue }

            $runType    = $info.runType
            $runStartUs = [int64]$info.startTimeUsecs
            $runEndUs   = [int64]$info.endTimeUsecs
            $progressId = $info.progressTaskId
            $startLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $runStartUs), $tz)
            $endLocal   = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $runEndUs),   $tz)

            # --- Skip if later success exists ---
            $laterSuccess = $runs | Where-Object {
                $_.localBackupInfo[0].runType -eq $runType -and
                $_.localBackupInfo[0].status  -eq "Succeeded" -and
                [int64]$_.localBackupInfo[0].startTimeUsecs -gt $runEndUs
            }
            if ($laterSuccess) { continue }

            # =========================================================
            # 1️⃣ HOST-LEVEL DISCOVERY FAILURES (e.g. "Failed to discover databases")
            # =========================================================
            if ($info.messages) {
                foreach ($msg in $info.messages) {
                    if ($msg -match "Failed to discover databases") {

                        $hostObjs = @()
                        if ($run.objects) {
                            $hostObjs = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }
                        }

                        if ($hostObjs.Count -eq 0) {
                            $globalFailures += [pscustomobject]@{
                                Cluster         = $cluster_name
                                ProtectionGroup = $pgName
                                Hosts           = "Unknown (Discovery Failure)"
                                DatabaseName    = "N/A"
                                RunType         = $runType
                                StartTime       = $startLocal
                                EndTime         = $endLocal
                                FailedMessage   = $msg
                            }
                        } else {
                            foreach ($h in $hostObjs) {
                                $globalFailures += [pscustomobject]@{
                                    Cluster         = $cluster_name
                                    ProtectionGroup = $pgName
                                    Hosts           = $h.object.name
                                    DatabaseName    = "N/A"
                                    RunType         = $runType
                                    StartTime       = $startLocal
                                    EndTime         = $endLocal
                                    FailedMessage   = $msg
                                }
                            }
                        }
                    }
                }
            }

            # =========================================================
            # 2️⃣ OBJECT-LEVEL FAILURES (DB + Host backup failures)
            # =========================================================
            if ($run.objects) {
                $dbObjs    = $run.objects | Where-Object { $_.object.objectType  -eq 'kDatabase' }
                $hostObjs  = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }

                # --- DB-level failures ---
                foreach ($db in $dbObjs) {
                    $attempts = $db.localSnapshotInfo.failedAttempts
                    if (-not $attempts) { continue }

                    $matched = $attempts | Where-Object {
                        $_.message -and (
                            (-not $_.progressTaskId) -or
                            ($_.progressTaskId -eq $progressId)
                        )
                    }

                    foreach ($fa in $matched) {
                        $parentHost = $hostObjs | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                        $hostName   = if ($parentHost) { $parentHost.object.name } else { "N/A" }

                        $globalFailures += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            Hosts           = $hostName
                            DatabaseName    = $db.object.name
                            RunType         = $runType
                            StartTime       = $startLocal
                            EndTime         = $endLocal
                            FailedMessage   = $fa.message
                        }
                    }
                }

                # --- Host-level backup failures (not discovery) ---
                $phyObjs = $run.objects | Where-Object { $_.object.objectType -eq 'kPhysical' -and $_.localSnapshotInfo.failedAttempts }
                foreach ($phy in $phyObjs) {
                    foreach ($fa in $phy.localSnapshotInfo.failedAttempts) {
                        $globalFailures += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            Hosts           = $phy.object.name
                            DatabaseName    = "N/A"
                            RunType         = $runType
                            StartTime       = $startLocal
                            EndTime         = $endLocal
                            FailedMessage   = $fa.message
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
    Write-Host "`n🔥 Unresolved Failures (No Later Success):`n" -ForegroundColor Cyan
    $globalFailures | Sort-Object ProtectionGroup, Hosts, DatabaseName |
        Format-Table ProtectionGroup, Hosts, DatabaseName, RunType, StartTime, EndTime, FailedMessage -AutoSize

    # Optional CSV export
    $timestamp = Get-Date -Format "yyyyMMdd_HHmm"
    $csvPath = "X:\PowerShell\Cohesity_Reports\BackupFailures_Oracle_$timestamp.csv"
    $globalFailures | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
    Write-Host "`n📁 CSV exported to: $csvPath" -ForegroundColor Green
} else {
    Write-Host "`n✅ No unresolved DB/Host failures found." -ForegroundColor Green
}
