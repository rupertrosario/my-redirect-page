# -------------------------------------------------------------
# Cohesity Oracle – Unresolved Failures (Host+DB Linked)
# -------------------------------------------------------------
# ✅ One cluster only
# ✅ Checks host-level + DB-level relationship (sourceId mapping)
# ✅ Shows DBs that failed under failed hosts
# ✅ Skips runs with later success
# ✅ Sorted by EndTime
# ✅ Keeps your CSV path exactly the same
# -------------------------------------------------------------

$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"
$apiKeyPath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$ErrorActionPreference = 'Stop'

# --- API Key ---
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

    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

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
            # 1️⃣ RUN-LEVEL FAILURES
            # =========================================================
            if ($info.messages) {
                foreach ($msg in $info.messages) {
                    $msgClean = ($msg -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()
                    $globalFailures += [pscustomobject]@{
                        Cluster         = $cluster_name
                        ProtectionGroup = $pgName
                        Hosts           = "Unknown (Run-Level Failure)"
                        DatabaseName    = "No DBs Found"
                        RunType         = $runType
                        StartTime       = $startLocal
                        EndTime         = $endLocal
                        FailedMessage   = $msgClean
                    }
                }
            }

            # =========================================================
            # 2️⃣ OBJECT-LEVEL FAILURES
            # =========================================================
            if ($run.objects) {
                $dbObjs   = $run.objects | Where-Object { $_.object.objectType  -eq 'kDatabase' }
                $phyObjs  = $run.objects | Where-Object { $_.object.objectType  -eq 'kPhysical' }

                # --- DB-level failures ---
                foreach ($db in $dbObjs) {
                    $attempts = $db.localSnapshotInfo.failedAttempts
                    if (-not $attempts) { continue }
                    foreach ($fa in $attempts) {
                        $msgClean = ($fa.message -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()
                        $parentHost = $phyObjs | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                        $hostName   = if ($parentHost) { $parentHost.object.name } else { "N/A" }

                        $globalFailures += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            Hosts           = $hostName
                            DatabaseName    = $db.object.name
                            RunType         = $runType
                            StartTime       = $startLocal
                            EndTime         = $endLocal
                            FailedMessage   = $msgClean
                        }
                    }
                }

                # --- Host-level failures ---
                foreach ($phy in $phyObjs) {
                    $attempts = $phy.localSnapshotInfo.failedAttempts
                    if (-not $attempts) { continue }

                    foreach ($fa in $attempts) {
                        $msgClean = ($fa.message -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()

                        # find DBs under this host
                        $dbsUnderHost = $dbObjs | Where-Object { $_.object.sourceId -eq $phy.object.id }
                        if ($dbsUnderHost.Count -gt 0) {
                            foreach ($db in $dbsUnderHost) {
                                $dbFails = $db.localSnapshotInfo.failedAttempts
                                if ($dbFails) {
                                    foreach ($dbfa in $dbFails) {
                                        $dbMsg = ($dbfa.message -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()
                                        $globalFailures += [pscustomobject]@{
                                            Cluster         = $cluster_name
                                            ProtectionGroup = $pgName
                                            Hosts           = $phy.object.name
                                            DatabaseName    = $db.object.name
                                            RunType         = $runType
                                            StartTime       = $startLocal
                                            EndTime         = $endLocal
                                            FailedMessage   = $dbMsg
                                        }
                                    }
                                } else {
                                    $globalFailures += [pscustomobject]@{
                                        Cluster         = $cluster_name
                                        ProtectionGroup = $pgName
                                        Hosts           = $phy.object.name
                                        DatabaseName    = $db.object.name
                                        RunType         = $runType
                                        StartTime       = $startLocal
                                        EndTime         = $endLocal
                                        FailedMessage   = $msgClean
                                    }
                                }
                            }
                        } else {
                            $globalFailures += [pscustomobject]@{
                                Cluster         = $cluster_name
                                ProtectionGroup = $pgName
                                Hosts           = $phy.object.name
                                DatabaseName    = "No DBs Found (Host-Level Failure)"
                                RunType         = $runType
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
}

# =============================================================
# OUTPUT SECTION
# =============================================================
if ($globalFailures.Count -gt 0) {
    Write-Host "`n🔥 Unresolved Oracle Failures (Cluster: $cluster_name):`n" -ForegroundColor Cyan
    $sorted = $globalFailures | Sort-Object EndTime -Descending
    $sorted | Format-Table ProtectionGroup, Hosts, DatabaseName, RunType, StartTime, EndTime, FailedMessage -AutoSize

    $timestamp = Get-Date -Format "yyyyMMdd_HHmm"
    $csvPath = "X:\PowerShell\Data\Choesity\BackupFailutes\BackupFailures_Oracle_AllClusters_$timestamp.csv"
    $sorted | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8BOM

    Write-Host "`n📁 CSV exported to: $csvPath" -ForegroundColor Green
} else {
    Write-Host "`n✅ No unresolved DB/Host failures found on $cluster_name." -ForegroundColor Green
}
