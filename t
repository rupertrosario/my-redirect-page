# -------------------------------------------------------------
# Cohesity Oracle – Unresolved DB/Host Failures (Simplified & Working)
# -------------------------------------------------------------

# --- Config ---
$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"
$apiKeyPath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

$ErrorActionPreference = 'Stop'

if (-not (Test-Path $apiKeyPath)) { throw "API key file not found: $apiKeyPath" }
$apiKey = (Get-Content -Path $apiKeyPath -Raw).Trim()
$headers = @{ apiKey = $apiKey; accessClusterId = $cluster_id }

function Convert-ToUtcFromEpoch($v){
    if ($null -eq $v -or $v -eq 0) { return $null }
    try { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}

$estZone = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

# --- Get Protection Groups ---
$pgUrl  = "$baseUrl/v2/data-protect/protection-groups"
$pgBody = @{ environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True" }
$pgResp = Invoke-WebRequest -Uri $pgUrl -Headers $headers -Body $pgBody -Method Get
$pgs = ($pgResp.Content | ConvertFrom-Json).protectionGroups

$globalFailures = @()

foreach ($pg in $pgs) {

    $pgId   = $pg.id
    $pgName = $pg.name
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

    # --- STEP 1: Identify failed runs ---
    foreach ($run in $runs) {
        foreach ($info in $run.localBackupInfo) {
            if ($info.status -eq "Failed") {

                $runType = $info.runType
                $runStartUsecs = $info.startTimeUsecs
                $runEndUsecs   = $info.endTimeUsecs
                $runStart      = Convert-ToUtcFromEpoch $runStartUsecs
                $runEnd        = Convert-ToUtcFromEpoch $runEndUsecs

                # --- Print detected failed run (for confirmation) ---
                Write-Host "`nDetected failed run:" -ForegroundColor Yellow
                Write-Host "PG: $pgName  RunType: $runType  Start: $runStart  End: $runEnd"

                # --- STEP 2: Check if later success exists ---
                $laterSuccess = $runs | Where-Object {
                    $_.localBackupInfo[0].runType -eq $runType -and
                    $_.localBackupInfo[0].status  -eq "Succeeded" -and
                    $_.localBackupInfo[0].startTimeUsecs -gt $runEndUsecs
                }

                if (-not $laterSuccess) {

                    # --- STEP 3: Drill into unresolved failed run ---
                    if ($run.objects) {
                        $dbObjs    = $run.objects | Where-Object { $_.object.objectType  -eq 'kDatabase' }
                        $hostsObjs = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }

                        foreach ($db in $dbObjs) {
                            $failedAttempts = $db.localSnapshotInfo.failedAttempts
                            if ($failedAttempts) {
                                foreach ($fa in $failedAttempts) {
                                    if (-not $fa.message) { continue }

                                    $hostObj = $hostsObjs | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                                    $hostName = if ($hostObj) { $hostObj.object.name } else { 'N/A' }

                                    $globalFailures += [pscustomobject]@{
                                        Cluster         = $cluster_name
                                        ProtectionGroup = $pgName
                                        Hosts           = $hostName
                                        DatabaseName    = $db.object.name
                                        RunType         = $runType
                                        StartTime       = [System.TimeZoneInfo]::ConvertTimeFromUtc($runStart, $estZone)
                                        EndTime         = [System.TimeZoneInfo]::ConvertTimeFromUtc($runEnd, $estZone)
                                        FailedMessage   = $fa.message
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

# --- OUTPUT ---
if ($globalFailures.Count -gt 0) {
    Write-Host "`n🔥 Failed runs without any later success:`n" -ForegroundColor Cyan
    $globalFailures | Sort-Object ProtectionGroup, Hosts, DatabaseName |
        Format-Table ProtectionGroup, Hosts, DatabaseName, RunType, StartTime, EndTime, FailedMessage -AutoSize
} else {
    Write-Host "`n✅ No unresolved DB/Host failures found." -ForegroundColor Green
}
