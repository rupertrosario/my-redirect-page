# -------------------------------------------------------------
# Cohesity Oracle – Unresolved DB/Host Level Failures
# -------------------------------------------------------------

# --- Config ---
$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"

$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"
if (-not (Test-Path $logDirectory)) { New-Item -Path $logDirectory -ItemType Directory | Out-Null }

# --- Load API key ---
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
if (-not (Test-Path $apikeypath)) { throw "API key file not found at $apikeypath" }
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

# --- Helper: Convert Epoch to UTC ---
function Convert-ToUtcFromEpoch($v){
    if($null -eq $v -or $v -eq 0){ return $null }
    try { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}
$estZone = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

# --- API headers ---
$headers = @{
    "apiKey"          = $apiKey
    "accessClusterId" = "$cluster_id"
}

# --- Get Oracle Protection Groups ---
$pgUrl  = "$baseUrl/v2/data-protect/protection-groups"
$pgBody = @{ environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True" }
$pgResp = Invoke-WebRequest -Method Get -Uri $pgUrl -Headers $headers -Body $pgBody
$pgs    = ($pgResp.Content | ConvertFrom-Json).protectionGroups

$unresolved = @()

# --- Loop through each Protection Group ---
foreach ($pg in $pgs) {

    $pgId   = $pg.id
    $pgName = $pg.name

    # --- Get recent runs ---
    $runUrl = "$baseUrl/v2/data-protect/protection-groups/$pgId/runs"
    $runBody = @{
        environments             = "kOracle"
        isDeleted                = "False"
        isPaused                 = "False"
        isActive                 = "True"
        numRuns                  = "10"
        excludeNonRestorableRuns = "False"
    }
    $runResp = Invoke-WebRequest -Method Get -Uri $runUrl -Headers $headers -Body $runBody
    $json    = $runResp | ConvertFrom-Json
    if (-not $json.runs) { continue }

    $runs = $json.runs

    # --- Build host ↔ DB mapping from all runs.objects ---
    $objs  = $runs.objects
    $hosts = $objs | Where-Object { $_.object.environment -eq 'kPhysical' }
    $dbs   = $objs | Where-Object { $_.object.objectType  -eq 'kDatabase' }

    # --- Loop through DB objects with failedAttempts ---
    foreach ($db in $dbs) {
        $fails = $db.localSnapshotInfo.failedAttempts
        if ($fails -and $fails.Count -gt 0) {

            foreach ($f in $fails) {
                if ($f.message) {
                    $dbId = $db.object.id
                    $host = $hosts | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                    $hostId   = if ($host) { $host.object.id } else { $null }
                    $hostName = if ($host) { $host.object.name } else { 'N/A' }

                    # --- Find the run this failure came from ---
                    $runForFail = $runs | Where-Object { $_.objects.object.id -contains $dbId } | Select-Object -First 1
                    $runStart = Convert-ToUtcFromEpoch $runForFail.startTimeUsecs
                    $runEnd   = Convert-ToUtcFromEpoch $runForFail.endTimeUsecs

                    # --- Check for later success for same DB or host ---
                    $laterSuccess = $false
                    foreach ($r2 in $runs) {
                        if ($r2.objects) {
                            foreach ($o2 in $r2.objects) {
                                if (
                                    ($o2.object.id -eq $dbId -or $o2.object.id -eq $hostId) -and
                                    $o2.localSnapshotInfo.status -eq "kSuccess" -and
                                    $r2.startTimeUsecs -gt $runForFail.endTimeUsecs
                                ) {
                                    $laterSuccess = $true
                                    break
                                }
                            }
                        }
                        if ($laterSuccess) { break }
                    }

                    # --- Only record if unresolved (no later success) ---
                    if (-not $laterSuccess) {
                        $unresolved += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            HostName        = $hostName
                            DatabaseName    = $db.object.name
                            FailedMessage   = $f.message
                            StartTime       = if ($runStart) { [System.TimeZoneInfo]::ConvertTimeFromUtc($runStart, $estZone) } else { $null }
                            EndTime         = if ($runEnd)   { [System.TimeZoneInfo]::ConvertTimeFromUtc($runEnd, $estZone) } else { $null }
                        }
                    }
                }
            }
        }
    }
}

# --- Output unresolved failures ---
if ($unresolved.Count -gt 0) {
    Write-Host "`n🔥 Unresolved DB/Host Failures ---`n"
    $unresolved | Sort-Object Cluster, ProtectionGroup, HostName, DatabaseName |
        Format-Table Cluster, ProtectionGroup, HostName, DatabaseName, FailedMessage, StartTime, EndTime -AutoSize

    $reportDate = Get-Date -Format "yyyy-MM-dd_HHmm"
    $csvFile = "$logDirectory\Cohesity_Unresolved_DBHost_Failures_$reportDate.csv"
    $unresolved | Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8
    Write-Host "`n✅ Saved report to $csvFile"
}
else {
    Write-Host "`n✅ No unresolved DB/Host failures found on cluster $cluster_name."
}
