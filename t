# -------------------------------------------------------------
# Cohesity Oracle – Unresolved DB/Host Failures (Run-first + Object drill, Task-ID matched)
# -------------------------------------------------------------

# --- Config ---
$cluster_name = "YourClusterName"
$cluster_id   = "YourClusterID"
$baseUrl      = "https://helios.cohesity.com"
$apikeypath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$numRuns      = 30
$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"

# --- Prep ---
if (-not (Test-Path $apikeypath)) { throw "API key file not found: $apikeypath" }
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()
if (-not (Test-Path $logDirectory)) { New-Item -Path $logDirectory -ItemType Directory | Out-Null }

function Convert-ToUtcFromEpoch($v) {
    if ($null -eq $v -or $v -eq 0) { return $null }
    try   { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime }
    catch { [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime }
}
$tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

$headers = @{
    apiKey          = $apiKey
    accessClusterId = "$cluster_id"
}

# --- Get Oracle PGs ---
$pgResp = Invoke-WebRequest -Method GET -Uri "$baseUrl/v2/data-protect/protection-groups" -Headers $headers -Body @{
    environments = "kOracle"
    isDeleted    = "False"
    isPaused     = "False"
    isActive     = "True"
}
$pgs = ($pgResp.Content | ConvertFrom-Json).protectionGroups

$rows = @()

foreach ($pg in $pgs) {

    $pgId   = $pg.id
    $pgName = $pg.name

    # --- Get runs (with object details) ---
    $runsResp = Invoke-WebRequest -Method GET -Uri "$baseUrl/v2/data-protect/protection-groups/$pgId/runs" -Headers $headers -Body @{
        environments             = "kOracle"
        isDeleted                = "False"
        isPaused                 = "False"
        isActive                 = "True"
        numRuns                  = "$numRuns"
        excludeNonRestorableRuns = "False"
        includeObjectDetails     = "True"
    }
    $runsJson = $runsResp | ConvertFrom-Json
    if (-not $runsJson.runs) { continue }

    $runs = $runsJson.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

    # --- Step 1: Find all failed runs ---
    $failedRunRecords = foreach ($run in $runs) {
        foreach ($info in ($run.localBackupInfo | Where-Object { $_ })) {
            if ($info.status -eq "Failed") {
                [pscustomobject]@{
                    RunObj         = $run
                    RunType        = $info.runType
                    StartUsecs     = [int64]$info.startTimeUsecs
                    EndUsecs       = [int64]$info.endTimeUsecs
                    ProgressTaskId = $info.progressTaskId
                }
            }
        }
    }

    if (-not $failedRunRecords) { continue }

    foreach ($fail in $failedRunRecords) {

        # --- Step 2: Check for later success for same runType ---
        $laterSuccessExists = $false
        foreach ($r2 in $runs) {
            foreach ($i2 in ($r2.localBackupInfo | Where-Object { $_ })) {
                if ($i2.runType -eq $fail.RunType -and
                    $i2.status  -eq "Succeeded" -and
                    [int64]$i2.startTimeUsecs -gt $fail.EndUsecs) {
                    $laterSuccessExists = $true
                    break
                }
            }
            if ($laterSuccessExists) { break }
        }
        if ($laterSuccessExists) { continue }  # skip resolved runs

        # --- Step 3: Drill into this failed run's objects[] ---
        $run = $fail.RunObj
        if (-not $run.objects) { continue }

        $dbObjs    = $run.objects | Where-Object { $_.object.objectType  -eq 'kDatabase' }
        $hostsObjs = $run.objects | Where-Object { $_.object.environment -eq 'kPhysical' }

        $startLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $fail.StartUsecs), $tz)
        $endLocal   = [System.TimeZoneInfo]::ConvertTimeFromUtc((Convert-ToUtcFromEpoch $fail.EndUsecs),   $tz)

        foreach ($db in $dbObjs) {
            $failedAttempts = $db.localSnapshotInfo.failedAttempts
            if (-not $failedAttempts) { continue }

            # Optional task-ID filter for precise match
            $matchingAttempts = foreach ($fa in $failedAttempts) {
                $objPT = $fa.progressTaskId
                if ($null -ne $objPT -and $null -ne $fail.ProgressTaskId) {
                    if ($objPT -eq $fail.ProgressTaskId) { $fa }
                }
                else { $fa }  # accept if taskId not present on one side
            }

            foreach ($fa in $matchingAttempts) {
                if (-not $fa.message) { continue }

                $hostObj = $hostsObjs | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                $hostName = if ($hostObj) { $hostObj.object.name } else { 'N/A' }

                $rows += [pscustomobject]@{
                    Cluster         = $cluster_name
                    ProtectionGroup = $pgName
                    Hosts           = $hostName
                    DatabaseName    = $db.object.name
                    RunType         = $fail.RunType
                    StartTime       = $startLocal
                    EndTime         = $endLocal
                    FailedMessage   = $fa.message
                    ProgressTaskId  = $fail.ProgressTaskId
                }
            }
        }
    }
}

# --- Output unresolved only ---
if ($rows.Count -gt 0) {
    Write-Host "`n🔥 Unresolved DB/Host Failures (Run-first, Task-ID matched) ---`n"
    $rows | Sort-Object Cluster, ProtectionGroup, Hosts, DatabaseName |
        Format-Table Cluster, ProtectionGroup, Hosts, DatabaseName, RunType, StartTime, EndTime, FailedMessage, ProgressTaskId -AutoSize

    $csvFile = Join-Path $logDirectory ("Cohesity_Unresolved_DBHost_Failures_{0}.csv" -f (Get-Date -Format "yyyy-MM-dd_HHmm"))
    $rows | Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8
    Write-Host "`n✅ Saved report to $csvFile"
}
else {
    Write-Host "`n✅ No unresolved DB/Host failures found on cluster $cluster_name."
}
