# -------------------------------------------------------------
# Cohesity Oracle Failures – Multi-Cluster (Helios)
# Strictly READ-ONLY (GET-only)
# Adds Host–DB mapping from $run.objects
# -------------------------------------------------------------

$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"

# Ensure folder exists and clean up if more than 50 files
if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory
}

$fileCount = (Get-ChildItem -Path $logDirectory -File).Count
if ($fileCount -gt 50) {
    $filesToDelete = Get-ChildItem -Path $logDirectory -File |
        Sort-Object CreationTime | Select-Object -First ($fileCount - 50)
    $filesToDelete | Remove-Item -Force
    Write-Host "$($filesToDelete.Count) old log files deleted."
}

# -------------------------------------------------------------
# 0️⃣ API key from your path
# -------------------------------------------------------------
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
if (-not (Test-Path $apikeypath)) { throw "API key file not found at $apikeypath" }
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()
$commonHeaders = @{ "apiKey" = $apiKey }

# -------------------------------------------------------------
# 1️⃣ Get Clusters (ClusterName + ClusterId)
# -------------------------------------------------------------
$url = "https://helios.cohesity.com/v2/mcm/cluster-mgmt/info"
$response = Invoke-WebRequest -Method Get -Uri $url -Headers $commonHeaders
$json_clu = $response.Content | ConvertFrom-Json
$json_clu = $json_clu.cohesityClusters

# -------------------------------------------------------------
# Initialize containers
# -------------------------------------------------------------
$globalFailures = @()
$globalHostDb   = @()

# -------------------------------------------------------------
# 2️⃣ Loop through all clusters and collect protection groups
# -------------------------------------------------------------
foreach ($clus in $json_clu) {

    $cluster_name = $clus.ClusterName
    $cluster_id   = $clus.ClusterId

    $headers = @{
        "apiKey"          = $apiKey
        "accessClusterId" = "$cluster_id"
    }

    $body = @{
        environments = "kOracle"
        isDeleted     = "False"
        isPaused      = "False"
        isActive      = "True"
    }

    $pgUrl = "https://helios.cohesity.com/v2/data-protect/protection-groups"
    $response = Invoke-WebRequest -Method Get -Uri $pgUrl -Headers $headers -Body $body
    $pgResponse = $response.Content | ConvertFrom-Json
    $pgs = $pgResponse.protectionGroups

    foreach ($pg in $pgs) {
        $pgId   = $pg.id
        $pgName = $pg.name

        $headers = @{
            "apiKey"          = $apiKey
            "accessClusterId" = "$cluster_id"
        }

        $body = @{
            environments             = "kOracle"
            isDeleted                = "False"
            isPaused                 = "False"
            isActive                 = "True"
            numRuns                  = "10"
            excludeNonRestorableRuns = "False"
        }

        $runUrl = "https://helios.cohesity.com/v2/data-protect/protection-groups/$pgId/runs"
        $response = Invoke-WebRequest -Method Get -Uri $runUrl -Headers $headers -Body $body
        $json = $response | ConvertFrom-Json

        if ($null -eq $json -or -not $json.runs) { continue }
        $runs = $json.runs

        # ---------------------------------------------------------
        # Flatten all run-level backup info
        # ---------------------------------------------------------
        $flatRuns = @()
        foreach ($run in $runs) {
            if ($run.localBackupInfo) {
                foreach ($info in $run.localBackupInfo) {
                    $flatRuns += [pscustomobject]@{
                        RunType          = $info.runType
                        Status           = $info.status
                        Message          = $info.messages
                        StartTimeUsecs   = $info.startTimeUsecs
                        EndTimeUsecs     = $info.endTimeUsecs
                        Cluster          = $cluster_name
                        ProtectionGroup  = $pgName
                    }
                }
            }

            # -----------------------------------------------------
            # 🔹 NEW: Extract Host–DB mapping from run.objects
            # -----------------------------------------------------
            if ($run.objects) {
                $objs  = $run.objects
                $hosts = $objs | Where-Object { $_.object.environment -eq 'kPhysical' }
                $dbs   = $objs | Where-Object { $_.object.environment -eq 'kDatabase' }

                $hostDbMappings = foreach ($db in $dbs) {
                    $host = $hosts | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                    if ($host) {
                        [pscustomobject]@{
                            Cluster       = $cluster_name
                            ProtectionJob = $pgName
                            HostName      = $host.object.name
                            DatabaseName  = $db.object.name
                        }
                    }
                }

                if ($hostDbMappings) { $globalHostDb += $hostDbMappings }
            }
        }

        # Skip if no runs
        if ($flatRuns.Count -eq 0) { continue }

        # ---------------------------------------------------------
        # Convert Epoch to UTC helper
        # ---------------------------------------------------------
        function Convert-ToUtcFromEpoch($v) {
            if ($null -eq $v -or $v -eq 0) { return $null }
            try {
                return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime
            } catch {
                return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime
            }
        }

        # ---------------------------------------------------------
        # Group by RunType and detect last failure without success
        # ---------------------------------------------------------
        $grouped = $flatRuns | Group-Object RunType
        foreach ($g in $grouped) {
            $latestFailed = $g.Group | Where-Object { $_.Status -eq "Failed" } |
                Sort-Object EndTimeUsecs -Descending | Select-Object -First 1
            if ($null -eq $latestFailed) { continue }

            $hasLaterSuccess = $g.Group | Where-Object {
                $_.Status -eq "Succeeded" -and $_.StartTimeUsecs -gt $latestFailed.EndTimeUsecs
            }

            if (-not $hasLaterSuccess) {
                $estZone  = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
                $startUtc = Convert-ToUtcFromEpoch $latestFailed.StartTimeUsecs
                $endUtc   = Convert-ToUtcFromEpoch $latestFailed.EndTimeUsecs

                $globalFailures += [pscustomobject]@{
                    Cluster         = $latestFailed.Cluster
                    ProtectionGroup = $latestFailed.ProtectionGroup
                    RunType         = $latestFailed.RunType
                    Status          = $latestFailed.Status
                    Message         = $latestFailed.Message -join ' '
                    StartTime       = if ($startUtc) { [System.TimeZoneInfo]::ConvertTimeFromUtc($startUtc, $estZone) } else { $null }
                    EndTime         = if ($endUtc)   { [System.TimeZoneInfo]::ConvertTimeFromUtc($endUtc, $estZone) } else { $null }
                }
            }
        }
    }
}

# -------------------------------------------------------------
# Print backup failures summary
# -------------------------------------------------------------
if ($globalFailures.Count -gt 0) {
    Write-Host "`n🔥 Backup Failures Without Later Success Across All Clusters ---`n"
    $globalFailures |
        Sort-Object Cluster, EndTime -Descending |
        Format-Table Cluster, StartTime, EndTime, ProtectionGroup, RunType, Status, Message -AutoSize

    # Export failures to CSV
    $reportDate = Get-Date -Format "yyyy-MM-dd_HHmm"
    $excelFile = "$logDirectory\BackupFailures_AllClusters_ORA_$reportDate.csv"
    $globalFailures | Export-Csv -Path $excelFile -NoTypeInformation -Encoding UTF8
    Write-Host "`n✅ Saved failures CSV report to $excelFile"
} else {
    Write-Host "`n✅ No failed backups without later success found."
}

# -------------------------------------------------------------
# Export Host–DB Mapping Summary
# -------------------------------------------------------------
if ($globalHostDb.Count -gt 0) {
    Write-Host "`n💾 Host–Database Mapping Summary ---`n"
    $globalHostDb | Format-Table Cluster, ProtectionJob, HostName, DatabaseName -AutoSize

    $csvPath = "$logDirectory\Host_DB_Mapping.csv"
    $globalHostDb | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
    Write-Host "✅ Saved Host–DB mapping to $csvPath"
} else {
    Write-Host "`n⚠️ No Host–Database mapping details found."
}
