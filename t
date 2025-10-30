# -------------------------------------------------------------
# Cohesity Oracle Failures – Multi-Cluster (Helios)
# Unified Host–DB–Failure Table (using object.objectType)
# -------------------------------------------------------------

$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"

# Ensure folder exists and cleanup old logs
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
# API Key setup
# -------------------------------------------------------------
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
if (-not (Test-Path $apikeypath)) { throw "API key file not found at $apikeypath" }
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()
$commonHeaders = @{ "apiKey" = $apiKey }

# -------------------------------------------------------------
# Get all clusters from Helios
# -------------------------------------------------------------
$url = "https://helios.cohesity.com/v2/mcm/cluster-mgmt/info"
$response = Invoke-WebRequest -Method Get -Uri $url -Headers $commonHeaders
$json_clu = ($response.Content | ConvertFrom-Json).cohesityClusters

$globalFailures = @()

# -------------------------------------------------------------
# Process all clusters
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
    $pgResponse = Invoke-WebRequest -Method Get -Uri $pgUrl -Headers $headers -Body $body
    $pgs = ($pgResponse.Content | ConvertFrom-Json).protectionGroups

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
        $runResponse = Invoke-WebRequest -Method Get -Uri $runUrl -Headers $headers -Body $body
        $json = $runResponse | ConvertFrom-Json
        if ($null -eq $json -or -not $json.runs) { continue }

        $runs = $json.runs
        $flatRuns = @()

        foreach ($run in $runs) {

            # 🔹 Map Host–DB relationships (if present)
            $hostName = $null
            $dbName   = $null
            if ($run.objects) {
                $objs  = $run.objects
                $hosts = $objs | Where-Object { $_.object.objectType -eq 'kPhysical' }
                $dbs   = $objs | Where-Object { $_.object.objectType -eq 'kDatabase' }

                foreach ($db in $dbs) {
                    $host = $hosts | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
                    if ($host) {
                        $hostName = $host.object.name
                        $dbName   = $db.object.name
                    }
                }
            }

            # 🔹 Capture backup run info
            if ($run.localBackupInfo) {
                foreach ($info in $run.localBackupInfo) {
                    $flatRuns += [pscustomobject]@{
                        Cluster          = $cluster_name
                        ProtectionGroup  = $pgName
                        HostName         = $hostName
                        DatabaseName     = $dbName
                        RunType          = $info.runType
                        Status           = $info.status
                        Message          = ($info.messages -join " ")
                        StartTimeUsecs   = $info.startTimeUsecs
                        EndTimeUsecs     = $info.endTimeUsecs
                    }
                }
            }
        }

        if ($flatRuns.Count -eq 0) { continue }

        # --- Convert Epoch Helper ---
        function Convert-ToUtcFromEpoch($v) {
            if ($null -eq $v -or $v -eq 0) { return $null }
            try {
                return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime
            } catch {
                return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime
            }
        }

        # --- Group by RunType and detect failed without later success ---
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
                    HostName        = $latestFailed.HostName
                    DatabaseName    = $latestFailed.DatabaseName
                    RunType         = $latestFailed.RunType
                    Status          = $latestFailed.Status
                    Message         = $latestFailed.Message
                    StartTime       = if ($startUtc) { [System.TimeZoneInfo]::ConvertTimeFromUtc($startUtc, $estZone) }
                    EndTime         = if ($endUtc)   { [System.TimeZoneInfo]::ConvertTimeFromUtc($endUtc, $estZone) }
                }
            }
        }
    }
}

# -------------------------------------------------------------
# 📊 Unified Output: Failures + Host–DB Info
# -------------------------------------------------------------
if ($globalFailures.Count -gt 0) {
    Write-Host "`n🔥 Backup Failures with Host–DB Mapping ---`n"
    $globalFailures |
        Sort-Object Cluster, EndTime -Descending |
        Format-Table Cluster, ProtectionGroup, HostName, DatabaseName, RunType, Status, Message, StartTime, EndTime -AutoSize

    $reportDate = Get-Date -Format "yyyy-MM-dd_HHmm"
    $csvFile = "$logDirectory\Cohesity_ORA_Failures_WithHostDB_$reportDate.csv"
    $globalFailures | Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8
    Write-Host "`n✅ Saved unified report to $csvFile"
} else {
    Write-Host "`n✅ No failed backups without later success found."
}
