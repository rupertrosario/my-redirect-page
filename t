# -------------------------------------------------------------
# Cohesity Oracle Failures – Unified Report (Helios)
# Only unresolved failures enriched with Host & DB mapping
# -------------------------------------------------------------

$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"

# --- Ensure log folder exists and clean old logs ---
if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory | Out-Null
}

$fileCount = (Get-ChildItem -Path $logDirectory -File).Count
if ($fileCount -gt 50) {
    $filesToDelete = Get-ChildItem -Path $logDirectory -File |
        Sort-Object CreationTime | Select-Object -First ($fileCount - 50)
    $filesToDelete | Remove-Item -Force
    Write-Host "$($filesToDelete.Count) old log files deleted."
}

# --- Load API key ---
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

# --- Get cluster list from Helios ---
$baseUrl = "https://helios.cohesity.com"
$commonHeaders = @{ "apiKey" = $apiKey }

$clusterUrl = "$baseUrl/v2/mcm/cluster-mgmt/info"
$response = Invoke-WebRequest -Method Get -Uri $clusterUrl -Headers $commonHeaders
$json_clu = ($response.Content | ConvertFrom-Json).cohesityClusters

$globalFailures = @()

# --- Epoch conversion helper ---
function Convert-ToUtcFromEpoch($v) {
    if ($null -eq $v -or $v -eq 0) { return $null }
    try {
        return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$v).UtcDateTime
    } catch {
        return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$v / 1000)).UtcDateTime
    }
}

# --- Process all clusters ---
foreach ($clus in $json_clu) {

    $cluster_name = $clus.ClusterName
    $cluster_id   = $clus.ClusterId

    $headers = @{
        "apiKey"          = $apiKey
        "accessClusterId" = "$cluster_id"
    }

    $pgUrl = "$baseUrl/v2/data-protect/protection-groups"
    $body = @{
        environments = "kOracle"
        isDeleted     = "False"
        isPaused      = "False"
        isActive      = "True"
    }

    $pgResponse = Invoke-WebRequest -Method Get -Uri $pgUrl -Headers $headers -Body $body
    $pgs = ($pgResponse.Content | ConvertFrom-Json).protectionGroups

    foreach ($pg in $pgs) {

        $pgId   = $pg.id
        $pgName = $pg.name

        $runUrl = "$baseUrl/v2/data-protect/protection-groups/$pgId/runs"
        $body = @{
            environments             = "kOracle"
            isDeleted                = "False"
            isPaused                 = "False"
            isActive                 = "True"
            numRuns                  = "10"
            excludeNonRestorableRuns = "False"
        }

        $runResponse = Invoke-WebRequest -Method Get -Uri $runUrl -Headers $headers -Body $body
        $json = $runResponse | ConvertFrom-Json

        if ($null -eq $json -or -not $json.runs) {
            continue
        }

        $runs = $json.runs

        # --- Loop each run and analyze failures ---
        foreach ($run in $runs) {

            if ($null -eq $run.localBackupInfo) { continue }

            foreach ($info in $run.localBackupInfo) {

                # Step 1: Only consider failed runTypes
                if ($info.status -ne "Failed") { continue }

                # Step 2: Check if later success exists
                $hasLaterSuccess = $false
                foreach ($r2 in $runs) {
                    if ($r2.localBackupInfo) {
                        foreach ($i2 in $r2.localBackupInfo) {
                            if (
                                $i2.runType -eq $info.runType -and
                                $i2.status -eq "Succeeded" -and
                                $i2.startTimeUsecs -gt $info.endTimeUsecs
                            ) {
                                $hasLaterSuccess = $true
                                break
                            }
                        }
                    }
                    if ($hasLaterSuccess) { break }
                }

                if ($hasLaterSuccess) { continue }  # skip resolved failures

                # Step 3: Extract Host–DB mapping for unresolved failure
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

                # Step 4: Record unresolved failure
                $estZone  = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
                $startUtc = Convert-ToUtcFromEpoch $info.startTimeUsecs
                $endUtc   = Convert-ToUtcFromEpoch $info.endTimeUsecs

                $globalFailures += [pscustomobject]@{
                    Cluster         = $cluster_name
                    ProtectionGroup = $pgName
                    HostName        = $hostName
                    DatabaseName    = $dbName
                    RunType         = $info.runType
                    Status          = $info.status
                    Message         = ($info.messages -join " ")
                    StartTime       = if ($startUtc) { [System.TimeZoneInfo]::ConvertTimeFromUtc($startUtc, $estZone) } else { $null }
                    EndTime         = if ($endUtc)   { [System.TimeZoneInfo]::ConvertTimeFromUtc($endUtc, $estZone) } else { $null }
                }
            } # end foreach $info
        } # end foreach $run
    } # end foreach PG
} # end foreach cluster

# --- Output results ---
if ($globalFailures.Count -gt 0) {
    Write-Host "`n🔥 Unresolved Backup Failures (with Host–DB Mapping) ---`n"
    $globalFailures |
        Sort-Object Cluster, EndTime -Descending |
        Format-Table Cluster, ProtectionGroup, HostName, DatabaseName, RunType, Status, Message, StartTime, EndTime -AutoSize

    $reportDate = Get-Date -Format "yyyy-MM-dd_HHmm"
    $csvFile = "$logDirectory\Cohesity_ORA_Unresolved_Failures_$reportDate.csv"
    $globalFailures | Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8
    Write-Host "`n✅ Saved unified report to $csvFile"
}
else {
    Write-Host "`n✅ No unresolved failures found."
}
