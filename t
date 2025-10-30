# -------------------------------------------------------------
# Cohesity Oracle – Diagnostic: List messages from failed runs
# -------------------------------------------------------------

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
$tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")

# --- Get Oracle PGs ---
$pgResp = Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups" -Headers $headers -Body @{
    environments = "kOracle"; isDeleted = "False"; isPaused = "False"; isActive = "True"
} -Method Get
$pgs = ($pgResp.Content | ConvertFrom-Json).protectionGroups

$results = @()

foreach ($pg in $pgs) {
    $pgId   = $pg.id
    $pgName = $pg.name

    $runResp = Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups/$pgId/runs" -Headers $headers -Body @{
        environments             = "kOracle"
        isDeleted                = "False"
        isPaused                 = "False"
        isActive                 = "True"
        numRuns                  = "10"
        excludeNonRestorableRuns = "False"
    } -Method Get

    $json = $runResp | ConvertFrom-Json
    if (-not $json.runs) { continue }

    $runs = $json.runs | Sort-Object { $_.localBackupInfo[0].startTimeUsecs }

    foreach ($run in $runs) {
        foreach ($info in $run.localBackupInfo) {
            if ($info.status -eq "Failed") {

                $runType = $info.runType
                $runStart = Convert-ToUtcFromEpoch $info.startTimeUsecs
                $runEnd   = Convert-ToUtcFromEpoch $info.endTimeUsecs

                $messages = $info.messages
                if ($messages) {
                    foreach ($msg in $messages) {
                        $isDbDiscoveryFailure = $msg -match "Failed to discover databases"

                        $results += [pscustomobject]@{
                            Cluster         = $cluster_name
                            ProtectionGroup = $pgName
                            RunType         = $runType
                            StartTime       = [System.TimeZoneInfo]::ConvertTimeFromUtc($runStart, $tz)
                            EndTime         = [System.TimeZoneInfo]::ConvertTimeFromUtc($runEnd, $tz)
                            Message         = $msg
                            Matched         = if ($isDbDiscoveryFailure) { "✅ Yes" } else { "No" }
                        }
                    }
                }
            }
        }
    }
}

# --- Output results ---
if ($results.Count -gt 0) {
    Write-Host "`n📋 Messages from failed runs (highlighting discovery errors):`n"
    $results | Sort-Object ProtectionGroup, StartTime |
        Format-Table ProtectionGroup, RunType, StartTime, EndTime, Matched, Message -AutoSize
} else {
    Write-Host "`n✅ No failed runs or messages found."
}

