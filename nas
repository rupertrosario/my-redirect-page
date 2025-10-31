# -------------------------------------------------------------
# Cohesity NAS Backup Failures – Multi-Cluster (Helios)
# Environment: kGenericNas | kIsilon
# ObjectType:  kHost
# -------------------------------------------------------------

$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"
if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory | Out-Null
}

# --- Load API key ---
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
if (-not (Test-Path $apikeypath)) { throw "API key file not found: $apikeypath" }
$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()
$commonHeaders = @{ "apiKey" = $apiKey }

# --- Get all clusters from Helios ---
$url = "https://helios.cohesity.com/v2/mcm/cluster-mgmt/info"
$response = Invoke-WebRequest -Uri $url -Headers $commonHeaders -Method Get
$json_clu = ($response.Content | ConvertFrom-Json).cohesityClusters
if (-not $json_clu) { throw "No clusters returned from Helios." }

# --- Helper for time conversion ---
function Convert-ToLocalFromEpoch($v,$tz){
    if(-not $v -or $v -eq 0){return $null}
    try{
        $utc=[DateTimeOffset]::FromUnixTimeMilliseconds([int64]($v/1000)).UtcDateTime
        if($tz){[System.TimeZoneInfo]::ConvertTimeFromUtc($utc,$tz)}else{$utc.ToLocalTime()}
    }catch{return $null}
}

$tz=[System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
$baseUrl="https://helios.cohesity.com"
$globalFailures=@()

# -------------------------------------------------------------
# Iterate clusters
# -------------------------------------------------------------
foreach($cluster in $json_clu){

    $cluster_name = $cluster.name
    if([string]::IsNullOrWhiteSpace($cluster_name)){$cluster_name=$cluster.clusterName}
    if([string]::IsNullOrWhiteSpace($cluster_name)){$cluster_name=$cluster.displayName}
    if([string]::IsNullOrWhiteSpace($cluster_name)){$cluster_name="Unknown-$($cluster.clusterId)"}
    $cluster_id=$cluster.clusterId

    Write-Host "`n🔹 Processing cluster: $cluster_name" -ForegroundColor Cyan
    $headers=@{apiKey=$apiKey;accessClusterId=$cluster_id}

    # --- Get NAS PGs (both Generic NAS and Isilon) ---
    $pgResp=Invoke-WebRequest -Uri "$baseUrl/v2/data-protect/protection-groups" -Headers $headers -Body @{
        environments="kGenericNas,kIsilon"
        isDeleted="False"
        isPaused="False"
        isActive="True"
    } -Method Get
    $pgs=($pgResp.Content|ConvertFrom-Json).protectionGroups
    if(-not $pgs){Write-Host "⚠️ No NAS PGs found on $cluster_name." -ForegroundColor Yellow;continue}

    foreach($pg in $pgs){
        $pgId=$pg.id
        $pgName=$pg.name
        Write-Host "`n📦 Checking NAS PG: $pgName" -ForegroundColor Yellow

        $runUrl="$baseUrl/v2/data-protect/protection-groups/$pgId/runs"
        $runBody=@{
            environments="kGenericNas,kIsilon"
            numRuns="10"
            excludeNonRestorableRuns="False"
            includeObjectDetails="True"
        }

        try{
            $runResp=Invoke-WebRequest -Uri $runUrl -Headers $headers -Body $runBody -Method Get
            $jsonRuns=$runResp|ConvertFrom-Json
        }catch{
            Write-Host "⚠️ Skipping PG $pgName on $cluster_name due to API error." -ForegroundColor Yellow
            continue
        }

        if(-not $jsonRuns.runs){continue}

        $runs=$jsonRuns.runs|Sort-Object{$_.localBackupInfo[0].endTimeUsecs}-Descending
        $latestRun=$runs|Select-Object -First 1
        if(-not $latestRun){continue}

        $info=$latestRun.localBackupInfo[0]
        $status=$info.status
        $runType=$info.runType
        $startLocal=Convert-ToLocalFromEpoch $info.startTimeUsecs $tz
        $endLocal=Convert-ToLocalFromEpoch $info.endTimeUsecs $tz

        $isFailed = ($status -ne "Succeeded" -and $status -ne "SucceededWithWarning")

        if($latestRun.objects){
            # objectType can vary: kHost, kNetapp, etc.
            $nasObjs=$latestRun.objects|Where-Object{
                ($_.object.environment -in @('kGenericNas','kIsilon'))
            }

            foreach($nas in $nasObjs){
                $attempts=$nas.localSnapshotInfo.failedAttempts
                if($isFailed -or $attempts){
                    if($attempts){
                        foreach($fa in $attempts){
                            $msgClean=($fa.message -replace '[\r\n]+',' ' -replace ',',' ' -replace '"','''').Trim()
                            $globalFailures += [pscustomobject]@{
                                Cluster=$cluster_name
                                ProtectionGroup=$pgName
                                RunType=$runType
                                NASName=$nas.object.name
                                StartTime=$startLocal
                                EndTime=$endLocal
                                FailedMessage=$msgClean
                            }
                        }
                    } else {
                        $globalFailures += [pscustomobject]@{
                            Cluster=$cluster_name
                            ProtectionGroup=$pgName
                            RunType=$runType
                            NASName=$nas.object.name
                            StartTime=$startLocal
                            EndTime=$endLocal
                            FailedMessage="No failedAttempts[] details found — Run marked Failed"
                        }
                    }
                }
            }
        }
    }
}

# -------------------------------------------------------------
# Smart de-duplication (null-safe)
# -------------------------------------------------------------
if ($globalFailures.Count -gt 0) {
    $globalFailures = $globalFailures |
        Group-Object {
            $end = if ($_.EndTime) { $_.EndTime.ToString('yyyy-MM-dd HH:mm') } else { 'N/A' }
            "$($_.Cluster)|$($_.ProtectionGroup)|$($_.RunType)|$($_.NASName)|$($_.FailedMessage)|$end"
        } |
        ForEach-Object {
            $_.Group | Sort-Object EndTime -Descending | Select-Object -First 1
        }
}

# -------------------------------------------------------------
# Output Section
# -------------------------------------------------------------
if($globalFailures.Count -gt 0){
    Write-Host "`n🔥 Latest Failed NAS Runs (All Clusters):`n" -ForegroundColor Cyan
    $sorted=$globalFailures|Sort-Object Cluster,ProtectionGroup,EndTime -Descending
    $sorted|Format-Table Cluster,ProtectionGroup,RunType,NASName,StartTime,EndTime,FailedMessage -AutoSize
}else{
    Write-Host "`n✅ All latest NAS runs succeeded across all clusters." -ForegroundColor Green
}

# -------------------------------------------------------------
# CSV Reference Path (for manual export if needed)
# -------------------------------------------------------------
$timestamp=Get-Date -Format "yyyyMMdd_HHmm"
$csvPath="X:\PowerShell\Data\Cohesity\BackupFailures\BackupFailures_NAS_AllClusters_$timestamp.csv"
Write-Host "`n📂 (CSV path ready if needed later): $csvPath" -ForegroundColor Gray
