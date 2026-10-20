# Cohesity Helios V2 and V1 raw-response diagnostic
# GET-only; PowerShell 5.1; based on commit 39029f63440f25d47aa43cab7475c76ec485e5d2
# Raw responses may contain sensitive configuration. Keep output local; do not commit it.
[CmdletBinding()]
param([string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready\Raw_Diagnostic")

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$baseUrl = "https://helios.cohesity.com"
$root = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath = Join-Path $root "Common\ApiKeyAesHelper.ps1"
$keyFile = Join-Path $root "Common\Secure\cohesity_apikey.enc"
if (-not (Test-Path $helperPath -PathType Leaf)) { throw "Missing API key helper: $helperPath" }
if (-not (Test-Path $keyFile -PathType Leaf)) { throw "Missing encrypted API key file: $keyFile" }
. $helperPath
$apiKey = & Get-CohesityApiKeyFromAes -EncryptedFile $keyFile
if ([string]::IsNullOrWhiteSpace([string]$apiKey)) { throw "API key helper returned an empty value." }

function New-Headers {
    param([string]$ClusterId)
    $h = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) { $h["accessClusterId"] = $ClusterId }
    return $h
}
function Get-RawResponse {
    param([string]$Uri,[hashtable]$Headers)
    if (-not $Uri.StartsWith("$baseUrl/",[StringComparison]::OrdinalIgnoreCase)) { throw "Non-Helios URL blocked" }
    return (Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop).Content
}
function First-Value {
    param($Values)
    foreach ($v in @($Values)) { if ($null -ne $v -and "$v".Trim()) { return [string]$v } }
    return ""
}
$clusterJson = Get-RawResponse -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers) | ConvertFrom-Json
$clusters = @(
    $clusterJson.cohesityClusters | ForEach-Object {
        $id = First-Value @($_.clusterId,$_.id)
        $name = First-Value @($_.clusterName,$_.displayName,$_.name)
        if ($id) { [pscustomobject]@{ ClusterName=$name; ClusterId=$id } }
    } | Sort-Object ClusterName
)
if ($clusters.Count -eq 0) { throw "Helios returned no clusters." }
$menu = for ($i=0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{ Index=$i+1; ClusterName=$clusters[$i].ClusterName; ClusterId=$clusters[$i].ClusterId }
}
Write-Host ""
Write-Host "Available Helios Clusters (sorted):" -ForegroundColor Cyan
$menu | Format-Table -AutoSize
Write-Host "[0] All clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow
while ($true) {
    $choice = Read-Host "Select cluster: 0 for ALL, 1-$($clusters.Count) for single, or X"
    if ($choice -match '^(?i:x|q)$') { return }
    $number=0
    if ([int]::TryParse($choice,[ref]$number) -and $number -ge 0 -and $number -le $clusters.Count) {
        if ($number -eq 0) { $selectedClusters=@($clusters) }
        else { $selectedClusters=@($clusters[$number-1]) }
        break
    }
    Write-Host "Invalid selection." -ForegroundColor Red
}

$EnvironmentMap = @(
    [pscustomobject]@{ Name="NAS"; ApiNames=@("kGenericNas","kIsilon") },
    [pscustomobject]@{ Name="SQL"; ApiNames=@("kSQL") },
    [pscustomobject]@{ Name="Hyper-V"; ApiNames=@("kHyperV") },
    [pscustomobject]@{ Name="Nutanix AHV"; ApiNames=@("kAcropolis") },
    [pscustomobject]@{ Name="Oracle"; ApiNames=@("kOracle") },
    [pscustomobject]@{ Name="Physical"; ApiNames=@("kPhysical") }
)
Write-Host ""
Write-Host "Available Workload Environments:" -ForegroundColor Cyan
for ($i=0; $i -lt $EnvironmentMap.Count; $i++) {
    Write-Host ("[{0}] {1}" -f ($i+1),$EnvironmentMap[$i].Name) -ForegroundColor Yellow
}
Write-Host "[0] All six environments" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow
while ($true) {
    $choice = Read-Host "Select environment: 0 for ALL, 1-6 for single, or X"
    if ($choice -match '^(?i:x|q)$') { return }
    $number=0
    if ([int]::TryParse($choice,[ref]$number) -and $number -ge 0 -and $number -le $EnvironmentMap.Count) {
        if ($number -eq 0) { $selectedEnvironments=@($EnvironmentMap) }
        else { $selectedEnvironments=@($EnvironmentMap[$number-1]) }
        break
    }
    Write-Host "Invalid selection." -ForegroundColor Red
}

foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId
    $timestamp=Get-Date -Format "yyyyMMdd_HHmmss"
    $dir=Join-Path $OutputDirectory ("{0}_{1}_Raw" -f ($cluster.ClusterId -replace '[^a-zA-Z0-9_-]','_'),$timestamp)
    New-Item -Path $dir -ItemType Directory -Force | Out-Null
    foreach ($env in $selectedEnvironments) {
        foreach ($apiEnvironment in $env.ApiNames) {
            $cookie=""
            $page=0
            $found=$false
            $seenCookies=@{}
            do {
                $uri="$baseUrl/v2/data-protect/protection-groups" +
                     "?environments=$([uri]::EscapeDataString($apiEnvironment))" +
                     "&isDeleted=false" +
                     "&isPaused=false" +
                     "&isActive=true" +
                     "&includeTenants=true" +
                     "&includeLastRunInfo=true" +
                     "&pruneSourceIds=false" +
                     "&pruneExcludedSourceIds=false" +
                     "&useCachedData=false" +
                     "&maxResultCount=1000"
                if ($cookie) { $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))" }
                $page++
                $raw=Get-RawResponse -Uri $uri -Headers $headers
                if ([string]::IsNullOrWhiteSpace($raw)) { throw "Empty raw response for $apiEnvironment page $page" }
                $file=Join-Path $dir ("{0}_Page_{1:D3}.json" -f $apiEnvironment,$page)
                [System.IO.File]::WriteAllText($file,$raw,(New-Object System.Text.UTF8Encoding($false)))
                if ($apiEnvironment -eq "kAcropolis") {
                    $hit=$raw.Contains("backupDirectlyAttachedVolumeGroups")
                    if ($hit) { $found=$true }
                    Write-Host ("{0} page {1}: AHV field {2}" -f $cluster.ClusterName,$page,$(if ($hit) {"PRESENT"} else {"ABSENT"}))
                }
                # JSON is parsed only for pagination, after the raw body is saved.
                $parsed=$raw | ConvertFrom-Json
                $nextCookie=First-Value @($parsed.paginationCookie)
                if ($nextCookie -and $seenCookies.ContainsKey($nextCookie)) { throw "Repeated pagination cookie; collection incomplete." }
                if ($nextCookie) { $seenCookies[$nextCookie]=$true }
                $cookie=$nextCookie
                if ($parsed.isResponseTruncated -eq $true -and -not $cookie) {
                    throw "Response truncated without pagination cookie; collection incomplete."
                }
            } while ($cookie)
            if ($apiEnvironment -eq "kAcropolis") {
                Write-Host ("RESULT: {0} | backupDirectlyAttachedVolumeGroups {1} in raw Helios response ({2} pages)" -f $cluster.ClusterName,$(if ($found) {"PRESENT"} else {"ABSENT"}),$page) -ForegroundColor Cyan
            }
            else { Write-Host ("{0} | {1} | {2} raw page(s)" -f $cluster.ClusterName,$apiEnvironment,$page) }

            # Supplemental V1 list request from baseline 39029f...
            # Kept separate from V2 output: do not merge or infer missing properties.
            $v1Uri = "$baseUrl/irisservices/api/v1/public/protectionJobs" +
                     "?environments=$([uri]::EscapeDataString($apiEnvironment))" +
                     "&isDeleted=false" +
                     "&isActive=true" +
                     "&onlyReturnBasicSummary=false" +
                     "&includeLastRunAndStats=false" +
                     "&pruneExcludedSourceIds=false" +
                     "&includeSourceNames=true" +
                     "&allUnderHierarchy=true" +
                     "&maxResultCount=1000"
            try {
                $v1Raw = Get-RawResponse -Uri $v1Uri -Headers $headers
                if ([string]::IsNullOrWhiteSpace($v1Raw)) { throw "Empty V1 response" }
                $v1File = Join-Path $dir ("{0}_V1_ProtectionJobs_Raw.json" -f $apiEnvironment)
                [System.IO.File]::WriteAllText($v1File,$v1Raw,(New-Object System.Text.UTF8Encoding($false)))
                if ($apiEnvironment -eq "kAcropolis") {
                    $v1Hit = $v1Raw.Contains("backupDirectlyAttachedVolumeGroups")
                    Write-Host ("RESULT: {0} | V1 AHV field {1}" -f $cluster.ClusterName,$(if ($v1Hit) {"PRESENT"} else {"ABSENT"})) -ForegroundColor Cyan
                }
                Write-Host ("  V1 Protection Jobs raw response saved: {0}" -f $v1File)
            }
            catch {
                Write-Warning ("V1 GET failed for {0} on {1}: {2}" -f $apiEnvironment,$cluster.ClusterName,$_.Exception.Message)
            }
        }
    }
    Write-Host "Raw output: $dir"
}
Write-Host "CAUTION: Raw JSON may contain sensitive data. Do not commit or share unredacted files." -ForegroundColor Yellow
