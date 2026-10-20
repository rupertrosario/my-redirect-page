# Cohesity Helios AHV V2 raw-response diagnostic
# GET-only; PowerShell 5.1; based on commit 39029f63440f25d47aa43cab7475c76ec485e5d2
# Raw responses may contain sensitive configuration. Keep output local; do not commit it.
[CmdletBinding()]
param([string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready\AHV_Raw_Diagnostic")

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
    if (-not $Uri.StartsWith("$baseUrl/",[StringComparison]::OrdinalIgnoreCase)) { throw "Non-Helios URI blocked" }
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
$menu | Format-Table -AutoSize
Write-Host "[X] Exit"
do {
    $selection = Read-Host "Select ONE AHV cluster (1-$($menu.Count)), or X"
    if ($selection -match '^[xX]$') { return }
    $number = 0
    $valid = [int]::TryParse($selection,[ref]$number) -and $number -ge 1 -and $number -le $menu.Count
} until ($valid)
$cluster = $clusters[$number-1]
$headers = New-Headers -ClusterId $cluster.ClusterId
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dir = Join-Path $OutputDirectory ("AHV_{0}_{1}" -f ($cluster.ClusterId -replace '[^a-zA-Z0-9_-]','_'),$timestamp)
New-Item -Path $dir -ItemType Directory -Force | Out-Null
$cookie = ""
$page = 0
$found = $false
$seenCookies = @{}
do {
    $uri = "$baseUrl/v2/data-protect/protection-groups" +
           "?environments=kAcropolis" +
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
    # Save exactly the HTTP response text BEFORE ConvertFrom-Json or ConvertTo-Json.
    $raw = Get-RawResponse -Uri $uri -Headers $headers
    if ([string]::IsNullOrWhiteSpace($raw)) { throw "Empty AHV response on page $page" }
    $file = Join-Path $dir ("AHV_V2_Raw_Page_{0:D3}.json" -f $page)
    [System.IO.File]::WriteAllText($file,$raw,(New-Object System.Text.UTF8Encoding($false)))
    $hit = $raw.Contains("backupDirectlyAttachedVolumeGroups")
    if ($hit) { $found = $true }
    Write-Host ("Page {0}: property {1} | {2}" -f $page,$(if ($hit) {"PRESENT"} else {"ABSENT"}),$file)
    # Parsing is only for pagination, never for the written raw response.
    $parsed = $raw | ConvertFrom-Json
    $nextCookie = First-Value @($parsed.paginationCookie)
    if ($nextCookie -and $seenCookies.ContainsKey($nextCookie)) { throw "Repeated paginationCookie: stopped to avoid loop." }
    if ($nextCookie) { $seenCookies[$nextCookie] = $true }
    $cookie = $nextCookie
    if ($parsed.isResponseTruncated -eq $true -and -not $cookie) {
        throw "Response truncated without paginationCookie; diagnostic incomplete."
    }
} while ($cookie)
Write-Host ""
Write-Host ("RESULT: backupDirectlyAttachedVolumeGroups {0} in raw Helios HTTP response ({1} page(s))." -f $(if ($found) {"PRESENT"} else {"ABSENT"}),$page) -ForegroundColor Cyan
Write-Host "Raw output: $dir"
Write-Host "Do not upload raw files to GitHub; inspect for sensitive fields before sharing." -ForegroundColor Yellow
