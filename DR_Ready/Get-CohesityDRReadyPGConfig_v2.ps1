# Cohesity DR Ready - Helios V2 Protection Group configuration export
# PowerShell 5.1 | GET-only | Full nested V2 PG properties
# Output: <run>\<cluster>\ProtectionGroups\<workload>.json
# Exported configuration can contain sensitive information. Keep output access restricted.

[CmdletBinding()]
param([string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready")

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
    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) { $headers["accessClusterId"] = $ClusterId }
    return $headers
}

function Get-HeliosJson {
    param([string]$Uri, [hashtable]$Headers)
    if (-not $Uri.StartsWith("$baseUrl/", [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Only Helios requests are allowed."
    }
    $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace($response.Content)) { throw "Empty Helios HTTP response." }
    return ($response.Content | ConvertFrom-Json)
}

function First-Value {
    param($Values)
    foreach ($value in @($Values)) {
        if ($null -ne $value -and -not [string]::IsNullOrWhiteSpace([string]$value)) {
            return [string]$value
        }
    }
    return ""
}

function Select-Number {
    param([string]$Prompt, [int]$Maximum)
    while ($true) {
        $answer = Read-Host $Prompt
        if ($answer -match '^(?i:x|q)$') { return -1 }
        $selected = 0
        if ([int]::TryParse($answer, [ref]$selected) -and $selected -ge 0 -and $selected -le $Maximum) {
            return $selected
        }
        Write-Host "Invalid selection." -ForegroundColor Red
    }
}

function Get-ActiveGroups {
    param([string]$Environment, [hashtable]$Headers)
    $groups = New-Object System.Collections.ArrayList
    $seenCookies = @{}
    $seenIds = @{}
    $cookie = ""
    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups" +
               "?environments=$([uri]::EscapeDataString($Environment))" +
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

        $response = Get-HeliosJson -Uri $uri -Headers $Headers
        if ($null -eq $response -or $null -eq $response.protectionGroups) {
            throw "Unexpected V2 response: protectionGroups missing for $Environment."
        }
        foreach ($group in @($response.protectionGroups)) {
            if ($null -eq $group) { continue }
            # Keep only explicitly active PGs. Missing status flags are not assumed safe.
            if ($group.isDeleted -ne $false -or $group.isPaused -ne $false -or $group.isActive -ne $true) {
                continue
            }
            $id = [string]$group.id
            if ([string]::IsNullOrWhiteSpace($id)) { throw "PG without id in $Environment; cannot verify completeness." }
            if ($seenIds.ContainsKey($id)) { throw "Duplicate PG id $id in $Environment; export halted." }
            $seenIds[$id] = $true
            [void]$groups.Add($group)
        }

        $nextCookie = First-Value @($response.paginationCookie)
        if ($response.isResponseTruncated -eq $true -and -not $nextCookie) {
            throw "Truncated $Environment response without paginationCookie."
        }
        if ($nextCookie -and $seenCookies.ContainsKey($nextCookie)) {
            throw "Repeated paginationCookie for $Environment."
        }
        if ($nextCookie) { $seenCookies[$nextCookie] = $true }
        $cookie = $nextCookie
    } while ($cookie)

    # Unary comma preserves empty and single-element arrays across function output.
    return ,@($groups.ToArray())
}

function Save-JsonArray {
    param([object[]]$Groups, [string]$Path)
    $json = ConvertTo-Json -InputObject $Groups -Depth 100
    $null = ConvertFrom-Json -InputObject $json -ErrorAction Stop
    if (-not $json.TrimStart().StartsWith("[")) { throw "Expected JSON array: $Path" }
    [System.IO.File]::WriteAllText($Path, $json, (New-Object System.Text.UTF8Encoding($false)))
}

$environments = @(
    [pscustomobject]@{ Name="NAS";          Types=@("kGenericNas","kIsilon"); File="NAS.json" },
    [pscustomobject]@{ Name="SQL";          Types=@("kSQL"); File="SQL.json" },
    [pscustomobject]@{ Name="Hyper-V";      Types=@("kHyperV"); File="HyperV.json" },
    [pscustomobject]@{ Name="Nutanix AHV";  Types=@("kAcropolis"); File="AHV.json" },
    [pscustomobject]@{ Name="Oracle";       Types=@("kOracle"); File="Oracle.json" },
    [pscustomobject]@{ Name="Physical";     Types=@("kPhysical"); File="Physical.json" }
)

$clusterResponse = Get-HeliosJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$clusters = @(
    $clusterResponse.cohesityClusters | ForEach-Object {
        $id = First-Value @($_.clusterId, $_.id)
        $name = First-Value @($_.clusterName, $_.displayName, $_.name)
        if ($id) {
            if (-not $name) { $name = "Unknown-$id" }
            [pscustomobject]@{ ClusterName=$name; ClusterId=$id }
        }
    } | Sort-Object ClusterName
)
if ($clusters.Count -eq 0) { throw "No clusters returned by Helios." }

Write-Host ""
Write-Host "Available Helios Clusters:" -ForegroundColor Cyan
for ($i = 0; $i -lt $clusters.Count; $i++) {
    Write-Host ("[{0}] {1}" -f ($i + 1), $clusters[$i].ClusterName)
}
Write-Host "[0] All clusters"
Write-Host "[X] Exit"
$clusterChoice = Select-Number -Prompt "Select cluster" -Maximum $clusters.Count
if ($clusterChoice -lt 0) { return }
if ($clusterChoice -eq 0) { $selectedClusters = @($clusters) }
else { $selectedClusters = @($clusters[$clusterChoice - 1]) }

Write-Host ""
Write-Host "Available Workload Environments:" -ForegroundColor Cyan
for ($i = 0; $i -lt $environments.Count; $i++) {
    Write-Host ("[{0}] {1}" -f ($i + 1), $environments[$i].Name)
}
Write-Host "[0] All six environments"
Write-Host "[X] Exit"
$environmentChoice = Select-Number -Prompt "Select environment" -Maximum $environments.Count
if ($environmentChoice -lt 0) { return }
if ($environmentChoice -eq 0) { $selectedEnvironments = @($environments) }
else { $selectedEnvironments = @($environments[$environmentChoice - 1]) }

$runFolder = Join-Path $OutputDirectory ("PG_Config_V2_{0}" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -Path $runFolder -ItemType Directory -Force | Out-Null

foreach ($cluster in $selectedClusters) {
    $clusterFolderName = ("{0}_{1}" -f ($cluster.ClusterName -replace '[\\/:*?"<>|]', '_'), $cluster.ClusterId)
    $pgFolder = Join-Path (Join-Path $runFolder $clusterFolderName) "ProtectionGroups"
    New-Item -Path $pgFolder -ItemType Directory -Force | Out-Null
    $headers = New-Headers -ClusterId $cluster.ClusterId
    Write-Host ("Cluster: {0}" -f $cluster.ClusterName) -ForegroundColor Cyan

    foreach ($environment in $selectedEnvironments) {
        $all = New-Object System.Collections.ArrayList
        foreach ($apiType in $environment.Types) {
            $collected = Get-ActiveGroups -Environment $apiType -Headers $headers
            foreach ($pg in $collected) { [void]$all.Add($pg) }
        }
        $destination = Join-Path $pgFolder $environment.File
        Save-JsonArray -Groups ([object[]]$all.ToArray()) -Path $destination
        Write-Host ("  {0}: {1} active PGs -> {2}" -f $environment.Name, $all.Count, $environment.File)
    }
}
Write-Host ("Export complete: {0}" -f $runFolder) -ForegroundColor Green
Write-Host "Treat exported JSON as sensitive configuration; do not commit it to Git." -ForegroundColor Yellow
