# =====================================================================
# Cohesity Protection Group Status Inventory - Multi-Cluster via Helios
# READ-ONLY / GET-only
#
# Output:
# - ClusterName
# - Environment
# - ProtectionGroupName
# - Status: Active / Paused / Deleted
#
# Menu:
# 0 = All clusters
# S = Single cluster
# M = Multiple clusters
# X = Exit
#
# CSV:
# X:\PowerShell\Data\Cohesity\ProtectionGroups
# =====================================================================

$ErrorActionPreference = "Stop"

# -------------------------------------------------------------
# 0) API key - encrypted file supported
# -------------------------------------------------------------
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKeyFileText = (Get-Content -Path $apikeypath -Raw).Trim()

try {
    $secureApiKey = $apiKeyFileText | ConvertTo-SecureString -ErrorAction Stop
    $apiKey = [System.Net.NetworkCredential]::new("", $secureApiKey).Password
}
catch {
    # Backward-compatible fallback for an existing plain-text key file.
    $apiKey = $apiKeyFileText
}

$baseUrl = "https://helios.cohesity.com"

$commonHeaders = @{
    apiKey = $apiKey
    accept = "application/json"
}

# -------------------------------------------------------------
# GET-only wrapper
# -------------------------------------------------------------
function Invoke-HeliosGetJson {
    param(
        [Parameter(Mandatory)] [string] $Uri,
        [Parameter(Mandatory)] [hashtable] $Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

# -------------------------------------------------------------
# Environment helper
# -------------------------------------------------------------
function Get-PgEnvironment {
    param(
        [Parameter(Mandatory)] $ProtectionGroup
    )

    $environment = $null

    if ($ProtectionGroup.PSObject.Properties["environment"]) {
        $environment = $ProtectionGroup.environment
    }
    elseif ($ProtectionGroup.PSObject.Properties["environmentType"]) {
        $environment = $ProtectionGroup.environmentType
    }
    elseif ($ProtectionGroup.PSObject.Properties["environments"]) {
        $environment = @($ProtectionGroup.environments)[0]
    }

    if ([string]::IsNullOrWhiteSpace([string]$environment)) {
        return "Unknown"
    }

    $environment = [string]$environment

    if ($environment.StartsWith("k") -and $environment.Length -gt 1) {
        return $environment.Substring(1)
    }

    return $environment
}

# -------------------------------------------------------------
# 1) Get clusters
# -------------------------------------------------------------
try {
    $clusterJson = Invoke-HeliosGetJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
    $json_clu = @($clusterJson.cohesityClusters)
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

if (-not $json_clu -or $json_clu.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$sorted = $json_clu | Sort-Object -Property clusterName

$clusters = for ($i = 0; $i -lt $sorted.Count; $i++) {
    [pscustomobject]@{
        Index       = $i + 1
        ClusterName = $sorted[$i].clusterName
        ClusterId   = $sorted[$i].clusterId
    }
}

Write-Host ""
Write-Host "Available Helios Clusters (sorted by name):" -ForegroundColor Cyan
$clusters | Format-Table -AutoSize

Write-Host ""
Write-Host "[0] All clusters" -ForegroundColor Yellow
Write-Host "[S] Single cluster" -ForegroundColor Yellow
Write-Host "[M] Multiple clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow
Write-Host ""

# -------------------------------------------------------------
# 2) Cluster selection
# -------------------------------------------------------------
$mode = $null
$SelectedClusters = @()

while ($true) {
    $choice = Read-Host "Choose 0 / S / M / X"

    if ($choice -match '^(x|X|q|Q)$') {
        Write-Host "Exit selected. No clusters chosen." -ForegroundColor Cyan
        return
    }

    if ($choice -eq "0") {
        $mode = "ALL"
        $SelectedClusters = $clusters
        break
    }

    if ($choice -match '^(s|S)$') {
        $idx = Read-Host "Enter single cluster index (1-$($clusters.Count))"
        [int]$n = 0

        if (-not [int]::TryParse($idx, [ref]$n) -or $n -lt 1 -or $n -gt $clusters.Count) {
            Write-Host "Invalid index." -ForegroundColor Red
            continue
        }

        $mode = "SINGLE"
        $SelectedClusters = @($clusters | Where-Object { $_.Index -eq $n })
        break
    }

    if ($choice -match '^(m|M)$') {
        $list = Read-Host "Enter indices separated by comma (example: 1,4,9)"
        $parts = $list -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }

        $nums = @()
        $bad = $false

        foreach ($p in $parts) {
            [int]$n = 0

            if (-not [int]::TryParse($p, [ref]$n) -or $n -lt 1 -or $n -gt $clusters.Count) {
                $bad = $true
                break
            }

            $nums += $n
        }

        if ($bad -or $nums.Count -eq 0) {
            Write-Host "Invalid list. Use indices like 1,4,9" -ForegroundColor Red
            continue
        }

        $mode = "MULTI"
        $nums = $nums | Select-Object -Unique | Sort-Object

        $SelectedClusters = foreach ($n in $nums) {
            $clusters | Where-Object { $_.Index -eq $n }
        }

        break
    }

    Write-Host "Invalid option. Choose 0 / S / M / X" -ForegroundColor Red
}

Write-Host ""
Write-Host "Selected Mode: $mode" -ForegroundColor Green
Write-Host "Selected Clusters:" -ForegroundColor Green
$SelectedClusters | Select-Object Index, ClusterName | Format-Table -AutoSize

# -------------------------------------------------------------
# 3) Protection Group status queries
#
# IMPORTANT:
# These are GET requests only.
# Status is assigned from the explicit API query scope.
# -------------------------------------------------------------
$statusQueries = @(
    [pscustomobject]@{
        Status = "Active"
        Query  = "isDeleted=false&isPaused=false&isActive=true"
    }
    [pscustomobject]@{
        Status = "Paused"
        Query  = "isDeleted=false&isPaused=true"
    }
    [pscustomobject]@{
        Status = "Deleted"
        Query  = "isDeleted=true"
    }
)

$AllRows = @()

foreach ($cluster in $SelectedClusters) {
    $cluster_name = $cluster.ClusterName
    $cluster_id   = $cluster.ClusterId

    Write-Host ""
    Write-Host "Processing cluster: $cluster_name" -ForegroundColor Cyan

    $headers = @{
        apiKey          = $apiKey
        accessClusterId = $cluster_id
        accept          = "application/json"
    }

    foreach ($statusQuery in $statusQueries) {
        $status = $statusQuery.Status
        $pgUri  = "$baseUrl/v2/data-protect/protection-groups?$($statusQuery.Query)"

        try {
            $pgJson = Invoke-HeliosGetJson -Uri $pgUri -Headers $headers
            $pgs = @($pgJson.protectionGroups | Where-Object { $null -ne $_ })
        }
        catch {
            Write-Host "  Failed to query $status PGs: $($_.Exception.Message)" -ForegroundColor Yellow
            continue
        }

        Write-Host ("  {0,-7}: {1}" -f $status, $pgs.Count) -ForegroundColor Gray

        foreach ($pg in $pgs) {
            $pgName = [string]$pg.name

            if ([string]::IsNullOrWhiteSpace($pgName)) {
                continue
            }

            $AllRows += [pscustomobject]@{
                ClusterName         = $cluster_name
                Environment         = Get-PgEnvironment -ProtectionGroup $pg
                ProtectionGroupName = $pgName
                Status              = $status
            }
        }
    }
}

# -------------------------------------------------------------
# 4) Remove duplicates / sort
# -------------------------------------------------------------
$AllRows = @(
    $AllRows |
        Sort-Object ClusterName, Environment, ProtectionGroupName, Status -Unique
)

if (-not $AllRows -or $AllRows.Count -eq 0) {
    Write-Host ""
    Write-Host "No protection groups returned for the selected cluster(s)." -ForegroundColor Yellow
    return
}

# ----------------------------------------------------------------
# 5) Console output
# -------------------------------------------------------------
Write-Host ""
Write-Host "=== Protection Group Status Inventory ===" -ForegroundColor Cyan
Write-Host ""

$AllRows |
    Format-Table ClusterName, Environment, ProtectionGroupName, Status -AutoSize

# -------------------------------------------------------------
# 6) Summary
# -------------------------------------------------------------
Write-Host ""
Write-Host "=== Summary ===" -ForegroundColor Cyan
Write-Host ""

$AllRows |
    Group-Object ClusterName, Status |
    ForEach-Object {
        [pscustomobject]@{
            ClusterStatus = $_.Name
            Count         = $_.Count
        }
    } |
    Format-Table -AutoSize

# -------------------------------------------------------------
# 7) CSV export
# -------------------------------------------------------------
$csvDir = "X:\PowerShell\Data\Cohesity\ProtectionGroups"

if (-not (Test-Path $csvDir)) {
    New-Item -ItemType Directory -Path $csvDir | Out-Null
}

$reportDate = Get-Date -Format "yyyy-MM-dd_HHmm"

$suffix = switch ($mode) {
    "ALL"    { "ALL" }
    "MULTI"  { "MULTI" }
    "SINGLE" { ($SelectedClusters[0].ClusterName -replace '[^\w\-]', '_') }
    default  { "UNKNOWN" }
}

$csvFile = Join-Path -Path $csvDir -ChildPath "Protection_Group_Status_${suffix}_${reportDate}.csv"

$AllRows |
    Select-Object ClusterName, Environment, ProtectionGroupName, Status |
    Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "CSV written: $csvFile" -ForegroundColor Green
Write-Host ""
Write-Host "READ-ONLY validation: Cohesity API calls in this script use GET only." -ForegroundColor Green
