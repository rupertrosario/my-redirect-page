# Cohesity Helios - Cluster Selection Test
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
# Purpose: validate Helios authentication, cluster discovery, and accessClusterId selection only.

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$root                = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath          = Join-Path $root "Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = Join-Path $root "Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}

if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function Get-Json {
    param(
        [Parameter(Mandatory=$true)][string]$Uri,
        [Parameter(Mandatory=$true)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $resp -or [string]::IsNullOrWhiteSpace($resp.Content)) {
        return $null
    }

    return ($resp.Content | ConvertFrom-Json)
}

function First-Value {
    param($Values)
    foreach ($v in @($Values)) {
        foreach ($x in @($v)) {
            if ($null -ne $x -and "$x".Trim() -ne "") {
                return "$x"
            }
        }
    }
    return ""
}

function Get-Prop {
    param($Object,[string[]]$Names)
    if ($null -eq $Object) { return $null }

    foreach ($name in $Names) {
        foreach ($p in @($Object.PSObject.Properties)) {
            if ($p.Name -ieq $name) {
                return $p.Value
            }
        }
    }

    return $null
}

$commonHeaders = @{
    accept = "application/json"
    apiKey = $apiKey
}

Write-Host "Connecting to Helios..." -ForegroundColor Cyan
$clusterResponse = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
$rawClusters = @(Get-Prop $clusterResponse @("cohesityClusters"))

if (-not $rawClusters -or $rawClusters.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$clusters = @(
    $rawClusters | ForEach-Object {
        [pscustomobject]@{
            ClusterName = First-Value @(
                (Get-Prop $_ @("clusterName")),
                (Get-Prop $_ @("displayName")),
                (Get-Prop $_ @("name"))
            )
            ClusterId = First-Value @(
                (Get-Prop $_ @("clusterId")),
                (Get-Prop $_ @("id"))
            )
        }
    } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ClusterId) } |
    Sort-Object ClusterName
)

$menu = for ($i = 0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{
        Index       = $i + 1
        ClusterName = $clusters[$i].ClusterName
        ClusterId   = $clusters[$i].ClusterId
    }
}

Write-Host ""
Write-Host "Available Helios Clusters:" -ForegroundColor Cyan
$menu | Format-Table -AutoSize
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host "Select cluster: 1-$($menu.Count), or X"

    if ($selection -match '^(x|X|q|Q)$') {
        return
    }

    $n = 0
    if (-not [int]::TryParse($selection,[ref]$n)) {
        Write-Host "Invalid selection." -ForegroundColor Red
        continue
    }

    if ($n -lt 1 -or $n -gt $menu.Count) {
        Write-Host "Invalid selection." -ForegroundColor Red
        continue
    }

    $selected = $menu[$n - 1]
    break
}

$selectedHeaders = @{
    accept          = "application/json"
    apiKey          = $apiKey
    accessClusterId = $selected.ClusterId
}

Write-Host ""
Write-Host "Selected cluster:" -ForegroundColor Green
[pscustomobject]@{
    ClusterName     = $selected.ClusterName
    ClusterId       = $selected.ClusterId
    HeliosBaseUrl   = $baseUrl
    AccessClusterId = $selectedHeaders.accessClusterId
    TestResult      = "PASS"
} | Format-List

Write-Host "Cluster selection test completed successfully." -ForegroundColor Green
