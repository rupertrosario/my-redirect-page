# Cohesity Helios SSO / Identity Provider Inventory
# Multi-cluster | Helios | GET-only | PowerShell 5.1 compatible

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\SSOInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}
if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found at $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found at $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function New-Headers {
    param([string]$ClusterId)

    $headers = @{
        accept = "application/json"
        apiKey = $apiKey
    }

    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers["accessClusterId"] = $ClusterId
    }

    return $headers
}

function Get-Json {
    param(
        [Parameter(Mandatory)] [string]$Uri,
        [Parameter(Mandatory)] [hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Convert-ToDisplayValue {
    param($Value)

    $items = @(
        As-Array $Value |
            ForEach-Object {
                if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                    ([string]$_).Trim()
                }
            }
    )

    if (@($items).Count -eq 0) { return "N/A" }
    return (($items | Select-Object -Unique) -join "; ")
}

function First-Property {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object -or $Object -is [string]) { return "N/A" }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                $value = Convert-ToDisplayValue $property.Value
                if ($value -ne "N/A") { return $value }
            }
        }
    }

    return "N/A"
}

function Get-IdpArray {
    param($Response)

    if ($null -eq $Response) { return @() }

    $idpsProperty = $Response.PSObject.Properties["idps"]
    if ($null -ne $idpsProperty -and $null -ne $idpsProperty.Value) {
        return @($idpsProperty.Value | Where-Object { $null -ne $_ })
    }

    if ($Response -is [System.Array]) {
        return @($Response | Where-Object { $null -ne $_ })
    }

    return @($Response)
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   COHESITY SSO CONFIGURATION INVENTORY" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan

try {
    $clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

$clusters = @()
if ($null -ne $clusterJson) {
    if ($clusterJson.cohesityClusters) { $clusters = @($clusterJson.cohesityClusters) }
    elseif ($clusterJson.clusters) { $clusters = @($clusterJson.clusters) }
    elseif ($clusterJson.clusterInfos) { $clusters = @($clusterJson.clusterInfos) }
    elseif ($clusterJson.mcmInfo -and $clusterJson.mcmInfo.clusterInfos) { $clusters = @($clusterJson.mcmInfo.clusterInfos) }
}

if (@($clusters).Count -eq 0) {
    throw "No clusters were returned from Helios."
}

$clusters = @($clusters | Sort-Object { First-Property $_ @("clusterName", "displayName", "name") })
$rows = @()
$issues = @()

foreach ($cluster in $clusters) {
    $clusterName = First-Property $cluster @("clusterName", "displayName", "name")
    $clusterId   = First-Property $cluster @("clusterId", "id")

    if ($clusterName -eq "N/A") { $clusterName = "Unknown" }

    if ($clusterId -eq "N/A") {
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Cluster ID missing"
        }
        continue
    }

    Write-Host "Querying SSO configuration for $clusterName..." -ForegroundColor Yellow

    try {
        $idpResponse = Get-Json -Uri "$baseUrl/v2/mcm/idps" -Headers (New-Headers -ClusterId $clusterId)
        $idps = @(Get-IdpArray -Response $idpResponse)
    }
    catch {
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = $_.Exception.Message
        }
        continue
    }

    if (@($idps).Count -eq 0) {
        $rows += [pscustomobject][ordered]@{
            Cluster              = $clusterName
            ClusterId            = $clusterId
            SSOConfigured        = "No"
            IdentityProviderName = "N/A"
            Enabled              = "N/A"
            Domain               = "N/A"
            IssuerId             = "N/A"
            SSOUrl               = "N/A"
            DefaultClusters      = "N/A"
            DefaultRoles         = "N/A"
            Id                   = "N/A"
        }
        continue
    }

    foreach ($idp in $idps) {
        $rows += [pscustomobject][ordered]@{
            Cluster              = $clusterName
            ClusterId            = $clusterId
            SSOConfigured        = "Yes"
            IdentityProviderName = Convert-ToDisplayValue $idp.name
            Enabled              = if ($null -eq $idp.isEnabled) { "N/A" } elseif ($idp.isEnabled) { "Yes" } else { "No" }
            Domain               = Convert-ToDisplayValue $idp.domain
            IssuerId             = Convert-ToDisplayValue $idp.issuerId
            SSOUrl               = Convert-ToDisplayValue $idp.ssoUrl
            DefaultClusters      = Convert-ToDisplayValue $idp.defaultClusters
            DefaultRoles         = Convert-ToDisplayValue $idp.defaultRoles
            Id                   = Convert-ToDisplayValue $idp.id
        }
    }
}

$rows = @($rows | Sort-Object Cluster, IdentityProviderName, Domain)

if (@($rows).Count -eq 0) {
    throw "No SSO inventory rows were produced."
}

$rows |
    Select-Object Cluster, SSOConfigured, IdentityProviderName, Enabled, Domain, DefaultRoles |
    Format-Table -AutoSize -Wrap |
    Out-Host

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_Helios_SSO_Configuration_$timestamp.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Clusters discovered : $(@($clusters).Count)"
Write-Host "Inventory rows      : $(@($rows).Count)"
Write-Host "Cluster query issues: $(@($issues).Count)"
Write-Host "CSV output          : $csvPath"

if (@($issues).Count -gt 0) {
    Write-Warning "One or more clusters could not be queried."
    $issues | Format-Table -AutoSize -Wrap | Out-Host
}
