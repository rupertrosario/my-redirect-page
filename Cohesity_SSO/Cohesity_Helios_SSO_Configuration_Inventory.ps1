# Cohesity Helios SSO / Identity Provider Inventory
# Multi-cluster | Helios proxy | GET-only | PowerShell 5.1 compatible

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

    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers["accessClusterId"] = $ClusterId
    }
    return $headers
}

function Get-Json {
    param([string]$Uri, [hashtable]$Headers)

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

function Get-PropertyValue {
    param($Object, [string[]]$Names)

    if ($null -eq $Object) { return $null }
    foreach ($name in $Names) {
        $property = @($Object.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1)
        if (@($property).Count -gt 0 -and $null -ne $property[0].Value) {
            return $property[0].Value
        }
    }
    return $null
}

function To-Text {
    param($Value)

    $items = @(
        @($Value) | ForEach-Object {
            if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                ([string]$_).Trim()
            }
        }
    )

    if (@($items).Count -eq 0) { return "N/A" }
    return (($items | Select-Object -Unique) -join "; ")
}

function Get-IdpArray {
    param($Response)

    if ($null -eq $Response) { return @() }
    $idps = Get-PropertyValue -Object $Response -Names @("idps")
    if ($null -eq $idps) { return @() }
    return @(@($idps) | Where-Object { $null -ne $_ })
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

$clusterSource = Get-PropertyValue -Object $clusterJson -Names @("cohesityClusters", "clusters", "clusterInfos")
if ($null -eq $clusterSource) {
    $mcmInfo = Get-PropertyValue -Object $clusterJson -Names @("mcmInfo")
    if ($null -ne $mcmInfo) {
        $clusterSource = Get-PropertyValue -Object $mcmInfo -Names @("clusterInfos")
    }
}

$clusters = @(@($clusterSource) | Where-Object { $null -ne $_ })
if (@($clusters).Count -eq 0) {
    throw "No clusters were returned from Helios."
}

$rows = @()
$issues = @()

foreach ($cluster in $clusters) {
    $clusterName = To-Text (Get-PropertyValue -Object $cluster -Names @("clusterName", "displayName", "name"))
    $clusterId   = To-Text (Get-PropertyValue -Object $cluster -Names @("clusterId", "id"))

    if ($clusterName -eq "N/A") { $clusterName = "Unknown" }

    if ($clusterId -eq "N/A") {
        $issues += [pscustomobject]@{ Cluster = $clusterName; Issue = "Cluster ID missing" }
        continue
    }

    Write-Host "Querying SSO configuration for $clusterName..." -ForegroundColor Yellow

    try {
        $idpResponse = Get-Json -Uri "$baseUrl/v2/idps" -Headers (New-Headers -ClusterId $clusterId)
        $idps = @(Get-IdpArray -Response $idpResponse)
    }
    catch {
        $message = $_.Exception.Message
        $issues += [pscustomobject]@{ Cluster = $clusterName; Issue = $message }
        $rows += [pscustomobject][ordered]@{
            Cluster              = $clusterName
            ClusterId            = $clusterId
            QueryStatus          = "Failed"
            SSOConfigured        = "Unknown"
            IdentityProviderName = "N/A"
            Enabled              = "N/A"
            AllowLocalUserLogin  = "N/A"
            Domain               = "N/A"
            IssuerId             = "N/A"
            SSOUrl               = "N/A"
            Roles                = "N/A"
            SamlAttributeName    = "N/A"
            SignRequest          = "N/A"
            TenantId             = "N/A"
            Id                   = "N/A"
            Issue                = $message
        }
        continue
    }

    if (@($idps).Count -eq 0) {
        $rows += [pscustomobject][ordered]@{
            Cluster              = $clusterName
            ClusterId            = $clusterId
            QueryStatus          = "Success"
            SSOConfigured        = "No"
            IdentityProviderName = "N/A"
            Enabled              = "N/A"
            AllowLocalUserLogin  = "N/A"
            Domain               = "N/A"
            IssuerId             = "N/A"
            SSOUrl               = "N/A"
            Roles                = "N/A"
            SamlAttributeName    = "N/A"
            SignRequest          = "N/A"
            TenantId             = "N/A"
            Id                   = "N/A"
            Issue                = ""
        }
        continue
    }

    foreach ($idp in $idps) {
        $rows += [pscustomobject][ordered]@{
            Cluster              = $clusterName
            ClusterId            = $clusterId
            QueryStatus          = "Success"
            SSOConfigured        = "Yes"
            IdentityProviderName = To-Text $idp.name
            Enabled              = if ($null -eq $idp.isEnabled) { "N/A" } elseif ([bool]$idp.isEnabled) { "Yes" } else { "No" }
            AllowLocalUserLogin  = if ($null -eq $idp.allowLocalUserLogin) { "N/A" } elseif ([bool]$idp.allowLocalUserLogin) { "Yes" } else { "No" }
            Domain               = To-Text $idp.domain
            IssuerId             = To-Text $idp.issuerId
            SSOUrl               = To-Text $idp.ssoUrl
            Roles                = To-Text $idp.roles
            SamlAttributeName    = To-Text $idp.samlAttributeName
            SignRequest          = if ($null -eq $idp.signRequest) { "N/A" } elseif ([bool]$idp.signRequest) { "Yes" } else { "No" }
            TenantId             = To-Text $idp.tenantId
            Id                   = To-Text $idp.id
            Issue                = ""
        }
    }
}

$rows = @($rows | Sort-Object Cluster, IdentityProviderName)

$rows |
    Select-Object Cluster, QueryStatus, SSOConfigured, IdentityProviderName, Enabled, Domain, Roles |
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
