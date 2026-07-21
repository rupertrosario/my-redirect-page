# Cohesity Helios SSO / Identity Provider Inventory
# Multi-cluster assignment report | Helios | GET-only | PowerShell 5.1 compatible

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

$headers = @{
    accept = "application/json"
    apiKey = $apiKey
}

function Get-Json {
    param([Parameter(Mandatory)][string]$Uri)

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $headers -Method Get -ErrorAction Stop
    }

    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return ,@() }
    return ,@($Value)
}

function Get-PropertyValue {
    param($Object, [string[]]$Names)

    if ($null -eq $Object) { return $null }

    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties |
            Where-Object { $_.Name -ieq $name } |
            Select-Object -First 1

        if ($null -ne $property -and $null -ne $property.Value) {
            return $property.Value
        }
    }

    return $null
}

function To-Text {
    param($Value)

    $values = @(
        @(As-Array $Value) |
            ForEach-Object {
                if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                    ([string]$_).Trim()
                }
            }
    )

    if ($values.Length -eq 0) { return "N/A" }
    return (($values | Select-Object -Unique) -join "; ")
}

function Get-IdpArray {
    param($Response)

    if ($null -eq $Response) { return ,@() }

    $value = Get-PropertyValue $Response @("idps", "identityProviders")
    if ($null -ne $value) {
        return ,@(@(As-Array $value) | Where-Object { $null -ne $_ })
    }

    if ($Response -is [System.Array]) {
        return ,@($Response | Where-Object { $null -ne $_ })
    }

    return ,@($Response)
}

function Get-AssignedClusterTokens {
    param($Idp)

    $assigned = Get-PropertyValue $Idp @("defaultClusters", "clusters", "clusterIds")
    $tokens = @()

    foreach ($item in @(As-Array $assigned)) {
        if ($null -eq $item) { continue }

        if ($item -is [string] -or $item -is [ValueType]) {
            $text = ([string]$item).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text)) { $tokens += $text }
            continue
        }

        foreach ($name in @("clusterId", "id", "clusterName", "displayName", "name")) {
            $value = Get-PropertyValue $item @($name)
            if ($null -ne $value -and -not [string]::IsNullOrWhiteSpace([string]$value)) {
                $tokens += ([string]$value).Trim()
            }
        }
    }

    return ,@($tokens | Select-Object -Unique)
}

function Test-IdpAssignedToCluster {
    param($Idp, [string]$ClusterId, [string]$ClusterName)

    $tokens = @(Get-AssignedClusterTokens $Idp)
    if ($tokens.Length -eq 0) { return $false }

    foreach ($token in $tokens) {
        if ($token -ieq $ClusterId -or $token -ieq $ClusterName) {
            return $true
        }
    }

    return $false
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   COHESITY SSO CONFIGURATION INVENTORY" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan

try {
    $clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info"
    $idpJson     = Get-Json -Uri "$baseUrl/v2/mcm/idps"
}
catch {
    throw "Failed to query Helios: $($_.Exception.Message)"
}

$clusters = @()
if ($null -ne $clusterJson) {
    $clusterSource = Get-PropertyValue $clusterJson @("cohesityClusters", "clusters", "clusterInfos")
    if ($null -eq $clusterSource) {
        $mcmInfo = Get-PropertyValue $clusterJson @("mcmInfo")
        if ($null -ne $mcmInfo) {
            $clusterSource = Get-PropertyValue $mcmInfo @("clusterInfos")
        }
    }
    $clusters = @(@(As-Array $clusterSource) | Where-Object { $null -ne $_ })
}

if ($clusters.Length -eq 0) {
    throw "No clusters were returned from Helios."
}

$idps = @(Get-IdpArray -Response $idpJson)
$rows = @()

foreach ($cluster in $clusters) {
    $clusterName = To-Text (Get-PropertyValue $cluster @("clusterName", "displayName", "name"))
    $clusterId   = To-Text (Get-PropertyValue $cluster @("clusterId", "id"))

    $assignedIdps = @(
        $idps | Where-Object {
            Test-IdpAssignedToCluster -Idp $_ -ClusterId $clusterId -ClusterName $clusterName
        }
    )

    if ($assignedIdps.Length -eq 0) {
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

    foreach ($idp in $assignedIdps) {
        $enabledValue = Get-PropertyValue $idp @("isEnabled", "enabled")

        $rows += [pscustomobject][ordered]@{
            Cluster              = $clusterName
            ClusterId            = $clusterId
            SSOConfigured        = "Yes"
            IdentityProviderName = To-Text (Get-PropertyValue $idp @("name", "displayName"))
            Enabled              = if ($null -eq $enabledValue) { "N/A" } elseif ([bool]$enabledValue) { "Yes" } else { "No" }
            Domain               = To-Text (Get-PropertyValue $idp @("domain", "domains"))
            IssuerId             = To-Text (Get-PropertyValue $idp @("issuerId", "issuer"))
            SSOUrl               = To-Text (Get-PropertyValue $idp @("ssoUrl", "loginUrl"))
            DefaultClusters      = To-Text (Get-AssignedClusterTokens $idp)
            DefaultRoles         = To-Text (Get-PropertyValue $idp @("defaultRoles", "roles"))
            Id                   = To-Text (Get-PropertyValue $idp @("id", "idpId"))
        }
    }
}

$rows = @($rows | Sort-Object Cluster, IdentityProviderName)

$rows |
    Select-Object Cluster, SSOConfigured, IdentityProviderName, Enabled, Domain, DefaultRoles |
    Format-Table -AutoSize -Wrap |
    Out-Host

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_Helios_SSO_Configuration_$timestamp.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Clusters discovered : $($clusters.Length)"
Write-Host "Helios IDPs returned: $($idps.Length)"
Write-Host "Inventory rows      : $($rows.Length)"
Write-Host "CSV output          : $csvPath"
