# Cohesity Helios SSO / Identity Provider Inventory
# Helios-level | GET-only | PowerShell 5.1 compatible

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

function Convert-ToDisplayValue {
    param($Value)

    if ($null -eq $Value) {
        return "N/A"
    }

    $items = @(
        @($Value) |
            ForEach-Object {
                if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                    ([string]$_).Trim()
                }
            }
    )

    if (@($items).Count -eq 0) {
        return "N/A"
    }

    return (($items | Select-Object -Unique) -join "; ")
}

try {
    $response = Invoke-RestMethod `
        -Uri "$baseUrl/v2/mcm/idps" `
        -Headers $headers `
        -Method Get `
        -ErrorAction Stop
}
catch {
    throw "Failed to query Helios identity providers: $($_.Exception.Message)"
}

$idps = @()

if ($null -ne $response) {
    $idpsProperty = $response.PSObject.Properties["idps"]

    if ($null -ne $idpsProperty -and $null -ne $idpsProperty.Value) {
        $idps = @(
            $idpsProperty.Value |
                Where-Object { $null -ne $_ }
        )
    }
    elseif ($response -is [System.Array]) {
        $idps = @(
            $response |
                Where-Object { $null -ne $_ }
        )
    }
    else {
        $idps = @($response)
    }
}

if (@($idps).Count -eq 0) {
    Write-Warning "No Helios identity provider configurations were returned."
    return
}

$rows = @(
    foreach ($idp in $idps) {
        [pscustomobject][ordered]@{
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
)

$rows = @($rows | Sort-Object IdentityProviderName, Domain)

$rows | Format-Table -AutoSize -Wrap | Out-Host

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_Helios_SSO_Configuration_$timestamp.csv"

$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host "Identity provider configurations: $(@($rows).Count)"
Write-Host "CSV output: $csvPath"
