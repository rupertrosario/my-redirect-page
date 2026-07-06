$ErrorActionPreference = "Stop"

$root = "X:\PowerShell\Cohesity_API_Scripts"
$keyCheckPath = Join-Path $root "DO_NOT_Delete\apikey.txt"
$helperPath = Join-Path $root "Common\ApiKeyAesHelper.ps1"
$encryptedFile = Join-Path $root "Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $keyCheckPath)) {
    throw "Required key check file not found at $keyCheckPath"
}

if (-not (Test-Path $helperPath)) {
    throw "Required helper file not found at $helperPath"
}

if (-not (Test-Path $encryptedFile)) {
    throw "Required encrypted key file not found at $encryptedFile"
}

. $helperPath

$cohesityToken = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedFile

$headers = @{
    "apiKey" = $cohesityToken
    "accept" = "application/json"
}

$uri = "https://helios.cohesity.com/v2/mcm/cluster-mgmt/info"

Write-Host ""
Write-Host "Cohesity Helios connection test" -ForegroundColor Cyan
Write-Host "Mode     : GET only"
Write-Host "Endpoint : $uri"
Write-Host "Saving   : Nothing"
Write-Host ""

try {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Method GET -Uri $uri -Headers $headers -UseBasicParsing
    }
    else {
        $response = Invoke-WebRequest -Method GET -Uri $uri -Headers $headers
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        throw "Empty response from Helios."
    }

    $json = $response.Content | ConvertFrom-Json
    $clusters = @($json.cohesityClusters)

    if (-not $clusters -or $clusters.Count -eq 0) {
        throw "No clusters returned from Helios."
    }

    $rows = foreach ($cluster in ($clusters | Sort-Object clusterName)) {
        [pscustomobject]@{
            ClusterName = $cluster.clusterName
            ClusterId   = $cluster.clusterId
        }
    }

    Write-Host "Cluster count: $($rows.Count)" -ForegroundColor Green
    Write-Host ""
    $rows | Format-Table -AutoSize
}
catch {
    Write-Host ""
    Write-Host "Connection test failed." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    throw
}
