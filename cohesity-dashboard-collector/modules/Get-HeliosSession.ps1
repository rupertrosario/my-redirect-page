function Get-HeliosSession {
    [CmdletBinding()]
    param([hashtable]$Config)

    if (-not (Test-Path $Config.ApiKeyHelperPath -PathType Leaf)) {
        throw "Missing API key helper: $($Config.ApiKeyHelperPath)"
    }
    if (-not (Test-Path $Config.EncryptedApiKeyPath -PathType Leaf)) {
        throw "Missing encrypted API key: $($Config.EncryptedApiKeyPath)"
    }

    . $Config.ApiKeyHelperPath
    $apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $Config.EncryptedApiKeyPath
    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        throw 'AES API key helper returned an empty API key.'
    }

    return @{ accept = 'application/json'; apiKey = $apiKey }
}

function New-HeliosHeaders {
    [CmdletBinding()]
    param([hashtable]$BaseHeaders, [string]$ClusterId)

    $headers = @{}
    foreach ($key in $BaseHeaders.Keys) { $headers[$key] = $BaseHeaders[$key] }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers['accessClusterId'] = $ClusterId
    }
    return $headers
}
