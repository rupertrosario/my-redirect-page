function Get-HeliosSession {
    [CmdletBinding()]
    param([hashtable]$Config, [securestring]$Password)

    if ($Config.ApiKey) { return @{ Authorization = "Bearer $($Config.ApiKey)"; Accept = 'application/json' } }
    if (-not $Config.Username -or $null -eq $Password) {
        throw 'Set ApiKey in the private config, or provide Username and -Password.'
    }
    $credential = [pscredential]::new($Config.Username, $Password)
    $body = @{ username = $credential.UserName; password = $credential.GetNetworkCredential().Password } | ConvertTo-Json
    $uri = "{0}/irisservices/api/v1/public/accessTokens" -f $Config.HeliosBaseUrl.TrimEnd('/')
    $params = @{ Uri = $uri; Method = 'Post'; ContentType = 'application/json'; Body = $body }
    if (-not $Config.VerifyTls -and $PSVersionTable.PSVersion.Major -ge 7) { $params.SkipCertificateCheck = $true }
    $response = Invoke-RestMethod @params
    $token = Get-PropertyValue $response @('accessToken', 'token')
    if (-not $token) { throw 'Authentication succeeded but no access token was returned.' }
    @{ Authorization = "Bearer $token"; Accept = 'application/json' }
}
