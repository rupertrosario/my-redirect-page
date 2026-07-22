@{
    HeliosBaseUrl   = 'https://helios.cohesity.com'
    ApiKeyHelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
    EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
    TargetVersion  = '6.8.1'
    LookbackDays   = 7
    VerifyTls      = $true

    # Paths are adjustable if your Helios tenant exposes a different version.
    Endpoints = @{
        Clusters         = '/v2/mcm/cluster-mgmt/info'
        Alerts           = '/v2/alerts'
        ProtectionGroups = '/v2/data-protect/protection-groups?isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000'
        Sources          = '/v2/data-protect/sources'
        Runs             = '/v2/data-protect/protection-groups/runs'
    }
}
