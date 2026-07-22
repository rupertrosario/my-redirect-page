@{
    HeliosBaseUrl = 'https://helios.cohesity.com'
    Username       = ''
    ApiKey         = ''
    TargetVersion  = '6.8.1'
    LookbackDays   = 7
    VerifyTls      = $true

    # Keep endpoint paths here so they can be adjusted for the Helios release/tenant.
    Endpoints = @{
        Clusters         = '/v2/mcm/clusters/info'
        Alerts           = '/v2/alerts'
        ProtectionGroups = '/v2/data-protect/protection-groups'
        Sources          = '/v2/data-protect/sources'
        Runs             = '/v2/data-protect/protection-groups/runs'
    }
}
