@{
    HeliosBaseUrl       = 'https://helios.cohesity.com'
    ApiKeyHelperPath    = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
    EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

    # Performance: clusters are collected concurrently. Start at 6; lower it if Helios throttles.
    MaxConcurrency      = 6
    RequestTimeoutSec   = 90
    FailureRunsPerPG    = 6
    VerifyTls           = $true

    # No approved version baseline is configured. Actual cluster versions are displayed without grading.
    TargetVersion       = $null

    Endpoints = @{
        Clusters         = '/v2/mcm/cluster-mgmt/info'
        ProtectionGroups = '/v2/data-protect/protection-groups'
        PgRunsTemplate   = '/v2/data-protect/protection-groups/{0}/runs'
        Capacity         = '/irisservices/api/v1/public/stats/storage'
        Garbage          = '/irisservices/api/v1/public/statistics/timeSeriesStats'
        Alerts           = '/v2/mcm/alerts?maxAlerts=10000&alertStateList=kOpen,kNote'
    }
}
