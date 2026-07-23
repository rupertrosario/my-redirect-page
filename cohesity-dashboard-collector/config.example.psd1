@{
    HeliosBaseUrl       = 'https://helios.cohesity.com'
    ApiKeyHelperPath    = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
    EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

    MaxConcurrency      = 6
    RequestTimeoutSec   = 90
    FailureRunsPerPG    = 6
    VerifyTls           = $true
    TargetVersion       = $null

    Endpoints = @{
        Clusters         = '/v2/mcm/cluster-mgmt/info'
        ProtectionGroups = '/v2/data-protect/protection-groups'
        PgRunsTemplate   = '/v2/data-protect/protection-groups/{0}/runs'
        Capacity         = '/irisservices/api/v1/public/stats/storage'
        Garbage          = '/irisservices/api/v1/public/statistics/timeSeriesStats'

        # Queried separately for every cluster with accessClusterId.
        # This is not the MCM fleet-alert endpoint.
        ClusterAlerts    = '/v2/alerts?maxAlerts=10000&alertStates=kOpen,kNote'
    }
}
