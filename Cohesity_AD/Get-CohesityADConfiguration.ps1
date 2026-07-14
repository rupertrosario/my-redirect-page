<#
.SYNOPSIS
    Collects Active Directory integration configuration from all Cohesity clusters managed by Helios.

.DESCRIPTION
    GET-only collector. The script first retrieves the cluster inventory from Helios, then queries
    each cluster's Active Directory configuration using the accessClusterId header.

    No POST, PUT, PATCH, or DELETE requests are used.

.PARAMETER ApiKey
    Cohesity Helios API key. If omitted, the script reads COHESITY_API_KEY from the environment.

.PARAMETER HeliosUrl
    Cohesity Helios base URL. Default: https://helios.cohesity.com

.PARAMETER OutputCsvPath
    CSV output path. If omitted, a timestamped file is created in the current directory.

.PARAMETER TimeoutSec
    HTTP timeout for each GET request. Default: 120 seconds.

.EXAMPLE
    $env:COHESITY_API_KEY = '<api-key>'
    .\Get-CohesityADConfiguration.ps1

.EXAMPLE
    .\Get-CohesityADConfiguration.ps1 `
        -ApiKey $env:COHESITY_API_KEY `
        -OutputCsvPath .\Cohesity_AD_Configuration.csv
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$ApiKey = $env:COHESITY_API_KEY,

    [Parameter(Mandatory = $false)]
    [ValidateNotNullOrEmpty()]
    [string]$HeliosUrl = 'https://helios.cohesity.com',

    [Parameter(Mandatory = $false)]
    [string]$OutputCsvPath,

    [Parameter(Mandatory = $false)]
    [ValidateRange(10, 600)]
    [int]$TimeoutSec = 120
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

# Required for Windows PowerShell 5.1 when the host has not already enabled TLS 1.2.
if ($PSVersionTable.PSVersion.Major -lt 6) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
}

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'Helios API key is required. Pass -ApiKey or set the COHESITY_API_KEY environment variable.'
}

$HeliosUrl = $HeliosUrl.TrimEnd('/')

if ([string]::IsNullOrWhiteSpace($OutputCsvPath)) {
    $fileName = 'Cohesity_AD_Configuration_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
    $OutputCsvPath = Join-Path -Path (Get-Location) -ChildPath $fileName
}

function Invoke-CohesityGet {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri,

        [Parameter(Mandatory = $true)]
        [hashtable]$Headers
    )

    Invoke-RestMethod `
        -Method Get `
        -Uri $Uri `
        -Headers $Headers `
        -ContentType 'application/json' `
        -TimeoutSec $TimeoutSec
}

function ConvertTo-Array {
    param([object]$Value)

    if ($null -eq $Value) {
        return @()
    }

    return @($Value)
}

function Get-ObjectPropertyValue {
    param(
        [object]$Object,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Join-NonEmpty {
    param(
        [object[]]$Values,
        [string]$Separator = '; '
    )

    $items = @(
        $Values |
            ForEach-Object {
                if ($null -ne $_) {
                    ([string]$_).Trim()
                }
            } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Select-Object -Unique
    )

    return ($items -join $Separator)
}

function Get-ControllerDisplay {
    param([object]$Controllers)

    $values = foreach ($controller in (ConvertTo-Array $Controllers)) {
        if ($null -eq $controller) {
            continue
        }

        $name = [string](Get-ObjectPropertyValue -Object $controller -Name 'name')
        $status = [string](Get-ObjectPropertyValue -Object $controller -Name 'status')

        if (-not [string]::IsNullOrWhiteSpace($name) -and -not [string]::IsNullOrWhiteSpace($status)) {
            '{0} [{1}]' -f $name, $status
        }
        elseif (-not [string]::IsNullOrWhiteSpace($name)) {
            $name
        }
    }

    return (Join-NonEmpty -Values $values)
}

function Get-DomainControllerDisplay {
    param([object]$DomainControllerGroups)

    $values = foreach ($group in (ConvertTo-Array $DomainControllerGroups)) {
        if ($null -eq $group) {
            continue
        }

        $domain = [string](Get-ObjectPropertyValue -Object $group -Name 'domainName')
        $controllers = Get-ControllerDisplay -Controllers (Get-ObjectPropertyValue -Object $group -Name 'controllers')

        if (-not [string]::IsNullOrWhiteSpace($domain) -and -not [string]::IsNullOrWhiteSpace($controllers)) {
            '{0}: {1}' -f $domain, $controllers
        }
        elseif (-not [string]::IsNullOrWhiteSpace($controllers)) {
            $controllers
        }
    }

    return (Join-NonEmpty -Values $values)
}

function Get-MachineAccountDisplay {
    param([object]$MachineAccounts)

    $values = foreach ($account in (ConvertTo-Array $MachineAccounts)) {
        if ($null -eq $account) {
            continue
        }

        $name = [string](Get-ObjectPropertyValue -Object $account -Name 'name')
        $dnsHostName = [string](Get-ObjectPropertyValue -Object $account -Name 'dnsHostName')

        if (-not [string]::IsNullOrWhiteSpace($name) -and -not [string]::IsNullOrWhiteSpace($dnsHostName)) {
            '{0} ({1})' -f $name, $dnsHostName
        }
        elseif (-not [string]::IsNullOrWhiteSpace($name)) {
            $name
        }
        elseif (-not [string]::IsNullOrWhiteSpace($dnsHostName)) {
            $dnsHostName
        }
    }

    return (Join-NonEmpty -Values $values)
}

function Get-TrustedDomainNames {
    param([object]$TrustedDomainParams)

    if ($null -eq $TrustedDomainParams) {
        return ''
    }

    $trustedDomains = Get-ObjectPropertyValue -Object $TrustedDomainParams -Name 'trustedDomains'

    $values = foreach ($domain in (ConvertTo-Array $trustedDomains)) {
        $domainName = [string](Get-ObjectPropertyValue -Object $domain -Name 'domainName')
        if (-not [string]::IsNullOrWhiteSpace($domainName)) {
            $domainName
        }
    }

    return (Join-NonEmpty -Values $values)
}

function Get-ActiveDirectoriesFromResponse {
    param([object]$Response)

    if ($null -eq $Response) {
        return @()
    }

    $propertyNames = @($Response.PSObject.Properties.Name)

    if ($propertyNames -contains 'activeDirectories') {
        return @(ConvertTo-Array (Get-ObjectPropertyValue -Object $Response -Name 'activeDirectories'))
    }

    return @(ConvertTo-Array $Response)
}

function Test-UnreachableController {
    param([object]$ActiveDirectory)

    $statuses = @()

    foreach ($controller in (ConvertTo-Array (Get-ObjectPropertyValue -Object $ActiveDirectory -Name 'preferredDomainControllers'))) {
        $status = [string](Get-ObjectPropertyValue -Object $controller -Name 'status')
        if (-not [string]::IsNullOrWhiteSpace($status)) {
            $statuses += $status
        }
    }

    foreach ($group in (ConvertTo-Array (Get-ObjectPropertyValue -Object $ActiveDirectory -Name 'domainControllers'))) {
        foreach ($controller in (ConvertTo-Array (Get-ObjectPropertyValue -Object $group -Name 'controllers'))) {
            $status = [string](Get-ObjectPropertyValue -Object $controller -Name 'status')
            if (-not [string]::IsNullOrWhiteSpace($status)) {
                $statuses += $status
            }
        }
    }

    return [bool]($statuses | Where-Object { $_ -ne 'Reachable' } | Select-Object -First 1)
}

$commonHeaders = @{
    Accept = 'application/json'
    apiKey = $ApiKey
}

$clusterUri = '{0}/v2/mcm/cluster-mgmt/info' -f $HeliosUrl
Write-Host ('Retrieving Cohesity cluster inventory from {0} ...' -f $HeliosUrl)

$clusterResponse = Invoke-CohesityGet -Uri $clusterUri -Headers $commonHeaders
$clusters = @()

if ($null -ne $clusterResponse -and @($clusterResponse.PSObject.Properties.Name) -contains 'cohesityClusters') {
    $clusters = @(ConvertTo-Array (Get-ObjectPropertyValue -Object $clusterResponse -Name 'cohesityClusters'))
}
else {
    $clusters = @(ConvertTo-Array $clusterResponse)
}

$clusters = @(
    $clusters |
        Where-Object { $null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string](Get-ObjectPropertyValue -Object $_ -Name 'clusterId')) } |
        Sort-Object -Property clusterName
)

if ($clusters.Count -eq 0) {
    throw 'No clusters were returned by Helios.'
}

$rows = New-Object System.Collections.Generic.List[object]
$adUri = '{0}/v2/active-directories?includeTenants=true' -f $HeliosUrl

foreach ($cluster in $clusters) {
    $clusterId = [string](Get-ObjectPropertyValue -Object $cluster -Name 'clusterId')
    $clusterName = [string](Get-ObjectPropertyValue -Object $cluster -Name 'clusterName')

    if ([string]::IsNullOrWhiteSpace($clusterName)) {
        $clusterName = $clusterId
    }

    Write-Host ('[{0}] Retrieving Active Directory configuration ...' -f $clusterName)

    $clusterHeaders = @{
        Accept = 'application/json'
        apiKey = $ApiKey
        accessClusterId = $clusterId
    }

    try {
        $adResponse = Invoke-CohesityGet -Uri $adUri -Headers $clusterHeaders
        $activeDirectories = @(Get-ActiveDirectoriesFromResponse -Response $adResponse)

        if ($activeDirectories.Count -eq 0) {
            $rows.Add([pscustomobject]@{
                ClusterName                 = $clusterName
                ClusterId                   = $clusterId
                ADConfigured                = 'No'
                DomainName                 = ''
                OrganizationalUnit          = ''
                WorkGroupName               = ''
                ConnectionId                = ''
                MachineAccounts             = ''
                PreferredDomainControllers  = ''
                DomainControllers           = ''
                DomainControllersDenyList   = ''
                IdMappingType               = ''
                TrustedDomains              = ''
                WhitelistedDomains          = ''
                BlacklistedDomains          = ''
                OnlyUseWhitelistedDomains   = ''
                TrustDiscoveryStatus        = ''
                TrustEnabled                = ''
                ErrorCode                   = ''
                ErrorMessage                = ''
                CollectionStatus            = 'NOT_CONFIGURED'
                CollectionError             = ''
            })

            continue
        }

        foreach ($ad in $activeDirectories) {
            $errorCode = ''
            $errorMessage = ''

            $adError = Get-ObjectPropertyValue -Object $ad -Name 'error'
            $idMappingParams = Get-ObjectPropertyValue -Object $ad -Name 'idMappingParams'
            $userIdMappingParams = Get-ObjectPropertyValue -Object $idMappingParams -Name 'userIdMappingParams'
            $trustedDomainParams = Get-ObjectPropertyValue -Object $ad -Name 'trustedDomainParams'

            if ($null -ne $adError) {
                $errorCode = [string](Get-ObjectPropertyValue -Object $adError -Name 'errorCode')
                $errorMessage = [string](Get-ObjectPropertyValue -Object $adError -Name 'errorMessage')
            }

            $collectionStatus = 'CONFIGURED'

            if (-not [string]::IsNullOrWhiteSpace($errorCode) -or -not [string]::IsNullOrWhiteSpace($errorMessage)) {
                $collectionStatus = 'ERROR'
            }
            elseif (Test-UnreachableController -ActiveDirectory $ad) {
                $collectionStatus = 'REVIEW'
            }

            $rows.Add([pscustomobject]@{
                ClusterName                 = $clusterName
                ClusterId                   = $clusterId
                ADConfigured                = 'Yes'
                DomainName                 = [string](Get-ObjectPropertyValue -Object $ad -Name 'domainName')
                OrganizationalUnit          = [string](Get-ObjectPropertyValue -Object $ad -Name 'organizationalUnitName')
                WorkGroupName               = [string](Get-ObjectPropertyValue -Object $ad -Name 'workGroupName')
                ConnectionId                = [string](Get-ObjectPropertyValue -Object $ad -Name 'connectionId')
                MachineAccounts             = Get-MachineAccountDisplay -MachineAccounts (Get-ObjectPropertyValue -Object $ad -Name 'machineAccounts')
                PreferredDomainControllers  = Get-ControllerDisplay -Controllers (Get-ObjectPropertyValue -Object $ad -Name 'preferredDomainControllers')
                DomainControllers           = Get-DomainControllerDisplay -DomainControllerGroups (Get-ObjectPropertyValue -Object $ad -Name 'domainControllers')
                DomainControllersDenyList   = Join-NonEmpty -Values (ConvertTo-Array (Get-ObjectPropertyValue -Object $ad -Name 'domainControllersDenyList'))
                IdMappingType               = [string](Get-ObjectPropertyValue -Object $userIdMappingParams -Name 'type')
                TrustedDomains              = Get-TrustedDomainNames -TrustedDomainParams $trustedDomainParams
                WhitelistedDomains          = Join-NonEmpty -Values (ConvertTo-Array (Get-ObjectPropertyValue -Object $trustedDomainParams -Name 'whitelistedDomains'))
                BlacklistedDomains          = Join-NonEmpty -Values (ConvertTo-Array (Get-ObjectPropertyValue -Object $trustedDomainParams -Name 'blacklistedDomains'))
                OnlyUseWhitelistedDomains   = [string](Get-ObjectPropertyValue -Object $trustedDomainParams -Name 'onlyUseWhitelistedDomains')
                TrustDiscoveryStatus        = [string](Get-ObjectPropertyValue -Object $trustedDomainParams -Name 'discoveryStatus')
                TrustEnabled                = [string](Get-ObjectPropertyValue -Object $trustedDomainParams -Name 'enabled')
                ErrorCode                   = $errorCode
                ErrorMessage                = $errorMessage
                CollectionStatus            = $collectionStatus
                CollectionError             = ''
            })
        }
    }
    catch {
        $message = $_.Exception.Message
        Write-Warning ('[{0}] Active Directory collection failed: {1}' -f $clusterName, $message)

        $rows.Add([pscustomobject]@{
            ClusterName                 = $clusterName
            ClusterId                   = $clusterId
            ADConfigured                = 'Unknown'
            DomainName                 = ''
            OrganizationalUnit          = ''
            WorkGroupName               = ''
            ConnectionId                = ''
            MachineAccounts             = ''
            PreferredDomainControllers  = ''
            DomainControllers           = ''
            DomainControllersDenyList   = ''
            IdMappingType               = ''
            TrustedDomains              = ''
            WhitelistedDomains          = ''
            BlacklistedDomains          = ''
            OnlyUseWhitelistedDomains   = ''
            TrustDiscoveryStatus        = ''
            TrustEnabled                = ''
            ErrorCode                   = ''
            ErrorMessage                = ''
            CollectionStatus            = 'COLLECTION_ERROR'
            CollectionError             = $message
        })
    }
}

$outputDirectory = Split-Path -Path $OutputCsvPath -Parent
if (-not [string]::IsNullOrWhiteSpace($outputDirectory) -and -not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

$sortedRows = @($rows | Sort-Object -Property ClusterName, DomainName)
$sortedRows | Export-Csv -Path $OutputCsvPath -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host 'Cohesity Active Directory Configuration Summary'
Write-Host '------------------------------------------------'
$sortedRows |
    Select-Object ClusterName, ADConfigured, DomainName, PreferredDomainControllers, IdMappingType, CollectionStatus |
    Format-Table -AutoSize |
    Out-Host

Write-Host ('Clusters queried : {0}' -f $clusters.Count)
Write-Host ('Rows produced    : {0}' -f $sortedRows.Count)
Write-Host ('CSV output       : {0}' -f (Resolve-Path -LiteralPath $OutputCsvPath))

# Return structured objects to support downstream PowerShell processing.
$sortedRows
