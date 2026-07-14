# =====================================================================
# Cohesity Active Directory Configuration CSV
# Multi-Cluster | Helios | GET-only | PowerShell 5.1 compatible
#
# Purpose:
# - Export required Active Directory integration details from all clusters
# - One CSV row per AD connection
# - Include clusters where AD is not configured
# - No password-policy collection
# - No JSON columns
# - No POST / PUT / PATCH / DELETE
#
# Uses:
# - GET /v2/mcm/cluster-mgmt/info
# - GET /v2/active-directories?includeTenants=true
#
# CSV Columns:
# Cluster, ADConfigured, DomainName, OrganizationalUnit, WorkGroupName,
# MachineAccounts, PreferredDomainControllers, DomainControllersDenyList,
# TrustedDomains, ADConfigurationId
# =====================================================================

$ErrorActionPreference = "Stop"

# -------------------------------
# Config
# -------------------------------
$baseUrl      = "https://helios.cohesity.com"
$apikeypath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$logDirectory = "X:\PowerShell\Data\Cohesity\ADInventory"

if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path -Path $apikeypath -PathType Leaf)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "API key file is empty: $apikeypath"
}

$commonHeaders = @{
    "apiKey" = $apiKey
    "accept" = "application/json"
}

# -------------------------------
# GET wrapper
# -------------------------------
function Invoke-HeliosGetJson {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -UseBasicParsing `
            -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

# -------------------------------
# Helpers
# -------------------------------
function ValueOrNA {
    param($Value)

    if ($null -eq $Value) {
        return "N/A"
    }

    if ($Value -is [array]) {
        $items = @(
            $Value | ForEach-Object {
                if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                    ([string]$_).Trim()
                }
            }
        )

        if ($items.Count -eq 0) {
            return "N/A"
        }

        return ($items -join ", ")
    }

    $text = [string]$Value

    if ([string]::IsNullOrWhiteSpace($text)) {
        return "N/A"
    }

    return $text.Trim()
}

function Get-FirstValue {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return "N/A"
    }

    foreach ($name in $Names) {
        if ($null -ne $Object.PSObject.Properties[$name]) {
            $value = ValueOrNA $Object.$name

            if ($value -ne "N/A") {
                return $value
            }
        }
    }

    return "N/A"
}

function Format-NameStatusList {
    param($Items)

    $values = @()

    foreach ($item in @($Items)) {
        if ($null -eq $item) {
            continue
        }

        $name   = Get-FirstValue $item @("name", "dnsHostName", "hostName")
        $status = Get-FirstValue $item @("status", "state")

        if ($name -ne "N/A" -and $status -ne "N/A") {
            $values += "$name [$status]"
        }
        elseif ($name -ne "N/A") {
            $values += $name
        }
        elseif ($status -ne "N/A") {
            $values += "Status=$status"
        }
    }

    if ($values.Count -eq 0) {
        return "N/A"
    }

    return ($values -join "; ")
}

function Format-MachineAccounts {
    param($MachineAccounts)

    $values = @()

    foreach ($account in @($MachineAccounts)) {
        if ($null -eq $account) {
            continue
        }

        $name    = Get-FirstValue $account @("name")
        $dnsName = Get-FirstValue $account @("dnsHostName")
        $parts   = @()

        if ($name -ne "N/A") {
            $parts += $name
        }

        if ($dnsName -ne "N/A" -and $dnsName -ne $name) {
            $parts += "DNS=$dnsName"
        }

        if ($parts.Count -gt 0) {
            $values += ($parts -join "; ")
        }
    }

    if ($values.Count -eq 0) {
        return "N/A"
    }

    return ($values -join " | ")
}

function Format-TrustedDomains {
    param($TrustedDomainParams)

    if ($null -eq $TrustedDomainParams) {
        return "N/A"
    }

    $values = @()

    foreach ($trustedDomain in @($TrustedDomainParams.trustedDomains)) {
        if ($null -eq $trustedDomain) {
            continue
        }

        $domainName = Get-FirstValue $trustedDomain @("domainName")

        if ($domainName -ne "N/A" -and $domainName -notin $values) {
            $values += $domainName
        }
    }

    foreach ($domain in @($TrustedDomainParams.whitelistedDomains)) {
        $domainName = ValueOrNA $domain

        if ($domainName -ne "N/A" -and $domainName -notin $values) {
            $values += $domainName
        }
    }

    if ($values.Count -eq 0) {
        return "N/A"
    }

    return ($values -join ", ")
}

function New-EmptyADRow {
    param(
        [string]$Cluster,
        [string]$ADConfigured
    )

    return [pscustomobject][ordered]@{
        Cluster                    = $Cluster
        ADConfigured               = $ADConfigured
        DomainName                 = "N/A"
        OrganizationalUnit         = "N/A"
        WorkGroupName              = "N/A"
        MachineAccounts            = "N/A"
        PreferredDomainControllers = "N/A"
        DomainControllersDenyList  = "N/A"
        TrustedDomains             = "N/A"
        ADConfigurationId          = "N/A"
    }
}

# -------------------------------
# Get clusters
# -------------------------------
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   COHESITY AD CONFIGURATION CSV" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Output: CSV only" -ForegroundColor Gray

try {
    $clusterJson = Invoke-HeliosGetJson `
        -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
        -Headers $commonHeaders
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

$clusters = @()

if ($clusterJson.cohesityClusters) {
    $clusters = @($clusterJson.cohesityClusters)
}
elseif ($clusterJson.clusters) {
    $clusters = @($clusterJson.clusters)
}
elseif ($clusterJson.clusterInfos) {
    $clusters = @($clusterJson.clusterInfos)
}
elseif ($clusterJson.mcmInfo.clusterInfos) {
    $clusters = @($clusterJson.mcmInfo.clusterInfos)
}

if (-not $clusters -or $clusters.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$clusters = @(
    $clusters | Sort-Object {
        Get-FirstValue $_ @(
            "name",
            "clusterName",
            "displayName",
            "ClusterName",
            "Name"
        )
    }
)

# -------------------------------
# Main collection
# -------------------------------
$rows = @()
$clusterIssues = @()
$configuredClusterCount = 0
$notConfiguredClusterCount = 0

foreach ($cluster in $clusters) {
    $clusterName = Get-FirstValue $cluster @(
        "name",
        "clusterName",
        "displayName",
        "ClusterName",
        "Name"
    )

    $clusterId = Get-FirstValue $cluster @(
        "clusterId",
        "id",
        "ClusterId",
        "Id"
    )

    if ($clusterName -eq "N/A") {
        $clusterName = "Unknown"
    }

    if ($clusterId -eq "N/A") {
        $clusterIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Cluster ID missing"
        }

        $rows += New-EmptyADRow `
            -Cluster $clusterName `
            -ADConfigured "Unknown"

        continue
    }

    Write-Host "Processing cluster: $clusterName" -ForegroundColor Cyan

    $headers = @{
        "apiKey"          = $apiKey
        "accessClusterId" = [string]$clusterId
        "accept"          = "application/json"
    }

    try {
        $adJson = Invoke-HeliosGetJson `
            -Uri "$baseUrl/v2/active-directories?includeTenants=true" `
            -Headers $headers
    }
    catch {
        $clusterIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "AD configuration fetch failed: $($_.Exception.Message)"
        }

        $rows += New-EmptyADRow `
            -Cluster $clusterName `
            -ADConfigured "Unknown"

        continue
    }

    $activeDirectories = @()

    if ($adJson.activeDirectories) {
        $activeDirectories = @(
            $adJson.activeDirectories |
                Where-Object { $null -ne $_ }
        )
    }
    elseif ($adJson -is [array]) {
        $activeDirectories = @(
            $adJson |
                Where-Object { $null -ne $_ }
        )
    }
    elseif (
        $adJson.domainName -or
        $adJson.connectionId -or
        $adJson.id
    ) {
        $activeDirectories = @($adJson)
    }

    if ($activeDirectories.Count -eq 0) {
        $rows += New-EmptyADRow `
            -Cluster $clusterName `
            -ADConfigured "No"

        $notConfiguredClusterCount++
        continue
    }

    $configuredClusterCount++

    foreach ($activeDirectory in $activeDirectories) {
        $rows += [pscustomobject][ordered]@{
            Cluster                    = $clusterName
            ADConfigured               = "Yes"
            DomainName                 = Get-FirstValue $activeDirectory @("domainName")
            OrganizationalUnit         = Get-FirstValue $activeDirectory @("organizationalUnitName")
            WorkGroupName              = Get-FirstValue $activeDirectory @("workGroupName")
            MachineAccounts            = Format-MachineAccounts $activeDirectory.machineAccounts
            PreferredDomainControllers = Format-NameStatusList $activeDirectory.preferredDomainControllers
            DomainControllersDenyList  = ValueOrNA $activeDirectory.domainControllersDenyList
            TrustedDomains             = Format-TrustedDomains $activeDirectory.trustedDomainParams
            ADConfigurationId          = Get-FirstValue $activeDirectory @("id")
        }
    }
}

# -------------------------------
# Export CSV
# -------------------------------
$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path `
    $logDirectory `
    "Cohesity_AD_Configuration_$timestamp.csv"

$rows |
    Sort-Object Cluster, DomainName |
    Export-Csv `
        -Path $csvPath `
        -NoTypeInformation `
        -Encoding UTF8

# -------------------------------
# Console summary
# -------------------------------
Write-Host "`n==============================" -ForegroundColor Cyan
Write-Host "AD CONFIGURATION SUMMARY" -ForegroundColor White
Write-Host "==============================" -ForegroundColor Cyan
Write-Host "Clusters discovered       : $($clusters.Count)"
Write-Host "Clusters with AD          : $configuredClusterCount"
Write-Host "Clusters without AD       : $notConfiguredClusterCount"
Write-Host "CSV rows                  : $($rows.Count)"
Write-Host "CSV output                : $csvPath"

if ($clusterIssues.Count -gt 0) {
    Write-Host "Cluster fetch issues      : $($clusterIssues.Count)" -ForegroundColor Yellow

    foreach ($issue in $clusterIssues) {
        Write-Host (
            " - {0}: {1}" -f $issue.Cluster, $issue.Issue
        ) -ForegroundColor Yellow
    }
}

Write-Host "=============================="
Write-Host "Processing complete."
