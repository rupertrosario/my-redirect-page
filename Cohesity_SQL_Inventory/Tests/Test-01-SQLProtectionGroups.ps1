# Cohesity SQL Inventory - Test 01
# Purpose: Validate the active SQL protection-group response shape only.
# Scope: GET-only, first cluster with SQL protection groups, maximum five rows by default.

[CmdletBinding()]
param(
    [string]$ClusterName,
    [int]$MaxProtectionGroups = 5,
    [switch]$ScanAllClusters,
    [string]$OutputDirectory = "X:\PowerShell\Data\Cohesity\SQLInventory\Tests"
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if ($MaxProtectionGroups -lt 1) {
    throw "MaxProtectionGroups must be at least 1."
}

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}

if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][hashtable]$Headers
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

function Get-PropertyValue {
    param(
        $Object,
        [string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object) {
        return $Default
    }

    foreach ($name in $Names) {
        $property = @($Object.PSObject.Properties) |
            Where-Object { $_.Name -ieq $name } |
            Select-Object -First 1

        if ($null -ne $property -and $null -ne $property.Value) {
            return $property.Value
        }
    }

    return $Default
}

function Get-ClusterNameValue {
    param($Cluster)

    return [string](Get-PropertyValue $Cluster @(
        "name",
        "clusterName",
        "displayName"
    ) "Unknown")
}

function Get-ClusterIdValue {
    param($Cluster)

    $id = Get-PropertyValue $Cluster @(
        "clusterId",
        "id"
    ) $null

    if ($null -eq $id) {
        return $null
    }

    return [string]$id
}

$commonHeaders = @{
    accept = "application/json"
    apiKey = $apiKey
}

$clusterResponse = Invoke-CohesityGet `
    -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers $commonHeaders

$clusters = @($clusterResponse.cohesityClusters)
if ($clusters.Count -eq 0) {
    throw "No clusters were returned by Cohesity Helios."
}

$clusters = @($clusters | Sort-Object { Get-ClusterNameValue $_ })

if (-not [string]::IsNullOrWhiteSpace($ClusterName)) {
    $clusters = @(
        $clusters | Where-Object {
            (Get-ClusterNameValue $_) -like $ClusterName
        }
    )

    if ($clusters.Count -eq 0) {
        throw "No cluster matched '$ClusterName'."
    }
}

$rows = @()
$issues = @()
$clustersChecked = 0
$clustersWithSqlProtectionGroups = 0

foreach ($cluster in $clusters) {
    $resolvedClusterName = Get-ClusterNameValue $cluster
    $clusterId = Get-ClusterIdValue $cluster

    if ([string]::IsNullOrWhiteSpace($clusterId)) {
        $issues += [pscustomobject]@{
            Cluster = $resolvedClusterName
            Stage   = "Cluster discovery"
            Issue   = "Cluster ID was not returned"
        }
        continue
    }

    $clustersChecked++

    $headers = @{
        accept          = "application/json"
        apiKey          = $apiKey
        accessClusterId = $clusterId
    }

    $uri = "$baseUrl/v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true"

    try {
        $pgResponse = Invoke-CohesityGet -Uri $uri -Headers $headers
        $protectionGroups = @($pgResponse.protectionGroups)

        if ($protectionGroups.Count -eq 0) {
            continue
        }

        $clustersWithSqlProtectionGroups++

        foreach ($pg in @($protectionGroups | Select-Object -First $MaxProtectionGroups)) {
            $sqlProperty = @(
                $pg.PSObject.Properties | Where-Object {
                    $_.Name -match '(?i)(sql|mssql)'
                }
            ) | Select-Object -First 1

            $sqlSectionName = if ($null -ne $sqlProperty) {
                $sqlProperty.Name
            }
            else {
                "Not found"
            }

            $sqlSectionProperties = if (
                $null -ne $sqlProperty -and
                $null -ne $sqlProperty.Value
            ) {
                @($sqlProperty.Value.PSObject.Properties.Name) -join ", "
            }
            else {
                "N/A"
            }

            $rows += [pscustomobject]@{
                Cluster             = $resolvedClusterName
                ProtectionGroupName = [string](Get-PropertyValue $pg @("name") "N/A")
                ProtectionGroupId   = [string](Get-PropertyValue $pg @("id") "N/A")
                Environment         = [string](Get-PropertyValue $pg @("environment") "N/A")
                PolicyId            = [string](Get-PropertyValue $pg @("policyId") "N/A")
                SqlSection          = $sqlSectionName
                SqlSectionFields    = $sqlSectionProperties
            }
        }

        if (-not $ScanAllClusters) {
            break
        }
    }
    catch {
        $issues += [pscustomobject]@{
            Cluster = $resolvedClusterName
            Stage   = "SQL protection groups"
            Issue   = $_.Exception.Message
        }
    }
}

$summary = @(
    [pscustomobject]@{ Metric = "Clusters available"; Count = $clusters.Count }
    [pscustomobject]@{ Metric = "Clusters checked"; Count = $clustersChecked }
    [pscustomobject]@{ Metric = "Clusters with SQL protection groups"; Count = $clustersWithSqlProtectionGroups }
    [pscustomobject]@{ Metric = "SQL protection groups displayed"; Count = $rows.Count }
    [pscustomobject]@{ Metric = "Issues"; Count = $issues.Count }
)

$summaryText = $summary | Format-Table -AutoSize | Out-String -Width 5000
$rowsText = if ($rows.Count -gt 0) {
    $rows |
        Format-Table `
            Cluster,
            ProtectionGroupName,
            ProtectionGroupId,
            Environment,
            PolicyId,
            SqlSection,
            SqlSectionFields `
            -AutoSize -Wrap |
        Out-String -Width 5000
}
else {
    "No active SQL protection groups were found in the checked cluster set.`r`n"
}

$issuesText = if ($issues.Count -gt 0) {
    $issues | Format-Table -AutoSize -Wrap | Out-String -Width 5000
}
else {
    "None`r`n"
}

Write-Host "`nTEST 01 - SQL PROTECTION GROUP SHAPE" -ForegroundColor Cyan
Write-Host "`nSUMMARY" -ForegroundColor Cyan
$summaryText | Write-Host
Write-Host "SQL PROTECTION GROUPS" -ForegroundColor Cyan
$rowsText | Write-Host
Write-Host "ISSUES" -ForegroundColor Cyan
$issuesText | Write-Host

if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputPath = Join-Path $OutputDirectory "Test-01-SQLProtectionGroups_$timestamp.txt"

@(
    "TEST 01 - SQL PROTECTION GROUP SHAPE"
    "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    ""
    "SUMMARY"
    $summaryText.TrimEnd()
    ""
    "SQL PROTECTION GROUPS"
    $rowsText.TrimEnd()
    ""
    "ISSUES"
    $issuesText.TrimEnd()
) | Set-Content -Path $outputPath -Encoding UTF8

Write-Host "Test report: $outputPath" -ForegroundColor Green

return [pscustomobject]@{
    Summary    = $summary
    Rows       = $rows
    Issues     = $issues
    OutputPath = $outputPath
}
