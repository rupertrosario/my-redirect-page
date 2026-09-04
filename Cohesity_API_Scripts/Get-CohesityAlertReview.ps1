# Cohesity Helios Open Alert Review
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Purpose:
#   1. Retrieve currently open alerts from every Helios-managed cluster.
#   2. Include all open alerts; no alert type or alert name is excluded.
#   3. Match live alerts against the local Cohesity alert catalog CSV.
#   4. Add catalog Reason and Action for later Claude Code review.
#   5. Export the complete review to CSV only; alert rows are not displayed.
#
# IMPORTANT:
#   - This script does NOT resolve alerts.
#   - This script does NOT use POST, PUT, PATCH, or DELETE.
#   - All Cohesity API requests are GET only.
#   - No alerts are excluded in the current version.
#   - If exclusions are added later, they must be based on exact Alert Name only.
#   - A cluster GET failure or timeout does NOT stop the remaining clusters.
#   - Numeric API alertType values are NOT written to the report.
#
# APIs used:
#   GET /v2/mcm/cluster-mgmt/info
#   GET /v2/alerts?maxAlerts=1000&alertStates=kOpen
#
# Authentication:
#   Uses the existing AES-encrypted Cohesity API key method already used by
#   the other Cohesity_API_Scripts scripts.

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# ------------------------------------------------------------
# Paths / configuration
# ------------------------------------------------------------

$baseUrl             = "https://helios.cohesity.com"
$alertsCsv           = "X:\PowerShell\Cohesity_API_Scripts\Cohesity_alerts.csv"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$maxAlerts           = 1000
$requestTimeoutSec   = 30

# ------------------------------------------------------------
# Validate local files
# ------------------------------------------------------------

if (-not (Test-Path $alertsCsv -PathType Leaf)) {
    throw "Alert catalog CSV not found: $alertsCsv"
}

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}

if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

# ------------------------------------------------------------
# Load encrypted Cohesity API key
# ------------------------------------------------------------

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath

if ([string]::IsNullOrWhiteSpace([string]$apiKey)) {
    throw "AES API key helper returned an empty API key."
}

# ------------------------------------------------------------
# Helper: build Cohesity headers
# ------------------------------------------------------------

function New-CohesityHeaders {
    param([string]$AccessClusterId)

    $headers = @{
        accept = "application/json"
        apiKey = $apiKey
    }

    if (-not [string]::IsNullOrWhiteSpace($AccessClusterId)) {
        $headers["accessClusterId"] = $AccessClusterId
    }

    return $headers
}

# ------------------------------------------------------------
# Helper: GET-only API wrapper
# ------------------------------------------------------------

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [int]$TimeoutSec = $requestTimeoutSec
    )

    # SAFETY: HTTP method is intentionally hard-coded to GET.
    # Timeout prevents one unreachable/hung cluster from blocking the run.
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -TimeoutSec $TimeoutSec `
            -UseBasicParsing `
            -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -TimeoutSec $TimeoutSec `
            -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace([string]$response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

# ------------------------------------------------------------
# Helper: convert Cohesity microsecond timestamp to US Eastern
# ------------------------------------------------------------

function Convert-UsecsToET {
    param($Usecs)

    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) {
        return ""
    }

    try {
        $milliseconds = [int64]([decimal]$Usecs / 1000)
        $utcDate = [DateTimeOffset]::FromUnixTimeMilliseconds($milliseconds)
        $easternTime = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        return ([TimeZoneInfo]::ConvertTime($utcDate, $easternTime)).ToString("yyyy-MM-dd HH:mm:ss")
    }
    catch {
        return ""
    }
}

# ------------------------------------------------------------
# Helper: retrieve a value from alert propertyList
# ------------------------------------------------------------

function Get-AlertProperty {
    param(
        $PropertyList,
        [string[]]$Names
    )

    foreach ($property in @($PropertyList)) {
        if ($null -eq $property) { continue }

        foreach ($name in $Names) {
            if ([string]$property.key -ieq $name) {
                if ($null -ne $property.value) {
                    if ($property.value -is [System.Array]) {
                        return (@($property.value) -join ", ")
                    }
                    return ([string]$property.value).Trim()
                }

                if ($null -ne $property.values) {
                    return (@($property.values) -join ", ")
                }
            }
        }
    }

    return ""
}

# ------------------------------------------------------------
# Helper: normalize API/catalog severity for reliable matching
# ------------------------------------------------------------

function Normalize-Severity {
    param($Severity)

    $value = ([string]$Severity).Trim()

    switch -Regex ($value) {
        '^(?i:k)?critical$'      { return "CRITICAL" }
        '^(?i:k)?warning$'       { return "WARNING" }
        '^(?i:k)?info$'          { return "INFORMATIONAL" }
        '^(?i:k)?informational$' { return "INFORMATIONAL" }
        default                  { return $value.ToUpperInvariant() }
    }
}

# ------------------------------------------------------------
# Helper: get live alert code from known API locations
# ------------------------------------------------------------

function Get-LiveAlertCode {
    param($Alert)

    foreach ($value in @(
        $Alert.alertCode,
        $Alert.alertDocument.alertCode,
        (Get-AlertProperty -PropertyList $Alert.propertyList -Names @("alert_code", "alertCode"))
    )) {
        if (-not [string]::IsNullOrWhiteSpace([string]$value)) {
            return ([string]$value).Trim()
        }
    }

    return ""
}

# ------------------------------------------------------------
# Helper: get live alert name from known API locations
# ------------------------------------------------------------

function Get-LiveAlertName {
    param($Alert)

    foreach ($value in @(
        $Alert.alertName,
        $Alert.name,
        $Alert.alertDocument.alertName,
        $Alert.alertDocument.name,
        (Get-AlertProperty -PropertyList $Alert.propertyList -Names @("alert_name", "alertName", "alertname"))
    )) {
        if (-not [string]::IsNullOrWhiteSpace([string]$value)) {
            return ([string]$value).Trim()
        }
    }

    return ""
}

# ------------------------------------------------------------
# Helper: combine useful live alert detail text
# ------------------------------------------------------------

function Get-AlertDetails {
    param($Alert)

    $details = @()

    if ($Alert.alertDocument) {
        foreach ($value in @(
            $Alert.alertDocument.alertDescription,
            $Alert.alertDocument.alertSummary,
            $Alert.alertDocument.alertCause,
            $Alert.alertDocument.description,
            $Alert.alertDocument.cause,
            $Alert.alertDocument.occurrence
        )) {
            if (-not [string]::IsNullOrWhiteSpace([string]$value)) {
                $details += ([string]$value).Trim()
            }
        }
    }

    foreach ($value in @(
        $Alert.description,
        $Alert.cause
    )) {
        if (-not [string]::IsNullOrWhiteSpace([string]$value)) {
            $details += ([string]$value).Trim()
        }
    }

    return (@($details | Select-Object -Unique) -join " | ")
}

# ------------------------------------------------------------
# Load Cohesity alert catalog CSV
# ------------------------------------------------------------

$catalog = @(Import-Csv -Path $alertsCsv)

if ($catalog.Count -eq 0) {
    throw "Alert catalog is empty: $alertsCsv"
}

$requiredColumns = @(
    "Alert Type",
    "Alert Code",
    "Alert Name",
    "Reason",
    "Alert Description",
    "Action",
    "Severity"
)

foreach ($requiredColumn in $requiredColumns) {
    if ($requiredColumn -notin $catalog[0].PSObject.Properties.Name) {
        throw "Alert catalog is missing required column: $requiredColumn"
    }
}

# ------------------------------------------------------------
# Build alert catalog lookups
# ------------------------------------------------------------
# Match order:
#   1. Alert Code + normalized Severity
#   2. Alert Name + normalized Severity
#   3. Alert Code only when unique in the catalog
#   4. Alert Name only when unique in the catalog

$catalogByCodeSeverity = @{}
$catalogByNameSeverity = @{}
$catalogRowsByCode = @{}
$catalogRowsByName = @{}

foreach ($catalogRow in $catalog) {
    $catalogCode = ([string]$catalogRow.'Alert Code').Trim().ToUpperInvariant()
    $catalogName = ([string]$catalogRow.'Alert Name').Trim().ToUpperInvariant()
    $catalogSeverity = Normalize-Severity $catalogRow.Severity

    if ($catalogCode) {
        $catalogByCodeSeverity["$catalogCode|$catalogSeverity"] = $catalogRow

        if (-not $catalogRowsByCode.ContainsKey($catalogCode)) {
            $catalogRowsByCode[$catalogCode] = @()
        }
        $catalogRowsByCode[$catalogCode] = @($catalogRowsByCode[$catalogCode]) + $catalogRow
    }

    if ($catalogName) {
        $catalogByNameSeverity["$catalogName|$catalogSeverity"] = $catalogRow

        if (-not $catalogRowsByName.ContainsKey($catalogName)) {
            $catalogRowsByName[$catalogName] = @()
        }
        $catalogRowsByName[$catalogName] = @($catalogRowsByName[$catalogName]) + $catalogRow
    }
}

# ------------------------------------------------------------
# Get Helios-managed clusters
# ------------------------------------------------------------

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   COHESITY OPEN ALERT REVIEW - GET ONLY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Catalog    : $alertsCsv"
Write-Host "Exclude    : None"
Write-Host "GET timeout: $requestTimeoutSec seconds"

try {
    # GET only: retrieve Helios-managed clusters.
    $clusterResponse = Invoke-CohesityGet `
        -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
        -Headers (New-CohesityHeaders) `
        -TimeoutSec $requestTimeoutSec
}
catch {
    throw "Unable to retrieve Helios clusters: $($_.Exception.Message)"
}

if ($null -eq $clusterResponse) {
    throw "Helios cluster list returned no response content."
}

$clusters = @($clusterResponse.cohesityClusters)

if ($clusters.Count -eq 0) {
    throw "No clusters were returned by Helios."
}

Write-Host "Clusters   : $($clusters.Count)"

# ------------------------------------------------------------
# Get open alerts from every cluster
# ------------------------------------------------------------

$results = @()
$failures = @()
$unmatchedCount = 0

foreach ($cluster in ($clusters | Sort-Object clusterName)) {

    $clusterName = ([string]$cluster.clusterName).Trim()
    $clusterId = ([string]$cluster.clusterId).Trim()

    if ([string]::IsNullOrWhiteSpace($clusterId)) {
        continue
    }

    if ([string]::IsNullOrWhiteSpace($clusterName)) {
        $clusterName = $clusterId
    }

    Write-Host "Getting open alerts: $clusterName"

    $headers = New-CohesityHeaders -AccessClusterId $clusterId
    $alertsUrl = "$baseUrl/v2/alerts?maxAlerts=$maxAlerts&alertStates=kOpen"

    try {
        # GET only: retrieve currently open alerts for this cluster.
        # If the cluster is unreachable or the request exceeds the timeout,
        # record the failure and continue with the next cluster.
        $alertResponse = Invoke-CohesityGet `
            -Uri $alertsUrl `
            -Headers $headers `
            -TimeoutSec $requestTimeoutSec

        if ($null -eq $alertResponse) {
            throw "No response content returned within the GET request."
        }
    }
    catch {
        $failures += [pscustomobject]@{
            Cluster   = $clusterName
            ClusterId = $clusterId
            Error     = $_.Exception.Message
        }

        Write-Warning "Skipping cluster '$clusterName': $($_.Exception.Message)"
        continue
    }

    $alerts = @($alertResponse.alerts)

    foreach ($alert in $alerts) {
        if ($null -eq $alert) { continue }

        $alertCode = Get-LiveAlertCode $alert
        $liveAlertName = Get-LiveAlertName $alert
        $severity = ([string]$alert.severity).Trim()
        $normalizedSeverity = Normalize-Severity $severity
        $normalizedAlertCode = $alertCode.ToUpperInvariant()
        $normalizedAlertName = $liveAlertName.ToUpperInvariant()

        # ----------------------------------------------------
        # Match live alert to the spreadsheet catalog
        # ----------------------------------------------------

        $matchedCatalogRow = $null

        if ($normalizedAlertCode) {
            $codeSeverityKey = "$normalizedAlertCode|$normalizedSeverity"
            if ($catalogByCodeSeverity.ContainsKey($codeSeverityKey)) {
                $matchedCatalogRow = $catalogByCodeSeverity[$codeSeverityKey]
            }
        }

        if (-not $matchedCatalogRow -and $normalizedAlertName) {
            $nameSeverityKey = "$normalizedAlertName|$normalizedSeverity"
            if ($catalogByNameSeverity.ContainsKey($nameSeverityKey)) {
                $matchedCatalogRow = $catalogByNameSeverity[$nameSeverityKey]
            }
        }

        if (-not $matchedCatalogRow -and $normalizedAlertCode) {
            if ($catalogRowsByCode.ContainsKey($normalizedAlertCode) -and @($catalogRowsByCode[$normalizedAlertCode]).Count -eq 1) {
                $matchedCatalogRow = @($catalogRowsByCode[$normalizedAlertCode])[0]
            }
        }

        if (-not $matchedCatalogRow -and $normalizedAlertName) {
            if ($catalogRowsByName.ContainsKey($normalizedAlertName) -and @($catalogRowsByName[$normalizedAlertName]).Count -eq 1) {
                $matchedCatalogRow = @($catalogRowsByName[$normalizedAlertName])[0]
            }
        }

        if (-not $matchedCatalogRow) {
            $unmatchedCount++
        }

        # ----------------------------------------------------
        # Build final review row
        # ----------------------------------------------------
        # Alert Type is taken ONLY from the catalog. Numeric API alertType IDs
        # are deliberately not written because they are not useful for review.

        $results += [pscustomobject][ordered]@{
            "Cluster"              = $clusterName
            "First Occurrence ET"  = Convert-UsecsToET $alert.firstTimestampUsecs
            "Latest Occurrence ET" = Convert-UsecsToET $alert.latestTimestampUsecs
            "Alert Type"           = if ($matchedCatalogRow) { $matchedCatalogRow.'Alert Type' } else { "UNMATCHED" }
            "Alert Code"           = $alertCode
            "Alert Name"           = if ($matchedCatalogRow) { $matchedCatalogRow.'Alert Name' } else { $liveAlertName }
            "Severity"             = $severity
            "Alert Details"        = Get-AlertDetails $alert
            "Reason"               = if ($matchedCatalogRow) { $matchedCatalogRow.Reason } else { "No matching row found in Cohesity_alerts.csv." }
            "Action"               = if ($matchedCatalogRow) { $matchedCatalogRow.Action } else { "Review manually." }
        }
    }
}

# ------------------------------------------------------------
# CSV Export
# ------------------------------------------------------------
# Same output pattern used by the existing Cohesity scripts.

$reportdate = Get-Date -Format "yyyy-MM-dd_HHmm"

$csvDir = "X:\PowerShell\Data\Cohesity\Alerts"
if (-not (Test-Path $csvDir)) {
    New-Item -ItemType Directory -Path $csvDir | Out-Null
}

$csvFile = Join-Path $csvDir "Cohesity_Open_Alert_Review_${reportdate}.csv"

# Exact CSV format used for later Claude Code review.
$csvColumns = @(
    "Cluster",
    "First Occurrence ET",
    "Latest Occurrence ET",
    "Alert Type",
    "Alert Code",
    "Alert Name",
    "Severity",
    "Alert Details",
    "Reason",
    "Action"
)

$csvRows = @($results | Select-Object -Property $csvColumns)

if ($csvRows.Count -gt 0) {
    $csvRows |
        Sort-Object Cluster, "Alert Code" |
        Export-Csv -Path $csvFile -NoTypeInformation -Encoding UTF8
}
else {
    # Still create a header-only CSV so the output file always exists.
    $headerLine = ($csvColumns | ForEach-Object { '"' + ($_ -replace '"','""') + '"' }) -join ','
    Set-Content -Path $csvFile -Value $headerLine -Encoding UTF8
}

if (-not (Test-Path $csvFile -PathType Leaf)) {
    throw "CSV export failed: $csvFile was not created."
}

# ------------------------------------------------------------
# Final status only - no alert data is displayed in the console
# ------------------------------------------------------------

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   ALERT REVIEW COMPLETE" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Open alerts included   : $($results.Count)"
Write-Host "Catalog unmatched      : $unmatchedCount"
Write-Host "Cluster GET failures   : $($failures.Count)"
Write-Host "Saved CSV report at    : $csvFile" -ForegroundColor Green