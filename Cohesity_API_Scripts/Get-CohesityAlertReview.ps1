# Cohesity Helios Open Alert Review
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Purpose:
#   1. Retrieve all currently open alerts from every Helios-managed cluster.
#   2. Match each live alert against the local Cohesity alert catalog CSV.
#   3. Add the catalog Reason and Action for review.
#   4. Export the combined result to CSV.
#
# IMPORTANT:
#   - This script does NOT resolve alerts.
#   - This script does NOT use POST, PUT, PATCH, or DELETE.
#   - All Cohesity API requests are GET only.
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
$alertsCsv           = "X:\PowerShell\Cohesity_API_Scripts\cohesit_alerts.csv"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$maxAlerts           = 1000
$outputCsv           = "X:\PowerShell\Cohesity_API_Scripts\cohesit_alert_review_{0}.csv" -f (Get-Date -Format "yyyyMMdd_HHmmss")

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
# Helper: GET only API wrapper
# ------------------------------------------------------------

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    # SAFETY: this function is intentionally hard-coded to GET.
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
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
# Helper: retrieve occurrence count only when returned by API
# ------------------------------------------------------------

function Get-AlertCount {
    param($Alert)

    foreach ($name in @(
        "occurrenceCount",
        "occurrencesCount",
        "numOccurrences",
        "numberOfOccurrences",
        "alertCount",
        "count"
    )) {
        $property = $Alert.PSObject.Properties[$name]
        if ($property -and $null -ne $property.Value -and [string]$property.Value -match "^\d+$") {
            return [int64]$property.Value
        }
    }

    $propertyCount = Get-AlertProperty -PropertyList $Alert.propertyList -Names @(
        "occurrence_count",
        "occurrenceCount",
        "num_occurrences",
        "numOccurrences",
        "numberOfOccurrences"
    )

    if ($propertyCount -match "^\d+$") {
        return [int64]$propertyCount
    }

    # Do not invent a count when the API does not return one.
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
            $Alert.alertDocument.alertCause
        )) {
            if (-not [string]::IsNullOrWhiteSpace([string]$value)) {
                $details += ([string]$value).Trim()
            }
        }
    }

    if ($Alert.PSObject.Properties["description"] -and -not [string]::IsNullOrWhiteSpace([string]$Alert.description)) {
        $details += ([string]$Alert.description).Trim()
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

# Build lookups once so every alert does not repeatedly scan all 512 catalog rows.
# Primary match = Alert Code + Severity.
# Secondary match = Alert Name + Severity.
$catalogByCodeSeverity = @{}
$catalogByNameSeverity = @{}
$catalogRowsByCode = @{}

foreach ($catalogRow in $catalog) {
    $catalogCode = ([string]$catalogRow.'Alert Code').Trim().ToUpperInvariant()
    $catalogName = ([string]$catalogRow.'Alert Name').Trim().ToUpperInvariant()
    $catalogSeverity = ([string]$catalogRow.Severity).Trim().ToUpperInvariant()

    if ($catalogCode) {
        $catalogByCodeSeverity["$catalogCode|$catalogSeverity"] = $catalogRow

        if (-not $catalogRowsByCode.ContainsKey($catalogCode)) {
            $catalogRowsByCode[$catalogCode] = @()
        }
        $catalogRowsByCode[$catalogCode] = @($catalogRowsByCode[$catalogCode]) + $catalogRow
    }

    if ($catalogName) {
        $catalogByNameSeverity["$catalogName|$catalogSeverity"] = $catalogRow
    }
}

# ------------------------------------------------------------
# Get Helios-managed clusters
# ------------------------------------------------------------

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   COHESITY OPEN ALERT REVIEW - GET ONLY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Catalog : $alertsCsv"

try {
    # GET only: retrieve Helios-managed clusters.
    $clusterResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-CohesityHeaders)
}
catch {
    throw "Unable to retrieve Helios clusters: $($_.Exception.Message)"
}

$clusters = @($clusterResponse.cohesityClusters)

if ($clusters.Count -eq 0) {
    throw "No clusters were returned by Helios."
}

Write-Host "Clusters: $($clusters.Count)"

# ------------------------------------------------------------
# Get open alerts from every cluster
# ------------------------------------------------------------

$results = @()
$failures = @()

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
        $alertResponse = Invoke-CohesityGet -Uri $alertsUrl -Headers $headers
    }
    catch {
        $failures += [pscustomobject]@{
            Cluster = $clusterName
            Error   = $_.Exception.Message
        }
        continue
    }

    $alerts = @($alertResponse.alerts)

    foreach ($alert in $alerts) {
        if ($null -eq $alert) { continue }

        $alertCode = ([string]$alert.alertCode).Trim()
        $severity = ([string]$alert.severity).Trim()
        $liveAlertName = ""

        if ($alert.alertDocument -and $alert.alertDocument.PSObject.Properties["alertName"]) {
            $liveAlertName = ([string]$alert.alertDocument.alertName).Trim()
        }

        # ----------------------------------------------------
        # Match live alert to the spreadsheet catalog
        # ----------------------------------------------------

        $matchedCatalogRow = $null
        $codeKey = "{0}|{1}" -f $alertCode.ToUpperInvariant(), $severity.ToUpperInvariant()

        if ($alertCode -and $catalogByCodeSeverity.ContainsKey($codeKey)) {
            $matchedCatalogRow = $catalogByCodeSeverity[$codeKey]
        }
        elseif ($alertCode) {
            $normalizedCode = $alertCode.ToUpperInvariant()

            # Safe fallback only when this alert code exists once in the catalog.
            if ($catalogRowsByCode.ContainsKey($normalizedCode) -and @($catalogRowsByCode[$normalizedCode]).Count -eq 1) {
                $matchedCatalogRow = @($catalogRowsByCode[$normalizedCode])[0]
            }
        }

        if (-not $matchedCatalogRow -and $liveAlertName) {
            $nameKey = "{0}|{1}" -f $liveAlertName.ToUpperInvariant(), $severity.ToUpperInvariant()

            if ($catalogByNameSeverity.ContainsKey($nameKey)) {
                $matchedCatalogRow = $catalogByNameSeverity[$nameKey]
            }
        }

        # ----------------------------------------------------
        # Extract node context from the live alert propertyList
        # ----------------------------------------------------

        $nodeId = Get-AlertProperty -PropertyList $alert.propertyList -Names @(
            "node_id",
            "nodeId"
        )

        $nodeIp = Get-AlertProperty -PropertyList $alert.propertyList -Names @(
            "node_ip",
            "nodeIp",
            "ip",
            "ipAddress",
            "host_ip",
            "source_ip",
            "sourceIp",
            "remote_ip",
            "remoteIp"
        )

        # ----------------------------------------------------
        # Build final review row
        # ----------------------------------------------------

        $results += [pscustomobject][ordered]@{
            "Cluster"              = $clusterName
            "First Occurrence ET"  = Convert-UsecsToET $alert.firstTimestampUsecs
            "Latest Occurrence ET" = Convert-UsecsToET $alert.latestTimestampUsecs
            "Count"                = Get-AlertCount $alert
            "Alert Type"           = if ($matchedCatalogRow) { $matchedCatalogRow.'Alert Type' } else { [string]$alert.alertType }
            "Alert Code"           = $alertCode
            "Alert Name"           = if ($matchedCatalogRow) { $matchedCatalogRow.'Alert Name' } else { $liveAlertName }
            "Severity"             = $severity
            "Node ID"              = $nodeId
            "Node IP"              = $nodeIp
            "Alert Details"        = Get-AlertDetails $alert
            "Reason"               = if ($matchedCatalogRow) { $matchedCatalogRow.Reason } else { "" }
            "Action"               = if ($matchedCatalogRow) { $matchedCatalogRow.Action } else { "" }
        }
    }
}

# ------------------------------------------------------------
# Export and display results
# ------------------------------------------------------------

$results = @($results | Sort-Object -Property @{ Expression = "Latest Occurrence ET"; Descending = $true }, Cluster, "Alert Code")

$results | Export-Csv -Path $outputCsv -NoTypeInformation -Encoding UTF8

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   ALERT REVIEW COMPLETE" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Open alerts : $($results.Count)"
Write-Host "Output      : $outputCsv"

if ($failures.Count -gt 0) {
    Write-Host "GET failures: $($failures.Count)" -ForegroundColor Yellow
    $failures | Format-Table Cluster, Error -AutoSize
}

if ($results.Count -gt 0) {
    $results | Format-Table -AutoSize
}
else {
    Write-Host "No open alerts returned." -ForegroundColor Green
}
