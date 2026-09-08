# Cohesity Helios Alert Review / Resolution
# PowerShell 5.1 compatible
#
# Purpose:
#   1. Retrieve currently open alerts from Helios-managed clusters.
#   2. Match live alerts against the local Cohesity alert catalog CSV.
#   3. Export the review to CSV; alert rows are not displayed in the console.
#   4. Optionally preview or resolve:
#        - alerts whose latest occurrence is older than X days
#        - approved non-actionable alerts from Cohesity_NonActionable_Alerts.csv
#
# SAFETY - CURRENT LAB PHASE:
#   - Default mode is Review. Running this script with no parameters performs GETs only.
#   - Resolution is allowed ONLY for the single discovered cluster whose name contains DET3.
#   - Every other cluster is unavailable for write operations.
#   - Resolve modes are preview-only unless -Execute is explicitly supplied.
#   - No alert type/category exclusion is used for the review report.
#   - Any future review exclusion must be based on exact Alert Name only.
#   - One cluster GET failure/timeout does not stop the remaining clusters in Review mode.
#
# APIs used:
#   GET  https://helios.cohesity.com/v2/mcm/cluster-mgmt/info
#   GET  https://helios.cohesity.com/v2/alerts?maxAlerts=1000&alertStates=kOpen
#   POST https://helios.cohesity.com/v2/mcm/alerts/resolutions
#        POST is reachable only in a resolve mode with explicit -Execute.
#
# Authentication:
#   Uses the existing AES-encrypted Cohesity API key method.

param(
    [ValidateSet("Review", "ResolveOlderThanDays", "ResolveNonActionable", "ResolveAll")]
    [string]$Mode = "Review",

    [int]$OlderThanDays = 0,

    [switch]$Execute
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# ------------------------------------------------------------
# Paths / configuration
# ------------------------------------------------------------

$baseUrl             = "https://helios.cohesity.com"
$alertsCsv           = "X:\PowerShell\Cohesity_API_Scripts\Cohesity_alerts.csv"
$nonActionableCsv    = "X:\PowerShell\Cohesity_API_Scripts\Cohesity_NonActionable_Alerts.csv"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$csvDir              = "X:\PowerShell\Data\Cohesity\Alerts"
$maxAlerts           = 1000
$requestTimeoutSec   = 30

# HARD LAB SAFETY: only the single discovered cluster whose name contains
# this text is eligible for any resolution operation.
$labClusterPattern   = "DET3"

# ------------------------------------------------------------
# Validate mode / safety inputs
# ------------------------------------------------------------

$isResolveMode = $Mode -ne "Review"
$usesOldAgeRule = $Mode -eq "ResolveOlderThanDays" -or $Mode -eq "ResolveAll"
$usesNonActionableRules = $Mode -eq "ResolveNonActionable" -or $Mode -eq "ResolveAll"

if ($usesOldAgeRule -and $OlderThanDays -le 0) {
    throw "-OlderThanDays must be greater than 0 for mode '$Mode'."
}

if ($Execute -and -not $isResolveMode) {
    throw "-Execute is valid only with a resolve mode."
}

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

if ($usesNonActionableRules -and -not (Test-Path $nonActionableCsv -PathType Leaf)) {
    throw "Non-actionable alert CSV not found: $nonActionableCsv"
}

if (-not (Test-Path $csvDir)) {
    New-Item -ItemType Directory -Path $csvDir | Out-Null
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
# Helper: controlled Helios alert resolution POST
# ------------------------------------------------------------

function Invoke-CohesityAlertResolution {
    param(
        [Parameter(Mandatory)][string]$ClusterName,
        [Parameter(Mandatory)][string]$ApprovedLabClusterName,
        [Parameter(Mandatory)][object[]]$ResolvedAlerts,
        [Parameter(Mandatory)][string]$ResolutionText
    )

    # HARD SAFETY GATE: exact discovered DET3 cluster only.
    if ([string]::IsNullOrWhiteSpace($ApprovedLabClusterName) -or $ClusterName -ine $ApprovedLabClusterName) {
        throw "WRITE BLOCKED: Cluster '$ClusterName' is not the approved DET3 lab cluster."
    }

    if ($ClusterName -notmatch "(?i)$([regex]::Escape($labClusterPattern))") {
        throw "WRITE BLOCKED: Cluster '$ClusterName' does not contain '$labClusterPattern'."
    }

    if (-not $Execute) {
        throw "Internal safety check: resolution POST called without -Execute."
    }

    if ($ResolvedAlerts.Count -eq 0) {
        throw "Internal safety check: no alerts were supplied for resolution."
    }

    if ([string]::IsNullOrWhiteSpace($ResolutionText)) {
        throw "Internal safety check: resolution text is empty."
    }

    $resolutionUrl = "$baseUrl/v2/mcm/alerts/resolutions"

    $body = [ordered]@{
        resolutionName = $ResolutionText
        description    = $ResolutionText
        resolvedAlerts = @($ResolvedAlerts)
    } | ConvertTo-Json -Depth 6

    $headers = @{
        accept         = "application/json"
        "content-type" = "application/json"
        apiKey         = $apiKey
    }

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest `
            -Uri $resolutionUrl `
            -Headers $headers `
            -Method Post `
            -Body $body `
            -TimeoutSec $requestTimeoutSec `
            -UseBasicParsing `
            -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest `
            -Uri $resolutionUrl `
            -Headers $headers `
            -Method Post `
            -Body $body `
            -TimeoutSec $requestTimeoutSec `
            -ErrorAction Stop
    }

    if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 300) {
        throw "Alert resolution returned HTTP $($response.StatusCode)."
    }

    return $response
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
# Helper: calculate age from latest occurrence
# ------------------------------------------------------------
# OlderThanDays is intentionally based on latestTimestampUsecs, not the first
# occurrence. An alert that first appeared long ago but is still occurring now
# must not be treated as stale simply because its first occurrence is old.

function Get-LatestAlertAgeDays {
    param($LatestTimestampUsecs)

    if ($null -eq $LatestTimestampUsecs -or [string]::IsNullOrWhiteSpace([string]$LatestTimestampUsecs)) {
        return $null
    }

    try {
        $milliseconds = [int64]([decimal]$LatestTimestampUsecs / 1000)
        $latestUtc = [DateTimeOffset]::FromUnixTimeMilliseconds($milliseconds).UtcDateTime
        return [math]::Floor(((Get-Date).ToUniversalTime() - $latestUtc).TotalDays)
    }
    catch {
        return $null
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
# Helper: find best non-actionable rule
# ------------------------------------------------------------
# Supported RuleType values:
#   AlertName   - exact case-insensitive Alert Name
#   AlertType   - exact API alertType value
#   MatchString - literal case-insensitive text in Alert Details
#   Severity    - normalized severity match (for example KInfo/Info)
#
# Priority prevents a broad Severity rule from overriding a specific rule:
#   AlertName > AlertType > MatchString > Severity
# If multiple rules match at the same highest priority with different
# resolution text, the alert is marked ambiguous and is NOT resolved.

function Get-NonActionableRuleMatch {
    param(
        [Parameter(Mandatory)]$Item,
        [Parameter(Mandatory)][object[]]$Rules
    )

    $matches = @()

    foreach ($rule in $Rules) {
        $ruleType = ([string]$rule.RuleType).Trim()
        $ruleValue = ([string]$rule.RuleValue).Trim()
        $resolution = ([string]$rule.Resolution).Trim()

        if ([string]::IsNullOrWhiteSpace($ruleType) -or
            [string]::IsNullOrWhiteSpace($ruleValue) -or
            [string]::IsNullOrWhiteSpace($resolution)) {
            continue
        }

        $matched = $false
        $priority = 0

        switch -Regex ($ruleType) {
            '^(?i)AlertName$' {
                $priority = 4
                $matched = $Item.AlertName -ieq $ruleValue
            }
            '^(?i)AlertType$' {
                $priority = 3
                $matched = $Item.AlertType -ieq $ruleValue
            }
            '^(?i)MatchString$' {
                $priority = 2
                if (-not [string]::IsNullOrWhiteSpace($Item.AlertDetails)) {
                    $matched = $Item.AlertDetails.IndexOf($ruleValue, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
                }
            }
            '^(?i)Severity$' {
                $priority = 1
                $matched = (Normalize-Severity $Item.Severity) -eq (Normalize-Severity $ruleValue)
            }
        }

        if ($matched) {
            $matches += [pscustomobject]@{
                RuleType   = $ruleType
                RuleValue  = $ruleValue
                Resolution = $resolution
                Priority   = $priority
            }
        }
    }

    if ($matches.Count -eq 0) {
        return $null
    }

    $highestPriority = ($matches | Measure-Object -Property Priority -Maximum).Maximum
    $bestMatches = @($matches | Where-Object { $_.Priority -eq $highestPriority })
    $uniqueResolutions = @($bestMatches.Resolution | Sort-Object -Unique)

    if ($uniqueResolutions.Count -gt 1) {
        return [pscustomobject]@{
            Ambiguous  = $true
            RuleType   = ($bestMatches.RuleType | Sort-Object -Unique) -join "+"
            RuleValue  = ($bestMatches.RuleValue | Sort-Object -Unique) -join "+"
            Resolution = ""
        }
    }

    return [pscustomobject]@{
        Ambiguous  = $false
        RuleType   = ($bestMatches.RuleType | Sort-Object -Unique) -join "+"
        RuleValue  = ($bestMatches.RuleValue | Sort-Object -Unique) -join "+"
        Resolution = $uniqueResolutions[0]
    }
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
# Load approved non-actionable rules when requested
# ------------------------------------------------------------

$nonActionableRules = @()

if ($usesNonActionableRules) {
    $nonActionableRules = @(Import-Csv -Path $nonActionableCsv)

    if ($nonActionableRules.Count -eq 0) {
        throw "Non-actionable alert CSV is empty: $nonActionableCsv"
    }

    foreach ($column in @("RuleType", "RuleValue", "Resolution")) {
        if ($column -notin $nonActionableRules[0].PSObject.Properties.Name) {
            throw "Non-actionable CSV must contain RuleType, RuleValue, and Resolution columns."
        }
    }

    $allowedRuleTypes = @("AlertName", "AlertType", "MatchString", "Severity")

    foreach ($rule in $nonActionableRules) {
        $ruleType = ([string]$rule.RuleType).Trim()
        $ruleValue = ([string]$rule.RuleValue).Trim()
        $resolution = ([string]$rule.Resolution).Trim()

        if ([string]::IsNullOrWhiteSpace($ruleType) -or
            [string]::IsNullOrWhiteSpace($ruleValue) -or
            [string]::IsNullOrWhiteSpace($resolution)) {
            throw "Non-actionable CSV contains a blank RuleType, RuleValue, or Resolution."
        }

        if ($allowedRuleTypes -notcontains $ruleType) {
            throw "Unsupported non-actionable RuleType '$ruleType'. Allowed values: $($allowedRuleTypes -join ', ')."
        }
    }
}

# ------------------------------------------------------------
# Get Helios-managed clusters
# ------------------------------------------------------------

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   COHESITY ALERT REVIEW / RESOLUTION" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Mode       : $Mode"
Write-Host "Execute    : $Execute"
Write-Host "Catalog    : $alertsCsv"
Write-Host "GET timeout: $requestTimeoutSec seconds"
Write-Host "Write scope: DET3 lab cluster only" -ForegroundColor Yellow

try {
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

$det3Clusters = @($clusters | Where-Object {
    ([string]$_.clusterName) -match "(?i)$([regex]::Escape($labClusterPattern))"
})

$approvedLabClusterName = ""

if ($isResolveMode) {
    if ($det3Clusters.Count -eq 0) {
        throw "LAB SAFETY BLOCK: No Helios-managed cluster containing '$labClusterPattern' was found."
    }

    if ($det3Clusters.Count -gt 1) {
        throw "LAB SAFETY BLOCK: More than one cluster containing '$labClusterPattern' was found. Resolve mode requires exactly one DET3 lab target."
    }

    $approvedLabClusterName = ([string]$det3Clusters[0].clusterName).Trim()

    if ([string]::IsNullOrWhiteSpace($approvedLabClusterName)) {
        throw "LAB SAFETY BLOCK: DET3 cluster name is empty."
    }

    Write-Host "Lab target : $approvedLabClusterName" -ForegroundColor Yellow
}

# ------------------------------------------------------------
# Get open alerts
# ------------------------------------------------------------

$results = @()
$rawAlerts = @()
$failures = @()
$unmatchedRows = @()
$unmatchedCount = 0

# Review mode reads all clusters.
# Resolve modes read ONLY the single discovered DET3 lab cluster.
$clustersToRead = if ($isResolveMode) { $det3Clusters } else { $clusters }

foreach ($cluster in ($clustersToRead | Sort-Object clusterName)) {

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
        $alertResponse = Invoke-CohesityGet `
            -Uri $alertsUrl `
            -Headers $headers `
            -TimeoutSec $requestTimeoutSec

        if ($null -eq $alertResponse) {
            throw "No response content returned within the GET request."
        }
    }
    catch {
        $failures += [pscustomobject][ordered]@{
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
        $alertType = ([string]$alert.alertType).Trim()
        $severity = ([string]$alert.severity).Trim()
        $normalizedSeverity = Normalize-Severity $severity
        $normalizedAlertCode = $alertCode.ToUpperInvariant()
        $normalizedAlertName = $liveAlertName.ToUpperInvariant()

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

        $alertDetails = Get-AlertDetails $alert

        if (-not $matchedCatalogRow) {
            $unmatchedCount++

            $unmatchedRows += [pscustomobject][ordered]@{
                Cluster          = $clusterName
                "Raw Alert Type" = $alertType
                "Alert Code"     = $alertCode
                "Alert Name"     = $liveAlertName
                Severity          = $severity
                "Alert Details"  = $alertDetails
            }
        }

        $latestAgeDays = Get-LatestAlertAgeDays $alert.latestTimestampUsecs
        $alertIdStr = ([string]$alert.id).Trim()

        $rawAlerts += [pscustomobject]@{
            ClusterName          = $clusterName
            ClusterId            = $clusterId
            AlertIdStr           = $alertIdStr
            AlertType            = $alertType
            AlertCode            = $alertCode
            AlertName            = $liveAlertName
            Severity             = $severity
            AlertDetails         = $alertDetails
            LatestAgeDays        = $latestAgeDays
            FirstTimestampUsecs  = $alert.firstTimestampUsecs
            LatestTimestampUsecs = $alert.latestTimestampUsecs
        }

        $results += [pscustomobject][ordered]@{
            "Cluster"              = $clusterName
            "First Occurrence ET"  = Convert-UsecsToET $alert.firstTimestampUsecs
            "Latest Occurrence ET" = Convert-UsecsToET $alert.latestTimestampUsecs
            "Alert Type"           = if ($matchedCatalogRow) { $matchedCatalogRow.'Alert Type' } else { "UNMATCHED" }
            "Alert Code"           = $alertCode
            "Alert Name"           = if ($matchedCatalogRow) { $matchedCatalogRow.'Alert Name' } else { $liveAlertName }
            "Severity"             = $severity
            "Alert Details"        = $alertDetails
            "Reason"               = if ($matchedCatalogRow) { $matchedCatalogRow.Reason } else { "No matching row found in Cohesity_alerts.csv." }
            "Action"               = if ($matchedCatalogRow) { $matchedCatalogRow.Action } else { "Review manually." }
        }
    }
}

$resolveGetFailed = $isResolveMode -and $failures.Count -gt 0

# ------------------------------------------------------------
# Main review CSV export
# ------------------------------------------------------------

$reportdate = Get-Date -Format "yyyy-MM-dd_HHmm"
$csvFile = Join-Path $csvDir "Cohesity_Open_Alert_Review_${reportdate}.csv"

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
    $headerLine = ($csvColumns | ForEach-Object { '"' + ($_ -replace '"','""') + '"' }) -join ','
    Set-Content -Path $csvFile -Value $headerLine -Encoding UTF8
}

if (-not (Test-Path $csvFile -PathType Leaf)) {
    throw "CSV export failed: $csvFile was not created."
}

# ------------------------------------------------------------
# Diagnostics CSVs
# ------------------------------------------------------------

$unmatchedCsvFile = $null
$failureCsvFile = $null

if ($unmatchedRows.Count -gt 0) {
    $unmatchedCsvFile = Join-Path $csvDir "Cohesity_Alert_Unmatched_${reportdate}.csv"
    $unmatchedRows |
        Sort-Object Cluster, "Alert Code", "Alert Name" |
        Export-Csv -Path $unmatchedCsvFile -NoTypeInformation -Encoding UTF8
}

if ($failures.Count -gt 0) {
    $failureCsvFile = Join-Path $csvDir "Cohesity_Alert_GET_Failures_${reportdate}.csv"
    $failures |
        Sort-Object Cluster |
        Export-Csv -Path $failureCsvFile -NoTypeInformation -Encoding UTF8
}

if ($resolveGetFailed) {
    Write-Warning "DET3 alert retrieval failed. Review and failure CSVs were saved; no resolution operation was attempted."
    throw "LAB SAFETY BLOCK: Unable to retrieve DET3 alerts. No alert was resolved."
}

# ------------------------------------------------------------
# Resolve candidate selection - DET3 only
# ------------------------------------------------------------

$resolveCandidates = @()
$skippedCandidates = @()

if ($isResolveMode) {
    foreach ($item in $rawAlerts) {

        if ($item.ClusterName -ine $approvedLabClusterName) {
            continue
        }

        $selectedReasons = @()
        $resolutionText = ""
        $ruleType = ""
        $ruleValue = ""

        if ($usesOldAgeRule -and
            $null -ne $item.LatestAgeDays -and
            $item.LatestAgeDays -ge $OlderThanDays) {

            $selectedReasons += "OlderThanDays"
            $resolutionText = "NoActReq: Alert latest occurrence is older than $OlderThanDays days."
        }

        if ($usesNonActionableRules) {
            $ruleMatch = Get-NonActionableRuleMatch -Item $item -Rules $nonActionableRules

            if ($ruleMatch) {
                if ($ruleMatch.Ambiguous) {
                    $skippedCandidates += [pscustomobject]@{
                        Cluster                 = $item.ClusterName
                        AlertIdStr              = $item.AlertIdStr
                        AlertName               = $item.AlertName
                        AlertType               = $item.AlertType
                        Severity                = $item.Severity
                        LatestOccurrenceAgeDays = $item.LatestAgeDays
                        ReasonSelected          = "NonActionable"
                        RuleType                = $ruleMatch.RuleType
                        RuleValue               = $ruleMatch.RuleValue
                        Resolution              = ""
                        Result                  = "Skipped - Ambiguous Rule"
                        Error                   = "Multiple highest-priority rules matched with different resolution text."
                    }

                    continue
                }

                $selectedReasons += "NonActionable"
                $resolutionText = $ruleMatch.Resolution
                $ruleType = $ruleMatch.RuleType
                $ruleValue = $ruleMatch.RuleValue
            }
        }

        if ($selectedReasons.Count -eq 0) {
            continue
        }

        if ([string]::IsNullOrWhiteSpace($item.AlertIdStr)) {
            Write-Warning "Skipping '$($item.AlertName)' on '$($item.ClusterName)' because alertIdStr is empty."
            continue
        }

        if ([string]::IsNullOrWhiteSpace($item.AlertName)) {
            Write-Warning "Skipping alert '$($item.AlertIdStr)' on '$($item.ClusterName)' because Alert Name is empty."
            continue
        }

        if ([string]::IsNullOrWhiteSpace($resolutionText)) {
            Write-Warning "Skipping '$($item.AlertName)' on '$($item.ClusterName)' because resolution text is empty."
            continue
        }

        $resolveCandidates += [pscustomobject]@{
            Cluster                 = $item.ClusterName
            AlertIdStr              = $item.AlertIdStr
            AlertName               = $item.AlertName
            AlertType               = $item.AlertType
            Severity                = $item.Severity
            LatestOccurrenceAgeDays = $item.LatestAgeDays
            ReasonSelected          = ($selectedReasons -join "+")
            RuleType                = $ruleType
            RuleValue               = $ruleValue
            Resolution              = $resolutionText
            Result                  = if ($Execute) { "Pending" } else { "Would Resolve" }
            Error                   = ""
        }
    }
}

# ------------------------------------------------------------
# Execute resolution - exact DET3 target and explicit -Execute only
# ------------------------------------------------------------

if ($isResolveMode -and $Execute -and $resolveCandidates.Count -gt 0) {

    $groups = $resolveCandidates | Group-Object { $_.Resolution }

    foreach ($group in $groups) {
        $groupRows = @($group.Group)
        $clusterName = $groupRows[0].Cluster
        $resolutionText = $groupRows[0].Resolution

        $resolvedAlerts = @()
        foreach ($row in $groupRows) {
            $resolvedAlerts += [ordered]@{
                alertIdStr = [string]$row.AlertIdStr
                alertName  = [string]$row.AlertName
            }
        }

        try {
            $null = Invoke-CohesityAlertResolution `
                -ClusterName $clusterName `
                -ApprovedLabClusterName $approvedLabClusterName `
                -ResolvedAlerts $resolvedAlerts `
                -ResolutionText $resolutionText

            foreach ($row in $groupRows) {
                $row.Result = "Resolved"
            }
        }
        catch {
            foreach ($row in $groupRows) {
                $row.Result = "Failed"
                $row.Error = $_.Exception.Message
            }

            Write-Warning "Resolution failed on '$clusterName': $($_.Exception.Message)"
        }
    }
}

# ------------------------------------------------------------
# Resolution preview/audit CSV
# ------------------------------------------------------------

$resolveCsvFile = $null

if ($isResolveMode) {
    $resolveCsvFile = Join-Path $csvDir "Cohesity_Alert_Resolution_${Mode}_${reportdate}.csv"

    $resolveColumns = @(
        "Cluster",
        "AlertIdStr",
        "AlertName",
        "AlertType",
        "Severity",
        "LatestOccurrenceAgeDays",
        "ReasonSelected",
        "RuleType",
        "RuleValue",
        "Resolution",
        "Result",
        "Error"
    )

    $resolveRows = @(
        @($resolveCandidates) + @($skippedCandidates) |
        Select-Object -Property $resolveColumns
    )

    if ($resolveRows.Count -gt 0) {
        $resolveRows |
            Sort-Object Cluster, AlertName |
            Export-Csv -Path $resolveCsvFile -NoTypeInformation -Encoding UTF8
    }
    else {
        $headerLine = ($resolveColumns | ForEach-Object { '"' + ($_ -replace '"','""') + '"' }) -join ','
        Set-Content -Path $resolveCsvFile -Value $headerLine -Encoding UTF8
    }
}

# ------------------------------------------------------------
# Final status only - no alert rows displayed in console
# ------------------------------------------------------------

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   ALERT PROCESS COMPLETE" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Mode                    : $Mode"
Write-Host "Open alerts included    : $($results.Count)"
Write-Host "Catalog unmatched       : $unmatchedCount"
Write-Host "Cluster GET failures    : $($failures.Count)"
Write-Host "Saved review CSV        : $csvFile" -ForegroundColor Green

if ($unmatchedCsvFile) {
    Write-Host "Saved unmatched CSV     : $unmatchedCsvFile" -ForegroundColor Yellow
}

if ($failureCsvFile) {
    Write-Host "Saved GET failures CSV  : $failureCsvFile" -ForegroundColor Yellow
}

if ($isResolveMode) {
    Write-Host "Approved lab target     : $approvedLabClusterName" -ForegroundColor Yellow
    Write-Host "Resolve candidates      : $($resolveCandidates.Count)"
    Write-Host "Ambiguous rules skipped : $($skippedCandidates.Count)"
    Write-Host "Execute requested       : $Execute"
    Write-Host "Saved resolution CSV    : $resolveCsvFile" -ForegroundColor Green

    if (-not $Execute) {
        Write-Host "Resolution status       : PREVIEW ONLY - no POST was sent" -ForegroundColor Yellow
    }
    elseif ($resolveCandidates.Count -eq 0) {
        Write-Host "Resolution status       : No matching alerts - no POST was sent" -ForegroundColor Yellow
    }
}
