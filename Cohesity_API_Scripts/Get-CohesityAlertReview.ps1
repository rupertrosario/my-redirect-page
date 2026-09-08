# Cohesity Helios Alert Review / Resolution
# PowerShell 5.1 compatible
#
# Purpose:
#   1. Retrieve currently open alerts from Helios-managed clusters.
#   2. Match live alerts against the local Cohesity alert catalog CSV.
#   3. Export the review to CSV; alert rows are not displayed in the console.
#   4. Optionally preview or resolve:
#        - alerts older than X days
#        - approved non-actionable alerts
#
# SAFETY - CURRENT LAB PHASE:
#   - Default mode is Review. Running this script with no parameters performs GETs only.
#   - Resolution is allowed ONLY for a cluster name containing DET3.
#   - Every other cluster is blocked from write operations.
#   - Resolve modes are preview-only unless -Execute is explicitly supplied.
#   - -Execute is blocked until $labDomain is configured below.
#   - No alert type/category exclusion is used.
#   - Any future exclusion must be based on exact Alert Name only.
#   - One cluster GET failure/timeout does not stop the remaining clusters.
#
# APIs used:
#   GET  https://helios.cohesity.com/v2/mcm/cluster-mgmt/info
#   GET  https://helios.cohesity.com/v2/alerts?maxAlerts=1000&alertStates=kOpen
#   POST https://<DET3-LAB-CLUSTER>/irisservices/api/v1/public/alertResolutions
#        POST is reachable only when -Execute is explicitly supplied.
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
$maxAlerts           = 1000
$requestTimeoutSec   = 30

# LAB SAFETY: only a cluster whose name contains DET3 can be resolved.
$labClusterPattern   = "DET3"

# LAB DNS suffix used only for the direct-cluster alert resolution POST.
# Example only: "lab.example.com"
# DO NOT enable -Execute until this is replaced with the real DET3 lab domain.
$labDomain           = "CHANGE_ME"

# ------------------------------------------------------------
# Validate mode / safety inputs
# ------------------------------------------------------------

$isResolveMode = $Mode -ne "Review"

if (($Mode -eq "ResolveOlderThanDays" -or $Mode -eq "ResolveAll") -and $OlderThanDays -le 0) {
    throw "-OlderThanDays must be greater than 0 for mode '$Mode'."
}

if ($Execute -and -not $isResolveMode) {
    throw "-Execute is valid only with a resolve mode."
}

if ($Execute -and ([string]::IsNullOrWhiteSpace($labDomain) -or $labDomain -eq "CHANGE_ME")) {
    throw "LAB SAFETY BLOCK: Configure `$labDomain before using -Execute. No alert was resolved."
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

if (($Mode -eq "ResolveNonActionable" -or $Mode -eq "ResolveAll") -and -not (Test-Path $nonActionableCsv -PathType Leaf)) {
    throw "Non-actionable alert CSV not found: $nonActionableCsv"
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
# Helper: controlled direct-cluster alert resolution POST
# ------------------------------------------------------------

function Invoke-CohesityAlertResolution {
    param(
        [Parameter(Mandatory)][string]$ClusterName,
        [Parameter(Mandatory)][string[]]$AlertIds,
        [Parameter(Mandatory)][string]$ResolutionText
    )

    # HARD SAFETY GATE: DET3 is the only writable lab target at this stage.
    if ($ClusterName -notmatch "(?i)$([regex]::Escape($labClusterPattern))") {
        throw "WRITE BLOCKED: Cluster '$ClusterName' is not an approved DET3 lab cluster."
    }

    if (-not $Execute) {
        throw "Internal safety check: resolution POST called without -Execute."
    }

    if ([string]::IsNullOrWhiteSpace($labDomain) -or $labDomain -eq "CHANGE_ME") {
        throw "WRITE BLOCKED: `$labDomain is not configured."
    }

    $clusterHost = $ClusterName
    if ($clusterHost -notmatch '\.') {
        $clusterHost = "$ClusterName.$labDomain"
    }

    $resolutionUrl = "https://$clusterHost/irisservices/api/v1/public/alertResolutions"

    $body = @{
        alertIdList = @($AlertIds)
        resolutionDetails = @{
            resolutionDetails = $ResolutionText
            resolutionSummary = $ResolutionText
        }
    } | ConvertTo-Json -Depth 5

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
# Helper: calculate alert age in days
# ------------------------------------------------------------

function Get-AlertAgeDays {
    param($FirstTimestampUsecs)

    if ($null -eq $FirstTimestampUsecs -or [string]::IsNullOrWhiteSpace([string]$FirstTimestampUsecs)) {
        return $null
    }

    try {
        $milliseconds = [int64]([decimal]$FirstTimestampUsecs / 1000)
        $firstUtc = [DateTimeOffset]::FromUnixTimeMilliseconds($milliseconds).UtcDateTime
        return [math]::Floor(((Get-Date).ToUniversalTime() - $firstUtc).TotalDays)
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
# Load approved non-actionable alert names when requested
# ------------------------------------------------------------

$nonActionableByName = @{}

if ($Mode -eq "ResolveNonActionable" -or $Mode -eq "ResolveAll") {
    $nonActionableRows = @(Import-Csv -Path $nonActionableCsv)

    if ($nonActionableRows.Count -eq 0) {
        throw "Non-actionable alert CSV is empty: $nonActionableCsv"
    }

    if ("AlertName" -notin $nonActionableRows[0].PSObject.Properties.Name -or
        "Resolution" -notin $nonActionableRows[0].PSObject.Properties.Name) {
        throw "Non-actionable CSV must contain AlertName and Resolution columns."
    }

    foreach ($row in $nonActionableRows) {
        $name = ([string]$row.AlertName).Trim()
        $resolution = ([string]$row.Resolution).Trim()

        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $nonActionableByName[$name.ToUpperInvariant()] = $resolution
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

$det3Clusters = @($clusters | Where-Object { ([string]$_.clusterName) -match "(?i)$([regex]::Escape($labClusterPattern))" })

if ($isResolveMode) {
    if ($det3Clusters.Count -eq 0) {
        throw "LAB SAFETY BLOCK: No Helios-managed cluster containing '$labClusterPattern' was found."
    }

    if ($det3Clusters.Count -gt 1) {
        throw "LAB SAFETY BLOCK: More than one cluster containing '$labClusterPattern' was found. Resolve mode requires exactly one DET3 lab target."
    }

    Write-Host "Lab target : $($det3Clusters[0].clusterName)" -ForegroundColor Yellow
}

# ------------------------------------------------------------
# Get open alerts
# ------------------------------------------------------------

$results = @()
$rawAlerts = @()
$failures = @()
$unmatchedCount = 0

# Review mode reads all clusters exactly as before.
# Resolve modes read ONLY the discovered DET3 lab cluster.
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

        $alertAgeDays = Get-AlertAgeDays $alert.firstTimestampUsecs

        $rawAlerts += [pscustomobject]@{
            ClusterName       = $clusterName
            ClusterId         = $clusterId
            Alert             = $alert
            AlertId           = ([string]$alert.id).Trim()
            AlertCode         = $alertCode
            AlertName         = $liveAlertName
            NormalizedName    = $normalizedAlertName
            Severity          = $severity
            AgeDays           = $alertAgeDays
            FirstTimestampUsecs = $alert.firstTimestampUsecs
        }

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
# Main review CSV export
# ------------------------------------------------------------

$reportdate = Get-Date -Format "yyyy-MM-dd_HHmm"

$csvDir = "X:\PowerShell\Data\Cohesity\Alerts"
if (-not (Test-Path $csvDir)) {
    New-Item -ItemType Directory -Path $csvDir | Out-Null
}

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
# Resolve candidate selection - DET3 only
# ------------------------------------------------------------

$resolveCandidates = @()

if ($isResolveMode) {
    foreach ($item in $rawAlerts) {

        # Redundant hard guard: never classify another cluster for resolution.
        if ($item.ClusterName -notmatch "(?i)$([regex]::Escape($labClusterPattern))") {
            continue
        }

        $selectedReasons = @()
        $resolutionText = ""

        if (($Mode -eq "ResolveOlderThanDays" -or $Mode -eq "ResolveAll") -and
            $null -ne $item.AgeDays -and
            $item.AgeDays -ge $OlderThanDays) {

            $selectedReasons += "OlderThanDays"
            $resolutionText = "AutoResolve: Alert older than $OlderThanDays days."
        }

        if (($Mode -eq "ResolveNonActionable" -or $Mode -eq "ResolveAll") -and
            $item.NormalizedName -and
            $nonActionableByName.ContainsKey($item.NormalizedName)) {

            $selectedReasons += "NonActionable"

            $configuredResolution = [string]$nonActionableByName[$item.NormalizedName]
            if (-not [string]::IsNullOrWhiteSpace($configuredResolution)) {
                $resolutionText = $configuredResolution
            }
        }

        if ($selectedReasons.Count -gt 0) {
            if ([string]::IsNullOrWhiteSpace($item.AlertId)) {
                Write-Warning "Skipping '$($item.AlertName)' on '$($item.ClusterName)' because alert id is empty."
                continue
            }

            $resolveCandidates += [pscustomobject]@{
                Cluster        = $item.ClusterName
                AlertId        = $item.AlertId
                AlertName      = $item.AlertName
                Severity       = $item.Severity
                AgeDays        = $item.AgeDays
                ReasonSelected = ($selectedReasons -join "+")
                Resolution     = $resolutionText
                Result         = if ($Execute) { "Pending" } else { "Would Resolve" }
                Error          = ""
            }
        }
    }
}

# ------------------------------------------------------------
# Execute resolution - DET3 only and explicit -Execute only
# ------------------------------------------------------------

if ($isResolveMode -and $Execute -and $resolveCandidates.Count -gt 0) {

    # Group by cluster + resolution text because the direct API accepts
    # multiple alert IDs under one resolution message.
    $groups = $resolveCandidates | Group-Object { "$($_.Cluster)|$($_.Resolution)" }

    foreach ($group in $groups) {
        $groupRows = @($group.Group)
        $clusterName = $groupRows[0].Cluster
        $resolutionText = $groupRows[0].Resolution
        $alertIds = @($groupRows.AlertId)

        try {
            $null = Invoke-CohesityAlertResolution `
                -ClusterName $clusterName `
                -AlertIds $alertIds `
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
        "AlertId",
        "AlertName",
        "Severity",
        "AgeDays",
        "ReasonSelected",
        "Resolution",
        "Result",
        "Error"
    )

    $resolveRows = @($resolveCandidates | Select-Object -Property $resolveColumns)

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

if ($isResolveMode) {
    Write-Host "DET3 candidates         : $($resolveCandidates.Count)"
    Write-Host "Execute requested       : $Execute"
    Write-Host "Saved resolution CSV    : $resolveCsvFile" -ForegroundColor Green

    if (-not $Execute) {
        Write-Host "Resolution status       : PREVIEW ONLY - no POST was sent" -ForegroundColor Yellow
    }
}
