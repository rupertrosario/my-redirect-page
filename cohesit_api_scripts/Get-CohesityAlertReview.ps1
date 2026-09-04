[CmdletBinding()]
param(
    [string]$BaseUri = 'https://helios.cohesity.com',
    [string]$ApiKey = $env:COHESITY_API_KEY,
    [string]$AlertsCsv = (Join-Path $PSScriptRoot 'cohesit_alerts.csv'),
    [int]$MaxAlerts = 1000,
    [string]$OutputCsv = (Join-Path $PSScriptRoot ("cohesit_alert_review_{0}.csv" -f (Get-Date -Format 'yyyyMMdd_HHmmss')))
)

$ErrorActionPreference = 'Stop'

# GET-only review script. All Cohesity HTTP calls go through this function.
function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )
    Invoke-RestMethod -Uri $Uri -Headers $Headers -Method Get -ContentType 'application/json'
}

function Convert-UsecsToET {
    param($Usecs)
    if (-not $Usecs -or "$Usecs" -notmatch '^\d+$') { return '' }

    $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([decimal]$Usecs / 1000))
    $tz = $null
    foreach ($id in @('Eastern Standard Time', 'America/New_York')) {
        try { $tz = [TimeZoneInfo]::FindSystemTimeZoneById($id); break } catch { }
    }
    if (-not $tz) { return $utc.UtcDateTime.ToString('yyyy-MM-dd HH:mm:ss') }
    ([TimeZoneInfo]::ConvertTime($utc, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
}

function Get-AlertProperty {
    param($PropertyList, [string[]]$Keys)
    foreach ($p in @($PropertyList)) {
        if (-not $p) { continue }
        $key = "$($p.key)".Trim()
        foreach ($wanted in $Keys) {
            if ($key -ieq $wanted) {
                $value = if ($null -ne $p.value) { $p.value } else { $p.values }
                if ($value -is [array]) { return (($value | ForEach-Object { "$_" }) -join ', ') }
                return "$value".Trim()
            }
        }
    }
    ''
}

function Get-OccurrenceCount {
    param($Alert)

    foreach ($value in @(
        $Alert.occurrenceCount,
        $Alert.occurrencesCount,
        $Alert.numOccurrences,
        $Alert.numberOfOccurrences,
        $Alert.alertCount,
        $Alert.count,
        $Alert.alertDocument.occurrenceCount,
        $Alert.alertDocument.numOccurrences,
        $Alert.alertDocument.numberOfOccurrences
    )) {
        if ($null -ne $value -and "$value" -match '^\d+$') { return [int64]$value }
    }

    $fromProperties = Get-AlertProperty $Alert.propertyList @(
        'occurrence_count','occurrenceCount','num_occurrences','numOccurrences','numberOfOccurrences'
    )
    if ($fromProperties -match '^\d+$') { return [int64]$fromProperties }

    foreach ($text in @(
        $Alert.alertDocument.occurrence,
        $Alert.alertDocument.alertOccurrence,
        $Alert.alertDocument.alertSummary,
        $Alert.alertDocument.alertDescription
    )) {
        if ("$text" -match '(?i)total\s+(\d+)\s+time') { return [int64]$Matches[1] }
    }

    ''
}

function Get-LiveAlertDetails {
    param($Alert)
    $parts = @(
        $Alert.alertDocument.alertDescription,
        $Alert.alertDocument.alertSummary,
        $Alert.alertDocument.alertCause,
        $Alert.description,
        $Alert.cause
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace("$_") } | ForEach-Object { "$_".Trim() } | Select-Object -Unique

    $parts -join ' | '
}

function Get-CatalogKey {
    param($Code, $Severity)
    ("{0}|{1}" -f "$Code".Trim().ToUpperInvariant(), "$Severity".Trim().ToUpperInvariant())
}

if (-not (Test-Path -LiteralPath $AlertsCsv)) {
    throw "Alert catalog not found: $AlertsCsv"
}

$catalog = @(Import-Csv -LiteralPath $AlertsCsv)
if ($catalog.Count -eq 0) { throw "Alert catalog is empty: $AlertsCsv" }

$requiredColumns = @('Alert Type','Alert Code','Alert Name','Reason','Alert Description','Action','Severity')
$missing = @($requiredColumns | Where-Object { $_ -notin $catalog[0].PSObject.Properties.Name })
if ($missing.Count -gt 0) {
    throw "Alert catalog is missing required column(s): $($missing -join ', ')"
}

$catalogByCodeSeverity = @{}
$catalogByNameSeverity = @{}
$codeRows = @{}
foreach ($row in $catalog) {
    $code = "$($row.'Alert Code')".Trim().ToUpperInvariant()
    $name = "$($row.'Alert Name')".Trim().ToUpperInvariant()
    $severity = "$($row.Severity)".Trim().ToUpperInvariant()

    if ($code) {
        $catalogByCodeSeverity["$code|$severity"] = $row
        if (-not $codeRows.ContainsKey($code)) { $codeRows[$code] = @() }
        $codeRows[$code] = @($codeRows[$code]) + $row
    }
    if ($name) { $catalogByNameSeverity["$name|$severity"] = $row }
}

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $secure = Read-Host 'Enter Cohesity Helios API key' -AsSecureString
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try { $ApiKey = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
}
if ([string]::IsNullOrWhiteSpace($ApiKey)) { throw 'No Cohesity API key supplied.' }

$BaseUri = $BaseUri.TrimEnd('/')
$commonHeaders = @{ accept = 'application/json'; apiKey = $ApiKey }

Write-Host 'Getting Helios cluster list...'
$clusterResponse = Invoke-CohesityGet "$BaseUri/v2/mcm/cluster-mgmt/info" $commonHeaders
$clusters = @($clusterResponse.cohesityClusters)
if ($clusters.Count -eq 0) { throw 'No clusters returned by Helios.' }

$results = [System.Collections.Generic.List[object]]::new()
$failedClusters = [System.Collections.Generic.List[string]]::new()

foreach ($cluster in $clusters) {
    $clusterId = "$($cluster.clusterId)".Trim()
    $clusterName = "$($cluster.clusterName)".Trim()
    if (-not $clusterId) { continue }
    if (-not $clusterName) { $clusterName = $clusterId }

    $headers = @{ accept = 'application/json'; apiKey = $ApiKey; accessClusterId = $clusterId }
    $uri = "$BaseUri/v2/alerts?maxAlerts=$MaxAlerts&alertStates=kOpen"

    try {
        $response = Invoke-CohesityGet $uri $headers
    }
    catch {
        $failedClusters.Add("$clusterName ($clusterId): $($_.Exception.Message)")
        continue
    }

    foreach ($alert in @($response.alerts)) {
        if (-not $alert) { continue }

        $alertCode = "$($alert.alertCode)".Trim()
        $severity = "$($alert.severity)".Trim()
        $liveAlertName = "$($alert.alertDocument.alertName)".Trim()

        $catalogRow = $null
        $codeKey = Get-CatalogKey $alertCode $severity
        if ($catalogByCodeSeverity.ContainsKey($codeKey)) {
            $catalogRow = $catalogByCodeSeverity[$codeKey]
        }
        elseif ($alertCode) {
            $normalizedCode = $alertCode.ToUpperInvariant()
            if ($codeRows.ContainsKey($normalizedCode) -and @($codeRows[$normalizedCode]).Count -eq 1) {
                $catalogRow = @($codeRows[$normalizedCode])[0]
            }
        }
        if (-not $catalogRow -and $liveAlertName) {
            $nameKey = Get-CatalogKey $liveAlertName $severity
            if ($catalogByNameSeverity.ContainsKey($nameKey)) { $catalogRow = $catalogByNameSeverity[$nameKey] }
        }

        $nodeId = Get-AlertProperty $alert.propertyList @('node_id','nodeId')
        $nodeIp = Get-AlertProperty $alert.propertyList @(
            'node_ip','nodeIp','ip','ipAddress','host_ip','source_ip','sourceIp','remote_ip','remoteIp'
        )

        $results.Add([pscustomobject][ordered]@{
            'Cluster'              = $clusterName
            'First Occurrence ET'  = Convert-UsecsToET $(if ($alert.firstTimestampUsecs) { $alert.firstTimestampUsecs } else { $alert.firstOccurrenceUsecs })
            'Latest Occurrence ET' = Convert-UsecsToET $(if ($alert.latestTimestampUsecs) { $alert.latestTimestampUsecs } else { $alert.lastOccurrenceUsecs })
            'Count'                = Get-OccurrenceCount $alert
            'Alert Type'           = if ($catalogRow) { $catalogRow.'Alert Type' } else { "$($alert.alertType)" }
            'Alert Code'           = $alertCode
            'Alert Name'           = if ($catalogRow) { $catalogRow.'Alert Name' } else { $liveAlertName }
            'Severity'             = $severity
            'Node ID'              = $nodeId
            'Node IP'              = $nodeIp
            'Alert Details'        = Get-LiveAlertDetails $alert
            'Reason'               = if ($catalogRow) { $catalogRow.Reason } else { '' }
            'Action'               = if ($catalogRow) { $catalogRow.Action } else { '' }
        })
    }
}

$sorted = @($results | Sort-Object -Property @{Expression='Latest Occurrence ET';Descending=$true}, Cluster, 'Alert Code')
$sorted | Export-Csv -LiteralPath $OutputCsv -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host "Open alerts: $($sorted.Count)"
Write-Host "Catalog:     $AlertsCsv"
Write-Host "Output:      $OutputCsv"

if ($failedClusters.Count -gt 0) {
    Write-Warning "GET failed for $($failedClusters.Count) cluster(s):"
    $failedClusters | ForEach-Object { Write-Warning $_ }
}

$sorted | Format-Table -AutoSize
