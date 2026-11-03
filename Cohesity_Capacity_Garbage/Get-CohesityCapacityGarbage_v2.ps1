#requires -Version 5.1
<#
.SYNOPSIS
  Version 2: GET-ONLY Helios capacity and garbage collection report (AES API key).
.DESCRIPTION
  Reads /v2/mcm/cluster-mgmt/info, /irisservices/api/v1/public/stats/storage,
  and /irisservices/api/v1/public/statistics/timeSeriesStats.
  ALL Cohesity HTTP operations are GET only. No POST, PATCH, PUT, or DELETE.
  Uses the existing local AES helper and encrypted .enc file shown in your PowerShell script.
  Reclaimed metric uses MRCounters / bytes_value, entityId and entityName
  both ReclaimedBytesPhysical.
.EXAMPLE
  .\Get-CohesityCapacityGarbage_v2.ps1
.EXAMPLE
  .\Get-CohesityCapacityGarbage_v2.ps1 -OutputCsv C:\Temp\CohesityCapacityGC.csv
#>
[CmdletBinding()]
param(
    [string]$HeliosUrl = 'https://helios.cohesity.com',
    [string]$ApiKeyHelperPath = 'X:\PowerShell\Cohesity_API_Scripts\common\ApiKeyAesHelper.ps1',
    [string]$EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\common\secure\cohesity_apikey.enc',
    [string]$OutputCsv = (Join-Path (Get-Location).Path 'Cohesity_GC_Capacity_Details.csv'),
    [ValidateSet('latest','average')][string]$RollupFunction = 'latest',
    [ValidateRange(1,86400)][int]$RollupIntervalSecs = 30,
    [string]$GarbageSchema = 'ApolloV2ClusterStats',
    [string]$ReclaimedSchema = 'MRCounters'
)

$ErrorActionPreference = 'Stop'
# Same AES-based API key loading pattern as Cohesity_GC_Capacity_Details.ps1.
# Neither a plaintext apikey.txt nor an interactive key prompt is used.
if (-not (Test-Path -LiteralPath $ApiKeyHelperPath -PathType Leaf)) {
    throw "Missing API key helper: $ApiKeyHelperPath"
}
if (-not (Test-Path -LiteralPath $EncryptedApiKeyPath -PathType Leaf)) {
    throw "Missing encrypted API key: $EncryptedApiKeyPath"
}
. $ApiKeyHelperPath
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'AES key helper returned an empty Helios API key.'
}

$baseHeaders = @{ accept = 'application/json'; apiKey = $ApiKey }
$TiB = [math]::Pow(1024,4)
$GiB = [math]::Pow(1024,3)

function Invoke-HeliosRead {
    param([string]$Path,[hashtable]$Headers)
    Invoke-RestMethod -Uri ($HeliosUrl.TrimEnd('/') + $Path) -Headers $Headers -Method Get -TimeoutSec 60
}
function Get-LatestMetric {
    param(
        [string]$ClusterName,[string]$ClusterId,[hashtable]$Headers,
        [string]$Schema,[string]$Metric,[string]$CounterEntityId = '',
        [string]$Range = 'day'
    )
    # Use actual current UTC boundaries, not the fixed epoch values from the sample URLs.
    $endMsecs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $rangeMillis = if ($Range -eq 'week') { 7L * 24 * 60 * 60 * 1000 } else { 24L * 60 * 60 * 1000 }
    $startMsecs = $endMsecs - $rangeMillis
    if ($CounterEntityId) {
        # MRCounters URL: entityId=entityName=<counter>, metricName=bytes_value.
        # Cluster selection is applied via the accessClusterId header.
        $variants = @($CounterEntityId)
        $entityName = $CounterEntityId
    } else {
        # Original ApolloV2ClusterStats lookup remains unchanged.
        $safeName = $ClusterName -replace '\s+',''
        $variants = @(
            "$safeName+(ID+$ClusterId)",
            [uri]::EscapeDataString("$ClusterName (ID $ClusterId)"),
            [uri]::EscapeDataString("$ClusterName(ID $ClusterId)")
        ) | Select-Object -Unique
        $entityName = ''
    }
    $lastError = 'No data points returned'
    foreach ($entity in $variants) {
        $query = '?schemaName={0}&metricName={1}&entityId={2}&startTimeMsecs={3}&endTimeMsecs={4}&rollupFunction={5}&rollupIntervalSecs={6}&metricUnitType=0&range={7}' -f `
            [uri]::EscapeDataString($Schema),[uri]::EscapeDataString($Metric),
            $(if ($CounterEntityId) { [uri]::EscapeDataString($entity) } else { $entity }),
            $startMsecs,$endMsecs,$RollupFunction,$RollupIntervalSecs,$Range
        if ($CounterEntityId) { $query += '&entityName=' + [uri]::EscapeDataString($entityName) }
        try {
            $response = Invoke-HeliosRead -Path ("/irisservices/api/v1/public/statistics/timeSeriesStats"+$query) -Headers $Headers
            $points = @($response.dataPointVec | Where-Object { $null -ne $_ })
            if ($points.Count -eq 0) { continue }
            $withTimestamp = @($points | Where-Object { $null -ne $_.timestampMsecs -or $null -ne $_.timestamp })
            if ($withTimestamp.Count -gt 0) {
                $point = $withTimestamp | Sort-Object -Property @{Expression = {
                    if ($null -ne $_.timestampMsecs) { [double]$_.timestampMsecs } else { [double]$_.timestamp }
                }} | Select-Object -Last 1
            } else { $point = $points[-1] }
            $raw = $point.data.int64Value
            if ($null -eq $raw) { $raw = $point.data.doubleValue }
            if ($null -eq $raw) { $raw = $point.int64Value }
            if ($null -eq $raw) { $lastError = 'Latest data point has no numeric value'; continue }
            $number = 0.0
            if (-not [double]::TryParse([string]$raw,[Globalization.NumberStyles]::Any,[Globalization.CultureInfo]::InvariantCulture,[ref]$number)) {
                $lastError = "Non-numeric metric value: $raw"; continue
            }
            $timestamp = $null
            if ($null -ne $point.timestampMsecs) { $timestamp = [string]$point.timestampMsecs }
            elseif ($null -ne $point.timestamp) { $timestamp = [string]$point.timestamp }
            return [pscustomobject]@{ Bytes = $number; Timestamp = $timestamp; EntityId = $entity; Error = $null }
        } catch { $lastError = $_.Exception.Message }
    }
    return [pscustomobject]@{ Bytes = $null; Timestamp = $null; EntityId = $null; Error = $lastError }
}
function To-TiB([object]$Bytes) { if ($null -eq $Bytes) { return $null }; return [math]::Round([double]$Bytes / $TiB, 2) }
function To-GiB([object]$Bytes) { if ($null -eq $Bytes) { return $null }; return [math]::Round([double]$Bytes / $GiB, 2) }

$inventory = Invoke-HeliosRead -Path '/v2/mcm/cluster-mgmt/info' -Headers $baseHeaders
$clusters = @($inventory.cohesityClusters | Where-Object { $null -ne $_.clusterId } | Sort-Object clusterName)
if ($clusters.Count -eq 0) { throw 'No clusters returned by Helios.' }
Write-Host ("Retrieving capacity and GC statistics for all {0} Helios clusters." -f $clusters.Count) -ForegroundColor Cyan

$rows = foreach ($cluster in $clusters) {
    $id = [string]$cluster.clusterId
    $name = [string]$cluster.clusterName
    $headers = $baseHeaders.Clone()
    $headers['accessClusterId'] = $id
    Write-Host "Reading $name ($id)..." -ForegroundColor Cyan
    $storage = $null; $capacityError = $null
    try { $storage = Invoke-HeliosRead -Path '/irisservices/api/v1/public/stats/storage' -Headers $headers }
    catch { $capacityError = $_.Exception.Message }
    $total = $null; $used = $null; $available = $null; $percent = $null
    if ($null -ne $storage) {
        if ($null -ne $storage.totalCapacityBytes) { $total = [double]$storage.totalCapacityBytes }
        if ($null -ne $storage.localUsageBytes) { $used = [double]$storage.localUsageBytes }
        if ($null -ne $storage.localAvailableBytes) { $available = [double]$storage.localAvailableBytes }
        if ($null -ne $total -and $total -gt 0 -and $null -ne $used) { $percent = [math]::Round(100*$used/$total,2) }
        if ($null -eq $total -or $null -eq $used) { $capacityError = 'Missing totalCapacityBytes or localUsageBytes' }
    }
    $garbage = Get-LatestMetric -ClusterName $name -ClusterId $id -Headers $headers -Schema $GarbageSchema -Metric 'EstimatedGarbageBytes'
    $reclaimed = Get-LatestMetric -ClusterName $name -ClusterId $id -Headers $headers -Schema $ReclaimedSchema -Metric 'bytes_value' -CounterEntityId 'ReclaimedBytesPhysical' -Range 'day'
    [pscustomobject]@{
        Cluster = $name
        ClusterId = $id
        Garbage_Data_In_GB = (To-GiB $garbage.Bytes)
        Garbage_Data_In_TB = (To-TiB $garbage.Bytes)
        Reclaimed_Physical_In_GB = (To-GiB $reclaimed.Bytes)
        Reclaimed_Physical_In_TB = (To-TiB $reclaimed.Bytes)
        Total_Capacity_Bytes_TiB = (To-TiB $total)
        Local_Usage_Bytes_TiB = (To-TiB $used)
        Local_Available_Bytes_TiB = (To-TiB $available)
        Consumed = if ($null -ne $percent) { '{0}%' -f $percent } else { $null }
        Garbage_Timestamp = $garbage.Timestamp
        Reclaimed_Timestamp = $reclaimed.Timestamp
        CapacityError = $capacityError
        GarbageError = $garbage.Error
        ReclaimedError = $reclaimed.Error
    }
}
# Sort on numeric utilization while retaining the percentage sign in output/CSV.
$rows = @($rows | Sort-Object -Property @{ Expression = {
    if ($null -eq $_.Consumed) { -1 } else { [double]($_.Consumed -replace '%', '') }
} } -Descending)
# Print all original capacity columns and both GB/TB conversions without PowerShell truncation.
$displayColumns = @(
    'Cluster', 'Garbage_Data_In_GB', 'Garbage_Data_In_TB',
    'Reclaimed_Physical_In_GB', 'Reclaimed_Physical_In_TB',
    'Total_Capacity_Bytes_TiB', 'Local_Usage_Bytes_TiB',
    'Local_Available_Bytes_TiB', 'Consumed'
)
$rows | Format-Table -Property $displayColumns -AutoSize |
    Out-String -Width 260 | Write-Host
$folder = Split-Path -Parent $OutputCsv
if ($folder -and -not (Test-Path -LiteralPath $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
$rows | Export-Csv -LiteralPath $OutputCsv -NoTypeInformation -Encoding UTF8
Write-Host "CSV saved to: $OutputCsv" -ForegroundColor Green
