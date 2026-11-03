#requires -Version 5.1
<#
.SYNOPSIS
  Read-only Helios cluster capacity and garbage metrics report.
.DESCRIPTION
  Reads /v2/mcm/cluster-mgmt/info, /irisservices/api/v1/public/stats/storage,
  and /irisservices/api/v1/public/statistics/timeSeriesStats.
  No Cohesity POST, PATCH, PUT, or DELETE requests are made.
  Metric ReclaimedBytesPhysical / ApolloV2ClusterStats must be validated against
  a raw API response for your cluster/version before interpreting the figures.
.EXAMPLE
  $env:COHESITY_API_KEY = '<key from your secure local session>'
  .\Get-CohesityCapacityGarbage.ps1
.EXAMPLE
  .\Get-CohesityCapacityGarbage.ps1 -AllClusters -OutputCsv C:\Temp\CohesityCapacityGC.csv
#>
[CmdletBinding()]
param(
    [string]$HeliosUrl = 'https://helios.cohesity.com',
    [string]$ApiKey = $env:COHESITY_API_KEY,
    [switch]$AllClusters,
    [string]$OutputCsv = (Join-Path (Get-Location).Path ("Cohesity_Capacity_GC_{0}.csv" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))),
    [ValidateSet('latest','average')][string]$RollupFunction = 'latest',
    [ValidateRange(1,86400)][int]$RollupIntervalSecs = 30,
    [string]$GarbageSchema = 'ApolloV2ClusterStats',
    [string]$ReclaimedSchema = 'ApolloV2ClusterStats',
    [string]$ReclaimedMetric = 'ReclaimedBytesPhysical'
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $secureKey = Read-Host 'Enter Cohesity Helios API key (not stored)' -AsSecureString
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureKey)
    try { $ApiKey = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
}
if ([string]::IsNullOrWhiteSpace($ApiKey)) { throw 'Missing Helios API key.' }

$baseHeaders = @{ accept = 'application/json'; apiKey = $ApiKey }
$TiB = [math]::Pow(1024,4)
$GiB = [math]::Pow(1024,3)

function Invoke-HeliosRead {
    param([string]$Path,[hashtable]$Headers)
    Invoke-RestMethod -Uri ($HeliosUrl.TrimEnd('/') + $Path) -Headers $Headers -Method Get -TimeoutSec 60
}
function Get-LatestMetric {
    param([string]$ClusterName,[string]$ClusterId,[hashtable]$Headers,[string]$Schema,[string]$Metric)
    $safeName = $ClusterName -replace '\s+',''
    # Preserve the entity-ID alternatives used in the existing PowerShell implementation.
    $variants = @(
        "$safeName+(ID+$ClusterId)",
        [uri]::EscapeDataString("$ClusterName (ID $ClusterId)"),
        [uri]::EscapeDataString("$ClusterName(ID $ClusterId)")
    ) | Select-Object -Unique
    $lastError = 'No data points returned'
    foreach ($entity in $variants) {
        $query = '?schemaName={0}&metricName={1}&startTimeMsecs=2&entityId={2}&rollupFunction={3}&rollupIntervalSecs={4}&metricUnitType=0&range=day' -f `
            [uri]::EscapeDataString($Schema),[uri]::EscapeDataString($Metric),$entity,$RollupFunction,$RollupIntervalSecs
        try {
            $response = Invoke-HeliosRead -Path ("/irisservices/api/v1/public/statistics/timeSeriesStats"+$query) -Headers $Headers
            $points = @($response.dataPointVec | Where-Object { $null -ne $_ })
            if ($points.Count -eq 0) { continue }
            # Do not assume API returns points in chronological order when timestamps exist.
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
if (-not $AllClusters) {
    Write-Host 'Select cluster (0 = all clusters):' -ForegroundColor Cyan
    for ($i=0; $i -lt $clusters.Count; $i++) { Write-Host ('{0,3}. {1}' -f ($i+1),$clusters[$i].clusterName) }
    do { $selection = Read-Host 'Cluster number'; $selectedNumber = 0; $valid = [int]::TryParse($selection,[ref]$selectedNumber) -and $selectedNumber -ge 0 -and $selectedNumber -le $clusters.Count } until ($valid)
    if ($selectedNumber -gt 0) { $clusters = @($clusters[$selectedNumber-1]) }
}

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
    $reclaimed = Get-LatestMetric -ClusterName $name -ClusterId $id -Headers $headers -Schema $ReclaimedSchema -Metric $ReclaimedMetric
    [pscustomobject]@{
        Cluster=$name; ClusterId=$id
        TotalTiB=(To-TiB $total); UsedTiB=(To-TiB $used); AvailableTiB=(To-TiB $available); UsedPercent=$percent
        EstimatedGarbageGiB=(To-GiB $garbage.Bytes); EstimatedGarbageTiB=(To-TiB $garbage.Bytes)
        ReclaimedPhysicalGiB=(To-GiB $reclaimed.Bytes); ReclaimedPhysicalTiB=(To-TiB $reclaimed.Bytes)
        GarbageTimestamp=$garbage.Timestamp; ReclaimedTimestamp=$reclaimed.Timestamp
        CapacityError=$capacityError; GarbageError=$garbage.Error; ReclaimedError=$reclaimed.Error
    }
}
$rows | Format-Table Cluster,TotalTiB,UsedTiB,AvailableTiB,UsedPercent,EstimatedGarbageGiB,ReclaimedPhysicalGiB -AutoSize
$folder = Split-Path -Parent $OutputCsv
if ($folder -and -not (Test-Path -LiteralPath $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
$rows | Export-Csv -LiteralPath $OutputCsv -NoTypeInformation -Encoding UTF8
Write-Host "CSV saved to: $OutputCsv" -ForegroundColor Green
Write-Warning 'ReclaimedBytesPhysical counter semantics (interval vs cumulative) and schema must be confirmed on your cluster before interpreting its value as reclaimed during the last day.'
