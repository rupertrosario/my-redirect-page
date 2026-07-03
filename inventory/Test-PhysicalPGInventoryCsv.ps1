# Cohesity Physical PG Inventory - CSV validation helper
# Run this after Get-PhysicalPGInventory.ps1 creates the latest CSV files.

$ErrorActionPreference = "Stop"

$outDir = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$summaryCsv = Join-Path $outDir "Physical_PG_Summary_Latest.csv"
$detailCsv  = Join-Path $outDir "Physical_PG_Object_Detail_Latest.csv"

Write-Host "Checking CSV files..." -ForegroundColor Cyan

if (-not (Test-Path $summaryCsv)) {
    throw "Summary CSV not found: $summaryCsv"
}

if (-not (Test-Path $detailCsv)) {
    throw "Detail CSV not found: $detailCsv"
}

$summary = Import-Csv $summaryCsv
$detail  = Import-Csv $detailCsv

Write-Host ""
Write-Host "CSV row counts" -ForegroundColor Cyan
Write-Host "Summary rows: $($summary.Count)"
Write-Host "Detail rows : $($detail.Count)"

Write-Host ""
Write-Host "Summary sample" -ForegroundColor Cyan
$summary |
    Select-Object -First 5 PGKey, Cluster, PGName, PolicyName, ProtectionType, PGObjectCount, LastRunStatus, LastRunEndET |
    Format-Table -AutoSize

Write-Host ""
Write-Host "Detail sample" -ForegroundColor Cyan
$detail |
    Select-Object -First 5 PGKey, Cluster, PGName, ObjectName, IncludedPath, ExcludedPathsUnderIncludedPath, GlobalExcludePaths |
    Format-Table -AutoSize

Write-Host ""
Write-Host "Basic validation" -ForegroundColor Cyan

$summaryMissingPgKey = @($summary | Where-Object { [string]::IsNullOrWhiteSpace($_.PGKey) }).Count
$detailMissingPgKey  = @($detail  | Where-Object { [string]::IsNullOrWhiteSpace($_.PGKey) }).Count
$summaryMissingPg    = @($summary | Where-Object { [string]::IsNullOrWhiteSpace($_.Cluster) -or [string]::IsNullOrWhiteSpace($_.PGName) }).Count
$detailMissingPg     = @($detail  | Where-Object { [string]::IsNullOrWhiteSpace($_.Cluster) -or [string]::IsNullOrWhiteSpace($_.PGName) }).Count
$detailMissingObject = @($detail  | Where-Object { [string]::IsNullOrWhiteSpace($_.ObjectName) }).Count

[PSCustomObject]@{
    SummaryRows          = $summary.Count
    DetailRows           = $detail.Count
    SummaryMissingPGKey  = $summaryMissingPgKey
    DetailMissingPGKey   = $detailMissingPgKey
    SummaryMissingPGName = $summaryMissingPg
    DetailMissingPGName  = $detailMissingPg
    DetailMissingObject  = $detailMissingObject
} | Format-List

if ($summary.Count -eq 0) {
    Write-Host "WARNING: Summary CSV has zero rows." -ForegroundColor Yellow
}

if ($detail.Count -eq 0) {
    Write-Host "WARNING: Detail CSV has zero rows." -ForegroundColor Yellow
}

if ($summaryMissingPgKey -gt 0 -or $detailMissingPgKey -gt 0 -or $summaryMissingPg -gt 0 -or $detailMissingPg -gt 0 -or $detailMissingObject -gt 0) {
    Write-Host "Validation completed with warnings. Review missing values above." -ForegroundColor Yellow
}
else {
    Write-Host "Validation passed: required key fields are populated." -ForegroundColor Green
}
