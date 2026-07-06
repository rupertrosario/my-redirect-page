# Validate Cohesity Protection Inventory CSV/JSON outputs
# PowerShell 5.1 compatible

$ErrorActionPreference = "Stop"

$outDir = "X:\PowerShell\Cohesity_API_Scripts\inventory"

$files = @(
    [PSCustomObject]@{ Name = "PG Summary";    Path = Join-Path $outDir "Cohesity_Protection_PG_Summary_Latest.csv" },
    [PSCustomObject]@{ Name = "Object Detail"; Path = Join-Path $outDir "Cohesity_Protection_Object_Detail_Latest.csv" },
    [PSCustomObject]@{ Name = "Path Detail";   Path = Join-Path $outDir "Cohesity_Protection_Path_Detail_Latest.csv" },
    [PSCustomObject]@{ Name = "Exceptions";    Path = Join-Path $outDir "Cohesity_Protection_Exceptions_Latest.csv" },
    [PSCustomObject]@{ Name = "Metadata";      Path = Join-Path $outDir "Cohesity_Protection_Run_Metadata.json" }
)

Write-Host ""
Write-Host "Checking output files..." -ForegroundColor Cyan

foreach ($file in $files) {
    if (Test-Path $file.Path) {
        Write-Host "FOUND   $($file.Name): $($file.Path)" -ForegroundColor Green
    }
    else {
        Write-Host "MISSING $($file.Name): $($file.Path)" -ForegroundColor Red
    }
}

$pgPath = Join-Path $outDir "Cohesity_Protection_PG_Summary_Latest.csv"
$objPath = Join-Path $outDir "Cohesity_Protection_Object_Detail_Latest.csv"
$pathPath = Join-Path $outDir "Cohesity_Protection_Path_Detail_Latest.csv"
$excPath = Join-Path $outDir "Cohesity_Protection_Exceptions_Latest.csv"
$metaPath = Join-Path $outDir "Cohesity_Protection_Run_Metadata.json"

if (-not (Test-Path $pgPath)) { throw "PG Summary CSV is missing." }
if (-not (Test-Path $objPath)) { throw "Object Detail CSV is missing." }
if (-not (Test-Path $pathPath)) { throw "Path Detail CSV is missing." }
if (-not (Test-Path $excPath)) { throw "Exceptions CSV is missing." }
if (-not (Test-Path $metaPath)) { throw "Metadata JSON is missing." }

$pgRows = @(Import-Csv $pgPath)
$objRows = @(Import-Csv $objPath)
$pathRows = @(Import-Csv $pathPath)
$excRows = @(Import-Csv $excPath)
$metadata = Get-Content -Path $metaPath -Raw | ConvertFrom-Json

$requiredPgColumns = @(
    "PGKey",
    "InventoryDateET",
    "Cluster",
    "ClusterId",
    "Environment",
    "ProtectionGroup",
    "ProtectionGroupId",
    "PolicyName",
    "PolicyId",
    "ObjectCount",
    "LastRunStatus",
    "IsPaused"
)

$requiredObjectColumns = @(
    "ObjectKey",
    "PGKey",
    "Cluster",
    "Environment",
    "ProtectionGroup",
    "ObjectName",
    "ObjectType",
    "ObjectId"
)

$requiredExceptionColumns = @(
    "InventoryDateET",
    "Cluster",
    "Environment",
    "ProtectionGroup",
    "ExceptionType",
    "Severity",
    "ExceptionReason",
    "RecommendedAction"
)

function Test-Columns {
    param(
        [string]$Name,
        $Rows,
        [string[]]$RequiredColumns
    )

    if (@($Rows).Count -eq 0) {
        Write-Host "$Name has zero rows. Column validation skipped." -ForegroundColor Yellow
        return
    }

    $columns = @($Rows[0].PSObject.Properties.Name)
    foreach ($col in $RequiredColumns) {
        if ($columns -contains $col) {
            Write-Host "$Name column OK      : $col" -ForegroundColor Green
        }
        else {
            Write-Host "$Name column MISSING : $col" -ForegroundColor Red
        }
    }
}

Write-Host ""
Write-Host "Row counts" -ForegroundColor Cyan
[PSCustomObject]@{
    PGSummaryRows    = $pgRows.Count
    ObjectDetailRows = $objRows.Count
    PathDetailRows   = $pathRows.Count
    ExceptionRows    = $excRows.Count
} | Format-List

Write-Host ""
Write-Host "Metadata" -ForegroundColor Cyan
$metadata | Format-List

Write-Host ""
Write-Host "Column validation" -ForegroundColor Cyan
Test-Columns -Name "PG Summary" -Rows $pgRows -RequiredColumns $requiredPgColumns
Test-Columns -Name "Object Detail" -Rows $objRows -RequiredColumns $requiredObjectColumns
Test-Columns -Name "Exceptions" -Rows $excRows -RequiredColumns $requiredExceptionColumns

Write-Host ""
Write-Host "Environment counts" -ForegroundColor Cyan
$pgRows | Group-Object Environment | Select-Object Name, Count | Sort-Object Name | Format-Table -AutoSize

Write-Host ""
Write-Host "PolicyName sanity check" -ForegroundColor Cyan
$policyIdLookingRows = @($pgRows | Where-Object { $_.PolicyName -eq $_.PolicyId -and -not [string]::IsNullOrWhiteSpace($_.PolicyName) })
Write-Host "PG rows where PolicyName still equals PolicyId: $($policyIdLookingRows.Count)" -ForegroundColor Yellow
if ($policyIdLookingRows.Count -gt 0) {
    $policyIdLookingRows | Select-Object Cluster, Environment, ProtectionGroup, PolicyName, PolicyId -First 10 | Format-Table -AutoSize
}

Write-Host ""
Write-Host "Validation complete." -ForegroundColor Green
