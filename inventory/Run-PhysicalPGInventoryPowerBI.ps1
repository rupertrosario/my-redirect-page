# =====================================================================
# Launcher — Cohesity Physical PG Inventory for Power BI Desktop
#
# Purpose:
# - Runs Get-PhysicalPGInventory.ps1
# - Suppresses Out-GridView during this launcher run
# - Opens the Power BI Desktop report if it exists
#
# No install. No write actions to Cohesity. The underlying inventory script is GET-only.
# =====================================================================

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$inventoryScript = Join-Path $scriptDir "Get-PhysicalPGInventory.ps1"
$powerBIReportPath = Join-Path $scriptDir "Physical_PG_Inventory.pbix"

if (-not (Test-Path -Path $inventoryScript -PathType Leaf)) {
    throw "Inventory script not found: $inventoryScript"
}

Write-Host "Running Cohesity Physical PG inventory collector..." -ForegroundColor Cyan
Write-Host "Script: $inventoryScript" -ForegroundColor DarkGray

# Suppress Out-GridView for this launcher run only.
# Function precedence is higher than cmdlet precedence in the current PowerShell session.
# The collector still creates the CSV files; this only prevents the grid windows from opening.
$originalOutGridView = $null
$hadOutGridViewFunction = Test-Path Function:\Out-GridView

if ($hadOutGridViewFunction) {
    $originalOutGridView = (Get-Command Out-GridView -CommandType Function).ScriptBlock
}

function global:Out-GridView {
    param(
        [Parameter(ValueFromPipeline = $true)]
        $InputObject,
        [string]$Title,
        [switch]$PassThru
    )

    begin { }
    process { }
    end { return $null }
}

try {
    . $inventoryScript
}
finally {
    Remove-Item Function:\Out-GridView -ErrorAction SilentlyContinue

    if ($hadOutGridViewFunction -and $null -ne $originalOutGridView) {
        Set-Item Function:\Out-GridView -Value $originalOutGridView
    }
}

Write-Host ""
Write-Host "Collector finished." -ForegroundColor Green

$summaryLatestCsv = Join-Path $scriptDir "Physical_PG_Summary_Latest.csv"
$detailLatestCsv  = Join-Path $scriptDir "Physical_PG_Object_Detail_Latest.csv"

Write-Host "Power BI Summary CSV: $summaryLatestCsv" -ForegroundColor Green
Write-Host "Power BI Detail CSV : $detailLatestCsv" -ForegroundColor Green

if (Test-Path -Path $powerBIReportPath -PathType Leaf) {
    Write-Host "Opening Power BI report: $powerBIReportPath" -ForegroundColor Cyan
    Start-Process -FilePath $powerBIReportPath
}
else {
    Write-Warning "Power BI report not found: $powerBIReportPath"
    Write-Warning "Create and save the PBIX with this exact name, then rerun this launcher."
}
