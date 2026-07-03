# =====================================================================
# Headless wrapper — Cohesity Physical PG Inventory
#
# Purpose:
# - Runs Get-PhysicalPGInventory.ps1 without manual prompt
# - Returns cluster selection automatically
# - Suppresses Out-GridView
# - Intended for Power BI Python refresh / scheduled local runs
#
# Default ClusterSelection = 0 means ALL clusters.
# No install. No Cohesity write actions. Underlying collector remains GET-only.
# =====================================================================

param(
    [string]$ClusterSelection = "0"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$inventoryScript = Join-Path $scriptDir "Get-PhysicalPGInventory.ps1"

if (-not (Test-Path -Path $inventoryScript -PathType Leaf)) {
    throw "Inventory script not found: $inventoryScript"
}

Write-Host "Running Physical PG inventory in headless mode..." -ForegroundColor Cyan
Write-Host "ClusterSelection: $ClusterSelection" -ForegroundColor Cyan
Write-Host "Script: $inventoryScript" -ForegroundColor DarkGray

# Preserve any existing function overrides in the session.
$hadReadHostFunction = Test-Path Function:\Read-Host
$hadOutGridViewFunction = Test-Path Function:\Out-GridView

$originalReadHost = $null
$originalOutGridView = $null

if ($hadReadHostFunction) {
    $originalReadHost = (Get-Command Read-Host -CommandType Function).ScriptBlock
}

if ($hadOutGridViewFunction) {
    $originalOutGridView = (Get-Command Out-GridView -CommandType Function).ScriptBlock
}

# Override Read-Host only for this run.
# The collector's cluster prompt receives the supplied ClusterSelection.
function global:Read-Host {
    param(
        [string]$Prompt
    )

    if ($Prompt -match 'Select cluster') {
        Write-Host "$Prompt $ClusterSelection" -ForegroundColor DarkGray
        return $ClusterSelection
    }

    return ""
}

# Suppress Out-GridView only for this run.
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
    & $inventoryScript
}
finally {
    Remove-Item Function:\Read-Host -ErrorAction SilentlyContinue
    Remove-Item Function:\Out-GridView -ErrorAction SilentlyContinue

    if ($hadReadHostFunction -and $null -ne $originalReadHost) {
        Set-Item Function:\Read-Host -Value $originalReadHost
    }

    if ($hadOutGridViewFunction -and $null -ne $originalOutGridView) {
        Set-Item Function:\Out-GridView -Value $originalOutGridView
    }
}

$summaryLatestCsv = Join-Path $scriptDir "Physical_PG_Summary_Latest.csv"
$detailLatestCsv  = Join-Path $scriptDir "Physical_PG_Object_Detail_Latest.csv"

if (-not (Test-Path -Path $summaryLatestCsv -PathType Leaf)) {
    throw "Expected summary CSV was not created: $summaryLatestCsv"
}

if (-not (Test-Path -Path $detailLatestCsv -PathType Leaf)) {
    throw "Expected detail CSV was not created: $detailLatestCsv"
}

Write-Host "Headless inventory completed." -ForegroundColor Green
Write-Host "Summary CSV: $summaryLatestCsv" -ForegroundColor Green
Write-Host "Detail CSV : $detailLatestCsv" -ForegroundColor Green
