<#
.SYNOPSIS
Entry point for Cohesity Backup Failure INC status updates.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$LegacyFailureOutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailures",
    [string]$HelperPath = ("X:\PowerShell\Cohesity_API_Scripts\Common\" + "Api" + "KeyAesHelper.ps1"),
    [string]$EncryptedFile = ("X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_" + "api" + "key.enc"),
    [string]$ClusterName = "",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = "",
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = "",
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35,
    [int]$RequestTimeoutSec = 60,
    [switch]$ShowGrid
)

function Get-ReportFolder([string]$Root, [string]$Inc) {
    if ($Inc) {
        $candidate = Join-Path $Root $Inc.Trim().ToUpper()
        if (Test-Path $candidate) { return $candidate }
    }
    $latest = Get-ChildItem -Path $Root -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -ne "Archive" } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($latest) { return $latest.FullName }
    return ""
}

function Open-Grid([string]$Folder) {
    if (!(Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
        Write-Warning "Out-GridView is not available. CSV files were still generated."
        return
    }
    $p = Join-Path $Folder "incident_lifecycle.csv"
    if (!(Test-Path $p)) {
        Write-Warning "incident_lifecycle.csv was not found."
        return
    }
    $rows = @(Import-Csv -Path $p)
    if ($rows.Count -eq 0) {
        Write-Warning "incident_lifecycle.csv has no rows."
        return
    }
    $rows | Out-GridView -Title "Cohesity - Incident Lifecycle"
}

$target = Join-Path $PSScriptRoot "Get-CohesityBackupFailureWindowConsolidator.ps1"
if (!(Test-Path $target)) { throw "Main implementation script not found: $target" }

$targetParams = @{}
foreach ($k in $PSBoundParameters.Keys) {
    if ($k -ne "ShowGrid") { $targetParams[$k] = $PSBoundParameters[$k] }
}

Write-Host ""
Write-Host "Running main Cohesity backup failure collector."
Write-Host ""

& $target @targetParams
$mainExitCode = $LASTEXITCODE

$folder = Get-ReportFolder -Root $OutputRoot -Inc $IncidentNumber
if ($folder) {
    Write-Host ""
    Write-Host "Final operator-facing files:"
    Write-Host (Join-Path $folder "worknotes_summary.txt")
    Write-Host (Join-Path $folder "incident_lifecycle.csv")
    Write-Host (Join-Path $folder "closing_summary.txt")
    Write-Host "Script memory retained, do not edit:"
    Write-Host (Join-Path $folder "state.json")
    if ($ShowGrid) { Open-Grid $folder }
} else {
    Write-Warning "Unable to locate incident output folder."
}

exit $mainExitCode
