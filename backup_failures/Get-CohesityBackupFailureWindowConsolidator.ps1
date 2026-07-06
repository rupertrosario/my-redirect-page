<#
.SYNOPSIS
Root launcher for the Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
Run this from backup_failures. The implementation is kept in window_consolidator so Claude can modify it safely.
CSV/TXT/JSON only. No Excel. Cohesity API calls remain GET-only.
#>

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $PSScriptRoot "window_consolidator\Get-CohesityBackupFailureWindowConsolidator.ps1"

if (-not (Test-Path -Path $scriptPath -PathType Leaf)) {
    throw "Window consolidator implementation not found: $scriptPath"
}

& $scriptPath @args
