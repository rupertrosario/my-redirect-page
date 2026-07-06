<#
Short runner for Cohesity Backup Failure Window Consolidator.
Use this to avoid typing the long script name.
#>

$scriptPath = Join-Path $PSScriptRoot 'Get-CohesityBackupFailureWindowConsolidator.ps1'
if (-not (Test-Path $scriptPath)) {
    throw "Main script not found: $scriptPath"
}

& $scriptPath @args
