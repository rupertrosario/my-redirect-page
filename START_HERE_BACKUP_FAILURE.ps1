<#
START HERE - Backup Failure Incident Evidence

Run from the repository root:
  .\START_HERE_BACKUP_FAILURE.ps1

Default mode is a safe limited CSV test:
  - MaxClusters 1
  - MaxProtectionGroupsPerCluster 3
  - ShowGridView false
  - ForceCsv

For full run:
  .\START_HERE_BACKUP_FAILURE.ps1 -Full
#>

[CmdletBinding()]
param(
    [switch]$Full,
    [string]$IncidentNumber
)

$ErrorActionPreference = 'Stop'

$mainScript = Join-Path $PSScriptRoot 'backup_failures\Get-CohesityBackupFailureWindowConsolidator.ps1'

if (-not (Test-Path $mainScript)) {
    Write-Host ''
    Write-Host 'SCRIPT NOT FOUND' -ForegroundColor Red
    Write-Host "Expected file: $mainScript"
    Write-Host ''
    Write-Host 'Fix:' -ForegroundColor Yellow
    Write-Host '1. Confirm you are on branch Cohesity_Automations.'
    Write-Host '2. Run: git pull'
    Write-Host '3. Confirm folder exists: dir .\backup_failures'
    throw 'Main backup failure script is missing.'
}

$apiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt'
if (-not (Test-Path $apiKeyPath)) {
    Write-Host ''
    Write-Host 'API KEY FILE NOT FOUND' -ForegroundColor Red
    Write-Host "Expected file: $apiKeyPath"
    throw 'Cohesity API key file is missing.'
}

if ($Full) {
    Write-Host 'Running FULL backup failure incident evidence collection in CSV mode...' -ForegroundColor Cyan
    if ($IncidentNumber) {
        & $mainScript -IncidentNumber $IncidentNumber -ShowGridView:$false -ForceCsv
    } else {
        & $mainScript -ShowGridView:$false -ForceCsv
    }
} else {
    Write-Host 'Running SAFE TEST backup failure incident evidence collection in CSV mode...' -ForegroundColor Cyan
    Write-Host 'Scope: MaxClusters 1, MaxProtectionGroupsPerCluster 3' -ForegroundColor Cyan
    if ($IncidentNumber) {
        & $mainScript -IncidentNumber $IncidentNumber -MaxClusters 1 -MaxProtectionGroupsPerCluster 3 -ShowGridView:$false -ForceCsv
    } else {
        & $mainScript -MaxClusters 1 -MaxProtectionGroupsPerCluster 3 -ShowGridView:$false -ForceCsv
    }
}
