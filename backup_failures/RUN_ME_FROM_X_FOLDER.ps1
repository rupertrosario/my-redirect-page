<#
RUN ME FROM:
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\RUN_ME.ps1

Expected local files:
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\RUN_ME.ps1
X:\PowerShell\Cohesity_API_Scripts\BackupFailureWindow\Get-CohesityBackupFailureWindowConsolidator.ps1
X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt

Default run is SAFE TEST + CSV:
- MaxClusters 1
- MaxProtectionGroupsPerCluster 3
- ShowGridView false
- ForceCsv

Full run:
.\RUN_ME.ps1 -Full
#>

[CmdletBinding()]
param(
    [switch]$Full,
    [string]$IncidentNumber
)

$ErrorActionPreference = 'Stop'

$BaseFolder = $PSScriptRoot
$MainScript = Join-Path $BaseFolder 'Get-CohesityBackupFailureWindowConsolidator.ps1'
$ApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt'
$OutputRoot = Join-Path $BaseFolder 'Output'
$RegistryFile = Join-Path $OutputRoot 'BackupFailure_WindowRegistry.json'

Write-Host ''
Write-Host 'BACKUP FAILURE WINDOW - LOCAL RUN CONTEXT' -ForegroundColor Cyan
Write-Host "Running from        : $BaseFolder"
Write-Host "Main script         : $MainScript"
Write-Host "API key path        : $ApiKeyPath"
Write-Host "Output root         : $OutputRoot"
Write-Host "Registry file       : $RegistryFile"
Write-Host "Cohesity API mode   : GET only"
Write-Host "Evidence format     : CSV"
Write-Host ''

if (-not (Test-Path $MainScript)) {
    Write-Host 'MAIN SCRIPT NOT FOUND' -ForegroundColor Red
    Write-Host "Copy Get-CohesityBackupFailureWindowConsolidator.ps1 into: $BaseFolder"
    throw 'Main script missing.'
}

if (-not (Test-Path $ApiKeyPath)) {
    Write-Host 'API KEY FILE NOT FOUND' -ForegroundColor Red
    Write-Host "Expected: $ApiKeyPath"
    throw 'API key file missing.'
}

if (-not (Test-Path $OutputRoot)) {
    New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
}

if ($Full) {
    Write-Host 'RUN MODE: FULL CSV RUN' -ForegroundColor Yellow
    if ($IncidentNumber) {
        & $MainScript -IncidentNumber $IncidentNumber -OutputRoot $OutputRoot -ApiKeyPath $ApiKeyPath -ShowGridView:$false -ForceCsv
    } else {
        & $MainScript -OutputRoot $OutputRoot -ApiKeyPath $ApiKeyPath -ShowGridView:$false -ForceCsv
    }
} else {
    Write-Host 'RUN MODE: SAFE LIMITED CSV TEST' -ForegroundColor Yellow
    Write-Host 'Scope   : MaxClusters 1, MaxProtectionGroupsPerCluster 3'
    if ($IncidentNumber) {
        & $MainScript -IncidentNumber $IncidentNumber -OutputRoot $OutputRoot -ApiKeyPath $ApiKeyPath -MaxClusters 1 -MaxProtectionGroupsPerCluster 3 -ShowGridView:$false -ForceCsv
    } else {
        & $MainScript -OutputRoot $OutputRoot -ApiKeyPath $ApiKeyPath -MaxClusters 1 -MaxProtectionGroupsPerCluster 3 -ShowGridView:$false -ForceCsv
    }
}
