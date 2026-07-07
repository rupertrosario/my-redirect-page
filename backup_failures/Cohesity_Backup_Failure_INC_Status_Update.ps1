<#
.SYNOPSIS
Entry point for Cohesity Backup Failure INC status updates.

.DESCRIPTION
This script name is the operational entry point for the backup failure incident update workflow.
It delegates to the main implementation script in the same folder so the logic remains in one place.

Run one cluster:
  .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "CLUSTER_NAME"

Run all clusters:
  .\Cohesity_Backup_Failure_INC_Status_Update.ps1
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$LegacyFailureOutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailures",
    [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
    [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
    [string]$ClusterName = "",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = "",
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = "",
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35
)

$target = Join-Path $PSScriptRoot "Get-CohesityBackupFailureWindowConsolidator.ps1"
if (!(Test-Path $target)) {
    throw "Main implementation script not found: $target"
}

& $target @PSBoundParameters
exit $LASTEXITCODE
