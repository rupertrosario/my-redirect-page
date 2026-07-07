<#
.SYNOPSIS
Entry point for Cohesity Backup Failure INC status updates.

.DESCRIPTION
This is the operational entry point for the backup failure incident update workflow.
It delegates to the main implementation script, then rewrites the human-readable text outputs
so there is one concise worknotes_summary.txt file focused on operator action and no row truncation.

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

function Clean-Text($Value) {
    if ($null -eq $Value) { return "" }
    if ($Value -is [array]) { $Value = $Value -join " | " }
    return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Trim()
}

function Read-JsonFile([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    try { return ($raw | ConvertFrom-Json) } catch { return $null }
}

function Import-ReportCsv([string]$Path) {
    if (!(Test-Path $Path)) { return @() }
    try { return @(Import-Csv -Path $Path) } catch { return @() }
}

function Save-SortedCsv([string]$Path, $Rows) {
    $list = @($Rows)
    if ($list.Count -eq 0) { return }
    $list | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
}

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

function Get-DateSortText($Value) {
    $t = Clean-Text $Value
    if (!$t) { return "0000-00-00 00:00:00" }
    return $t
}

function Get-ActiveSortText($Row) {
    $lastFailed = Get-DateSortText $Row.LastFailedET
    if ($lastFailed -ne "0000-00-00 00:00:00") { return $lastFailed }
    return (Get-DateSortText $Row.LastSeenET)
}

function Get-ClearedSortText($Row) {
    $cleared = Get-DateSortText $Row.ClearedET
    if ($cleared -ne "0000-00-00 00:00:00") { return $cleared }
    return (Get-DateSortText $Row.LastFailedET)
}

function Get-LifecycleSortText($Row) {
    $cleared = Get-DateSortText $Row.ClearedET
    if ($cleared -ne "0000-00-00 00:00:00") { return $cleared }
    $lastSeen = Get-DateSortText $Row.LastSeenET
    if ($lastSeen -ne "0000-00-00 00:00:00") { return $lastSeen }
    return (Get-DateSortText $Row.LastFailedET)
}

function Sort-ActiveRows($Rows) {
    @($Rows) | Sort-Object `
        @{ Expression = { Clean-Text $_.Cluster }; Ascending = $true }, `
        @{ Expression = { Get-ActiveSortText $_ }; Descending = $true }, `
        @{ Expression = { Clean-Text $_.Environment }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ProtectionGroup }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ObjectName }; Ascending = $true }
}

function Sort-ClearedRows($Rows) {
    @($Rows) | Sort-Object `
        @{ Expression = { Clean-Text $_.Cluster }; Ascending = $true }, `
        @{ Expression = { Get-ClearedSortText $_ }; Descending = $true }, `
        @{ Expression = { Clean-Text $_.Environment }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ProtectionGroup }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ObjectName }; Ascending = $true }
}

function Sort-LifecycleRows($Rows) {
    @($Rows) | Sort-Object `
        @{ Expression = { Clean-Text $_.Cluster }; Ascending = $true }, `
        @{ Expression = { Get-LifecycleSortText $_ }; Descending = $true }, `
        @{ Expression = { Clean-Text $_.Status }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.Environment }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ProtectionGroup }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ObjectName }; Ascending = $true }
}

function Format-ActiveRows([string]$Title, $Rows) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("")
    $lines.Add($Title)
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add("- None")
        return ($lines -join [Environment]::NewLine)
    }

    $i = 0
    foreach ($r in $list) {
        $i++
        $cluster = Clean-Text $r.Cluster
        $env = Clean-Text $r.Environment
        $pg = Clean-Text $r.ProtectionGroup
        $hostName = Clean-Text $r.Host
        $obj = Clean-Text $r.ObjectName
        $runType = Clean-Text $r.RunType
        $status = Clean-Text $r.Status
        $first = Clean-Text $r.FirstFailedET
        $last = Clean-Text $r.LastFailedET
        $latest = Clean-Text $r.LatestRunStatus
        $count = Clean-Text $r.ConsecutiveFailureCount
        $msg = Clean-Text $r.Message

        if (!$hostName) { $hostName = "-" }
        if (!$latest) { $latest = "-" }
        if (!$count) { $count = "0" }

        $lines.Add(("{0}. Cluster: {1} | Env: {2} | PG: {3} | Host: {4} | Object: {5} | RunType: {6} | Status: {7} | FirstFailedET: {8} | LastFailedET: {9} | FailureRuns: {10} | LatestRunStatus: {11} | Message: {12}" -f $i, $cluster, $env, $pg, $hostName, $obj, $runType, $status, $first, $last, $count, $latest, $msg))
    }
    return ($lines -join [Environment]::NewLine)
}

function Format-ClearedRows([string]$Title, $Rows) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("")
    $lines.Add($Title)
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add("- None")
        return ($lines -join [Environment]::NewLine)
    }

    $i = 0
    foreach ($r in $list) {
        $i++
        $cluster = Clean-Text $r.Cluster
        $env = Clean-Text $r.Environment
        $pg = Clean-Text $r.ProtectionGroup
        $hostName = Clean-Text $r.Host
        $obj = Clean-Text $r.ObjectName
        $runType = Clean-Text $r.RunType
        $status = Clean-Text $r.Status
        $last = Clean-Text $r.LastFailedET
        $cleared = Clean-Text $r.ClearedET

        if (!$hostName) { $hostName = "-" }
        if (!$cleared) { $cleared = "-" }

        $lines.Add(("{0}. Cluster: {1} | Env: {2} | PG: {3} | Host: {4} | Object: {5} | RunType: {6} | LastFailedET: {7} | ClearedET: {8} | Status: {9}" -f $i, $cluster, $env, $pg, $hostName, $obj, $runType, $last, $cleared, $status))
    }
    return ($lines -join [Environment]::NewLine)
}

function Format-Warnings($Warnings) {
    $lines = New-Object System.Collections.Generic.List[string]
    $list = @($Warnings | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    if ($list.Count -eq 0) {
        $lines.Add("- None")
    } else {
        foreach ($w in $list) { $lines.Add("- $(Clean-Text $w)") }
    }
    return ($lines -join [Environment]::NewLine)
}

function Format-LocalCleanupIssues($RetentionActions) {
    $issues = @($RetentionActions | Where-Object {
        $t = Clean-Text $_
        $t -match "(?i)(fail|failed|error|unable|exception|denied)"
    })
    if ($issues.Count -eq 0) { return "" }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("")
    $lines.Add("Local Output Cleanup Issues:")
    foreach ($i in $issues) { $lines.Add("- $(Clean-Text $i)") }
    return ($lines -join [Environment]::NewLine)
}

function Build-RerunCommand([string]$Incident, [string]$Cluster, [int]$RunLimit) {
    $line = '.\Cohesity_Backup_Failure_INC_Status_Update.ps1'
    if ($Incident) { $line += ' -IncidentNumber "' + $Incident + '"' }
    if ($Cluster) { $line += ' -ClusterName "' + $Cluster + '"' }
    if ($RunLimit -ne 30) { $line += ' -NumRuns ' + $RunLimit }

    @"
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
$line
"@
}

function Write-SingleWorknotesSummary([string]$Folder, [int]$RunLimit) {
    if ([string]::IsNullOrWhiteSpace($Folder) -or !(Test-Path $Folder)) { return }

    $state = Read-JsonFile (Join-Path $Folder "state.json")
    $currentPath = Join-Path $Folder "current_failures.csv"
    $clearedPath = Join-Path $Folder "cleared_by_success.csv"
    $lifecyclePath = Join-Path $Folder "incident_lifecycle.csv"

    $current = Sort-ActiveRows (Import-ReportCsv $currentPath)
    $cleared = Sort-ClearedRows (Import-ReportCsv $clearedPath)
    $lifecycle = Sort-LifecycleRows (Import-ReportCsv $lifecyclePath)

    Save-SortedCsv -Path $currentPath -Rows $current
    Save-SortedCsv -Path $clearedPath -Rows $cleared
    Save-SortedCsv -Path $lifecyclePath -Rows $lifecycle

    $incident = if ($state -and $state.IncidentNumber) { Clean-Text $state.IncidentNumber } else { Split-Path $Folder -Leaf }
    $windowLabel = if ($state -and $state.WindowLabel) { Clean-Text $state.WindowLabel } else { "" }
    $generated = if ($state -and $state.LastRunET) { Clean-Text $state.LastRunET } else { (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }
    $warnings = if ($state -and $state.Warnings) { @($state.Warnings) } else { @() }
    $retention = if ($state -and $state.RetentionActions) { @($state.RetentionActions) } else { @() }

    $newFailures = @($current | Where-Object { $_.Status -eq "NewlyFailedThisCheck" })
    $carried = @($current | Where-Object { $_.Status -eq "CarriedForwardStillFailing" })
    $older = @($current | Where-Object { $_.Status -eq "OlderStillFailing" -or $_.Status -eq "CurrentStillFailing" })
    $refailed = @($current | Where-Object { $_.Status -eq "ReFailedAfterClear" })
    $running = @($current | Where-Object { $_.Status -eq "RunningAtLatestCheck" })
    $cancelled = @($current | Where-Object { $_.Status -eq "CancelledAfterFailure" })
    $unknown = @($current | Where-Object { $_.Status -eq "UnknownNeedsReview" })

    $apiStatus = "Complete - all collected scopes returned without recorded lookup warnings."
    if ($warnings.Count -gt 0) {
        $apiStatus = "Incomplete - $($warnings.Count) collection warning(s) recorded. See Incomplete Collection section."
    }

    $rerunCommand = Build-RerunCommand -Incident $incident -Cluster $ClusterName -RunLimit $RunLimit
    $followUp = if ($warnings.Count -gt 0) {
@"
Retry Failed Collection Scope:
Run the command below to refresh the incident output after the timed-out Cohesity API scope is available.

$rerunCommand

After the rerun completes, use the refreshed worknotes_summary.txt and incident_lifecycle.csv for the incident update.
"@
    } else {
"Retry Failed Collection Scope:`n- Not required for this run."
    }

    $localCleanupIssues = Format-LocalCleanupIssues $retention

    $worknotesSummary = @"
Cohesity Backup Failure Incident Update

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Evidence Folder: $Folder
Cohesity API Collection Status: $apiStatus
Scope: latest $RunLimit runs per protection group/run type.
Sort Order: Cluster ascending; active failures by LastFailedET descending; cleared rows by ClearedET descending; lifecycle rows by latest activity descending.

Operator Guidance:
- If Cohesity API Collection Status is Complete, review and action the Active / Unresolved Failures section.
- If Cohesity API Collection Status is Incomplete, complete the Retry Failed Collection Scope step and replace the incident update with the refreshed output.
- For the full lifecycle view, refer to incident_lifecycle.csv.

Counts:
- Active / unresolved failures: $($current.Count)
- Cleared by later successful backup: $($cleared.Count)
- New failures since previous check: $($newFailures.Count)
- Older/current still failing: $($older.Count)
- Carried forward still failing: $($carried.Count)
- Re-failed after earlier clear: $($refailed.Count)
- Running / awaiting completion: $($running.Count)
- Cancelled after failure: $($cancelled.Count)
- Unknown / needs review: $($unknown.Count)
- Total lifecycle rows tracked: $($lifecycle.Count)
- Incomplete collection warnings: $($warnings.Count)

$(Format-ActiveRows "Active / Unresolved Failures:" $current)
$(Format-ClearedRows "Cleared By Later Successful Backup:" $cleared)

Incomplete Collection:
$(Format-Warnings $warnings)

$followUp

Detailed Lifecycle Reference:
- incident_lifecycle.csv contains the complete sortable lifecycle view, including all tracked statuses and historical state transitions.
- Use worknotes_summary.txt for the incident update.
- Use incident_lifecycle.csv for detailed review/filtering.

Files to Attach / Update:
- worknotes_summary.txt
- incident_lifecycle.csv
- current_failures.csv, if active failures exist
- cleared_by_success.csv, if cleared rows exist
- closing_summary.txt, for closure or handoff
$localCleanupIssues
"@

    $worknotesSummary | Set-Content -Path (Join-Path $Folder "worknotes_summary.txt") -Encoding UTF8

    $closing = @"
Backup Failure Incident Closure Summary

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Evidence Folder: $Folder
Cohesity API Collection Status: $apiStatus
Scope: latest $RunLimit runs per protection group/run type.
Sort Order: Cluster ascending; active failures by LastFailedET descending; cleared rows by ClearedET descending; lifecycle rows by latest activity descending.

Closure State:
- Active / unresolved failures: $($current.Count)
- Cleared by later successful backup: $($cleared.Count)
- Running / awaiting completion: $($running.Count)
- Cancelled after failure: $($cancelled.Count)
- Unknown / needs review: $($unknown.Count)
- Total lifecycle rows tracked: $($lifecycle.Count)
- Incomplete collection warnings: $($warnings.Count)

Carry Forward / Handoff:
$(if ($current.Count -eq 0) { "No active backup failures remain based on the latest saved state." } else { "$($current.Count) active/unresolved objects remain and should be carried forward or separately tracked." })

$(Format-ActiveRows "Active / Unresolved Failures:" $current)
$(Format-ClearedRows "Cleared By Later Successful Backup:" $cleared)

Incomplete Collection:
$(Format-Warnings $warnings)

Detailed Lifecycle Reference:
- incident_lifecycle.csv contains the complete sortable lifecycle view.

Evidence Files:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
- worknotes_summary.txt
- closing_summary.txt
- state.json
$localCleanupIssues
"@
    $closing | Set-Content -Path (Join-Path $Folder "closing_summary.txt") -Encoding UTF8

    foreach ($obsolete in @("worknotes.txt", "summary.txt")) {
        $obsoletePath = Join-Path $Folder $obsolete
        if (Test-Path $obsoletePath) {
            Remove-Item -Path $obsoletePath -Force -ErrorAction SilentlyContinue
        }
    }
}

$target = Join-Path $PSScriptRoot "Get-CohesityBackupFailureWindowConsolidator.ps1"
if (!(Test-Path $target)) {
    throw "Main implementation script not found: $target"
}

& $target @PSBoundParameters
$mainExitCode = $LASTEXITCODE

try {
    $folder = Get-ReportFolder -Root $OutputRoot -Inc $IncidentNumber
    if ($folder) {
        Write-SingleWorknotesSummary -Folder $folder -RunLimit $NumRuns
        Write-Host ""
        Write-Host "Text outputs normalized with simplified worknotes summary, clusterwise sorting, and no row truncation:"
        Write-Host (Join-Path $folder "worknotes_summary.txt")
        Write-Host (Join-Path $folder "closing_summary.txt")
    } else {
        Write-Warning "Unable to locate incident output folder for text normalization."
    }
} catch {
    Write-Warning "Text output normalization failed: $($_.Exception.Message)"
}

exit $mainExitCode
