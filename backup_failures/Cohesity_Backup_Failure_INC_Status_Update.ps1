<#
.SYNOPSIS
Entry point for Cohesity Backup Failure INC status updates.

.DESCRIPTION
This is the operational entry point for the backup failure incident update workflow.
It delegates to the main implementation script, then rewrites the human-readable text outputs
so there is one detailed source of truth with no truncated row sections.

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

function Format-AllRows([string]$Title, $Rows) {
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
        $cleared = Clean-Text $r.ClearedET
        $latest = Clean-Text $r.LatestRunStatus
        $count = Clean-Text $r.ConsecutiveFailureCount
        $msg = Clean-Text $r.Message

        if (!$hostName) { $hostName = "-" }
        if (!$cleared) { $cleared = "-" }
        if (!$latest) { $latest = "-" }
        if (!$count) { $count = "0" }

        $lines.Add(("{0}. Cluster: {1} | Env: {2} | PG: {3} | Host: {4} | Object: {5} | RunType: {6} | Status: {7} | FirstFailedET: {8} | LastFailedET: {9} | ClearedET: {10} | LatestRunStatus: {11} | FailureCount: {12} | Message: {13}" -f $i, $cluster, $env, $pg, $hostName, $obj, $runType, $status, $first, $last, $cleared, $latest, $count, $msg))
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

function Format-StatusTotals($Rows) {
    $lines = New-Object System.Collections.Generic.List[string]
    $groups = @($Rows | Group-Object Status | Sort-Object Name)
    if ($groups.Count -eq 0) { return "- None" }
    foreach ($g in $groups) { $lines.Add("- $($g.Name): $($g.Count)") }
    return ($lines -join [Environment]::NewLine)
}

function Write-ConsistentIncidentText([string]$Folder, [int]$RunLimit) {
    if ([string]::IsNullOrWhiteSpace($Folder) -or !(Test-Path $Folder)) { return }

    $state = Read-JsonFile (Join-Path $Folder "state.json")
    $current = Import-ReportCsv (Join-Path $Folder "current_failures.csv")
    $cleared = Import-ReportCsv (Join-Path $Folder "cleared_by_success.csv")
    $lifecycle = Import-ReportCsv (Join-Path $Folder "incident_lifecycle.csv")

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
    $consecutive = @($current | Where-Object { [int]($_.ConsecutiveFailureCount) -gt 1 })

    $completeness = "Complete based on successful API/CSV processing for this run."
    if ($warnings.Count -gt 0) {
        $completeness = "Incomplete. One or more cluster/protection-group queries failed or timed out. Do not treat missing rows from warned PGs/clusters as cleared unless listed in cleared_by_success.csv."
    }

    $worknotes = @"
Backup Failure Window Update

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Evidence Folder: $Folder

Summary:
- Current active/unresolved failures: $($current.Count)
- New failures since previous check: $($newFailures.Count)
- Older/current still failing: $($older.Count)
- Carried forward still failing: $($carried.Count)
- Re-failed after earlier clear: $($refailed.Count)
- Cleared by later successful backup: $($cleared.Count)
- Running / awaiting completion: $($running.Count)
- Cancelled after failure: $($cancelled.Count)
- Unknown / needs review: $($unknown.Count)
- Consecutive/repeated active failures: $($consecutive.Count)
- Warnings / incomplete collection: $($warnings.Count)
- Scope: latest $RunLimit runs per protection group/run type.

Report completeness:
$completeness

Warnings / Incomplete Collection:
$(Format-Warnings $warnings)

Detailed incident update:
See summary.txt

Closure / handoff summary:
See closing_summary.txt

CSV evidence:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
"@
    $worknotes | Set-Content -Path (Join-Path $Folder "worknotes.txt") -Encoding UTF8

    $summary = @"
Backup Failure Incident Summary

Incident: $incident
Compute Window: $windowLabel
Last Updated: $generated ET
Evidence Folder: $Folder

Report completeness:
$completeness

Current State:
- Current active/unresolved failures: $($current.Count)
- New failures since previous check: $($newFailures.Count)
- Older/current still failing: $($older.Count)
- Carried forward still failing: $($carried.Count)
- Re-failed after earlier clear: $($refailed.Count)
- Cleared by later successful backup: $($cleared.Count)
- Running / awaiting completion: $($running.Count)
- Cancelled after failure: $($cancelled.Count)
- Unknown / needs review: $($unknown.Count)
- Consecutive/repeated active failures: $($consecutive.Count)
- Total lifecycle rows tracked: $($lifecycle.Count)

Lifecycle Status Totals:
$(Format-StatusTotals $lifecycle)

Carry Forward / Handoff:
$(if ($current.Count -eq 0) { "No active backup failures remain based on the latest check." } else { "$($current.Count) active/unresolved objects remain and require follow-up tracking. See current_failures.csv." })

$(Format-AllRows "New Failures Since Previous Check:" $newFailures)
$(Format-AllRows "Older / Current Still Failing:" $older)
$(Format-AllRows "Carried Forward Still Failing:" $carried)
$(Format-AllRows "Re-Failed After Earlier Clear:" $refailed)
$(Format-AllRows "Consecutive / Repeated Active Failures:" $consecutive)
$(Format-AllRows "Running / Awaiting Completion:" $running)
$(Format-AllRows "Cancelled / Not Cleared:" $cancelled)
$(Format-AllRows "Unknown / Needs Review:" $unknown)
$(Format-AllRows "Failures Cleared By Later Successful Backup:" $cleared)
$(Format-AllRows "All Tracked Lifecycle Rows:" $lifecycle)

Warnings / Incomplete Collection:
$(Format-Warnings $warnings)

Retention Actions:
$(Format-Warnings $retention)

Scope / Limitations:
- Cohesity run evaluation is limited to the latest $RunLimit runs per protection group/run type.
- Only Succeeded or SucceededWithWarning clears a previous failure.
- Running does not clear a previous failure.
- Cancelled/Canceled does not clear a previous failure.
- Missing from the current scan is not treated as cleared unless a later successful backup is verified.
- This is incident lifecycle tracking for observed/latest uncleared failures, not an audit-grade history of every failure event.

Evidence Files:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
- worknotes.txt
- summary.txt
- closing_summary.txt
- state.json
"@
    $summary | Set-Content -Path (Join-Path $Folder "summary.txt") -Encoding UTF8

    $closing = @"
Backup Failure Incident Closure Summary

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Evidence Folder: $Folder

Report completeness:
$completeness

Closure State:
- Active/unresolved failures: $($current.Count)
- Cleared by later successful backup: $($cleared.Count)
- Running / awaiting completion: $($running.Count)
- Cancelled after failure: $($cancelled.Count)
- Unknown / needs review: $($unknown.Count)
- Consecutive/repeated active failures: $($consecutive.Count)
- Total lifecycle rows tracked: $($lifecycle.Count)

Carry Forward / Handoff:
$(if ($current.Count -eq 0) { "No active backup failures remain based on the latest saved state." } else { "$($current.Count) active/unresolved objects remain and should be carried forward or separately tracked." })

$(Format-AllRows "Active / Unresolved Failures:" $current)
$(Format-AllRows "Failures Cleared By Later Successful Backup:" $cleared)
$(Format-AllRows "Running / Awaiting Completion:" $running)
$(Format-AllRows "Cancelled / Not Cleared:" $cancelled)
$(Format-AllRows "Unknown / Needs Review:" $unknown)
$(Format-AllRows "All Tracked Lifecycle Rows:" $lifecycle)

Warnings / Incomplete Collection:
$(Format-Warnings $warnings)

Scope / Limitations:
- Evaluation is limited to the latest $RunLimit runs per protection group/run type.
- Only a later Succeeded or SucceededWithWarning backup clears a failure.
- Running and cancelled runs remain unresolved.
- This closure summary is generated from the incident state/files; it does not perform a separate Cohesity scan.

Evidence Files:
- current_failures.csv
- cleared_by_success.csv
- incident_lifecycle.csv
- worknotes.txt
- summary.txt
- closing_summary.txt
- state.json
"@
    $closing | Set-Content -Path (Join-Path $Folder "closing_summary.txt") -Encoding UTF8
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
        Write-ConsistentIncidentText -Folder $folder -RunLimit $NumRuns
        Write-Host ""
        Write-Host "Text outputs normalized with no row truncation:"
        Write-Host (Join-Path $folder "worknotes.txt")
        Write-Host (Join-Path $folder "summary.txt")
        Write-Host (Join-Path $folder "closing_summary.txt")
    } else {
        Write-Warning "Unable to locate incident output folder for text normalization."
    }
} catch {
    Write-Warning "Text output normalization failed: $($_.Exception.Message)"
}

exit $mainExitCode
