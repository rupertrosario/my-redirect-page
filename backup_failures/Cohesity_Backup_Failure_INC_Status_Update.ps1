<#
.SYNOPSIS
Entry point for Cohesity Backup Failure INC status updates.

.DESCRIPTION
This is the operational entry point for the backup failure incident update workflow.
It delegates to the main implementation script, then rewrites the human-readable text outputs
so there is one concise worknotes_summary.txt file focused on operator action and clear count tallying.

Run one cluster:
  .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ClusterName "CLUSTER_NAME"

Run all clusters:
  .\Cohesity_Backup_Failure_INC_Status_Update.ps1

Optional grid view after output generation:
  .\Cohesity_Backup_Failure_INC_Status_Update.ps1 -ShowGrid
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
    [int]$ArchiveFoldersUntilDays = 35,
    [switch]$ShowGrid
)

$script:CsvColumns = @(
    "IncidentNumber",
    "WindowKey",
    "Status",
    "Cluster",
    "Environment",
    "ProtectionGroup",
    "Host",
    "ObjectName",
    "ObjectType",
    "RunType",
    "FirstFailedET",
    "LastFailedET",
    "ClearedET",
    "LastSeenET",
    "LatestRunStatus",
    "ConsecutiveFailureCount",
    "Message",
    "ObjectKey"
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

function Write-JsonFile($Object, [string]$Path) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Import-ReportCsv([string]$Path) {
    if (!(Test-Path $Path)) { return @() }
    try { return @(Import-Csv -Path $Path) } catch { return @() }
}

function Save-SortedCsv([string]$Path, $Rows) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }

    $list = @($Rows)
    if ($list.Count -eq 0) {
        ($script:CsvColumns -join ",") | Set-Content -Path $Path -Encoding UTF8
        return
    }

    $list | Select-Object $script:CsvColumns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
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

function Set-RowProp($Object, [string]$Name, $Value) {
    if ($null -eq $Object) { return }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { $Object.$Name = $Value }
    else { $Object | Add-Member -MemberType NoteProperty -Name $Name -Value $Value -Force }
}

function Clone-Row($Row) {
    if ($null -eq $Row) { return $null }
    $Row | Select-Object *
}

function Parse-ReportDate([string]$Text) {
    $t = Clean-Text $Text
    if (!$t) { return $null }

    $formats = @(
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd H:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd H:mm",
        "M/d/yyyy H:mm:ss",
        "M/d/yyyy HH:mm:ss",
        "M/d/yyyy H:mm",
        "M/d/yyyy HH:mm",
        "M/d/yyyy h:mm:ss tt",
        "M/d/yyyy hh:mm:ss tt",
        "M/d/yyyy h:mm tt",
        "M/d/yyyy hh:mm tt"
    )

    foreach ($fmt in $formats) {
        try { return [datetime]::ParseExact($t, $fmt, [Globalization.CultureInfo]::InvariantCulture) } catch {}
    }

    try { return [datetime]::Parse($t, [Globalization.CultureInfo]::InvariantCulture) } catch { return $null }
}

function Get-DateSortText($Value) {
    $t = Clean-Text $Value
    if (!$t) { return "0000-00-00 00:00:00" }
    $dt = Parse-ReportDate $t
    if ($dt) { return $dt.ToString("yyyy-MM-dd HH:mm:ss") }
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

function Is-SuccessStatusText([string]$Status) {
    (Clean-Text $Status) -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning")
}

function Is-ClearedStatusText([string]$Status) {
    (Clean-Text $Status) -in @("ClearedByLaterSuccess", "NewlyClearedThisCheck")
}

function Test-LatestSuccessClearsActiveRow($Row) {
    if ($null -eq $Row) { return $false }
    if (Is-ClearedStatusText $Row.Status) { return $false }
    if (!(Is-SuccessStatusText $Row.LatestRunStatus)) { return $false }

    $lastSeen = Parse-ReportDate (Clean-Text $Row.LastSeenET)
    $lastFailed = Parse-ReportDate (Clean-Text $Row.LastFailedET)
    if ($null -eq $lastSeen -or $null -eq $lastFailed) { return $false }

    return ($lastSeen -gt $lastFailed)
}

function Get-RowKey($Row) {
    $key = Clean-Text $Row.ObjectKey
    if ($key) { return $key }
    $parts = @(
        (Clean-Text $Row.Cluster),
        (Clean-Text $Row.Environment),
        (Clean-Text $Row.ProtectionGroup),
        (Clean-Text $Row.Host),
        (Clean-Text $Row.ObjectName),
        (Clean-Text $Row.RunType)
    )
    return ($parts -join "|")
}

function Merge-RowsByKeyPreferLatestActivity($Rows) {
    $map = @{}
    foreach ($r in @($Rows)) {
        if ($null -eq $r) { continue }
        $key = Get-RowKey $r
        if (!$key) { continue }

        if (!$map.ContainsKey($key)) {
            $map[$key] = $r
            continue
        }

        $existingSort = Get-LifecycleSortText $map[$key]
        $newSort = Get-LifecycleSortText $r
        if ($newSort -ge $existingSort) { $map[$key] = $r }
    }
    @($map.Values)
}

function Reconcile-LatestSuccessfulBackupRows($Current, $Cleared) {
    $remainingCurrent = @()
    $movedToCleared = @()

    foreach ($r in @($Current)) {
        if (Test-LatestSuccessClearsActiveRow $r) {
            $c = Clone-Row $r
            Set-RowProp $c "Status" "NewlyClearedThisCheck"
            Set-RowProp $c "ClearedET" (Clean-Text $c.LastSeenET)
            $movedToCleared += $c
        } else {
            $remainingCurrent += $r
        }
    }

    $finalCleared = Merge-RowsByKeyPreferLatestActivity @($Cleared + $movedToCleared)
    $finalLifecycle = Merge-RowsByKeyPreferLatestActivity @($remainingCurrent + $finalCleared)

    [pscustomobject]@{
        Current = @($remainingCurrent)
        Cleared = @($finalCleared)
        Lifecycle = @($finalLifecycle)
        MovedToCleared = @($movedToCleared)
    }
}

function Get-StatusPriority($Status) {
    switch (Clean-Text $Status) {
        "NewlyFailedThisCheck"       { return 10 }
        "ReFailedAfterClear"         { return 20 }
        "OlderStillFailing"          { return 30 }
        "CurrentStillFailing"        { return 30 }
        "CarriedForwardStillFailing" { return 40 }
        "RunningAtLatestCheck"       { return 50 }
        "CancelledAfterFailure"      { return 60 }
        "UnknownNeedsReview"         { return 70 }
        "NewlyClearedThisCheck"      { return 80 }
        "ClearedByLaterSuccess"      { return 90 }
        default                       { return 99 }
    }
}

function Sort-ActiveRows($Rows) {
    @($Rows) | Sort-Object `
        @{ Expression = { Clean-Text $_.Cluster }; Ascending = $true }, `
        @{ Expression = { Get-StatusPriority $_.Status }; Ascending = $true }, `
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
        @{ Expression = { Get-StatusPriority $_.Status }; Ascending = $true }, `
        @{ Expression = { Get-LifecycleSortText $_ }; Descending = $true }, `
        @{ Expression = { Clean-Text $_.Environment }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ProtectionGroup }; Ascending = $true }, `
        @{ Expression = { Clean-Text $_.ObjectName }; Ascending = $true }
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

function Open-OutputGrid([string]$Folder) {
    if (!(Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
        Write-Warning "Out-GridView is not available in this PowerShell session. CSV files were still generated."
        return
    }

    $lifecyclePath = Join-Path $Folder "incident_lifecycle.csv"
    if (!(Test-Path $lifecyclePath)) {
        Write-Warning "incident_lifecycle.csv was not found. Grid view was not opened."
        return
    }

    $rows = Import-ReportCsv $lifecyclePath
    if ($rows.Count -eq 0) {
        Write-Warning "incident_lifecycle.csv has no rows. Grid view was not opened."
        return
    }

    $rows | Out-GridView -Title "Cohesity - Incident Lifecycle"
}

function Update-StateWithReconciledRows($State, [string]$StatePath, $Current, $Cleared, $Lifecycle, $MovedToCleared) {
    if ($null -eq $State -or [string]::IsNullOrWhiteSpace($StatePath)) { return }

    Set-RowProp $State "CurrentOpenFailures" @($Current)
    Set-RowProp $State "ClearedBySuccess" @($Cleared)
    Set-RowProp $State "LifecycleRows" @($Lifecycle)
    Set-RowProp $State "LatestSuccessReconciledClearCount" @($MovedToCleared).Count

    $lastRunCleared = @()
    if ($State.PSObject.Properties["LastRunClearedBySuccess"]) { $lastRunCleared += @($State.LastRunClearedBySuccess) }
    $lastRunCleared += @($MovedToCleared)
    $lastRunCleared = Merge-RowsByKeyPreferLatestActivity $lastRunCleared
    Set-RowProp $State "LastRunClearedBySuccess" @($lastRunCleared)

    Write-JsonFile $State $StatePath
}

function Write-FinalMonitorSummary {
    param(
        [string]$ApiStatus,
        [int]$ActiveCount,
        [int]$ClearedCount,
        [int]$LifecycleCount,
        [int]$NewCount,
        [int]$OlderCount,
        [int]$CarriedCount,
        [int]$RefailedCount,
        [int]$RunningCount,
        [int]$CancelledCount,
        [int]$UnknownCount,
        [int]$WarningCount,
        [string]$ActiveTally,
        [string]$LifecycleTally,
        [string]$LatestSuccessReconcileLine
    )

    Write-Host ""
    Write-Host "Final Normalized Summary (matches worknotes_summary.txt):"
    Write-Host "Cohesity API Collection Status : $ApiStatus"
    Write-Host "Active / Unresolved Failures  : $ActiveCount"
    Write-Host "  Newly failed this check     : $NewCount"
    Write-Host "  Older/current still failing : $OlderCount"
    Write-Host "  Carried forward still failing: $CarriedCount"
    Write-Host "  Re-failed after earlier clear: $RefailedCount"
    Write-Host "  Running / awaiting completion: $RunningCount"
    Write-Host "  Cancelled after failure     : $CancelledCount"
    Write-Host "  Needs review / not verified : $UnknownCount"
    Write-Host "Cleared By Later Success      : $ClearedCount"
    Write-Host "Total Lifecycle Rows          : $LifecycleCount"
    Write-Host "Incomplete Collection Warnings: $WarningCount"
    Write-Host "Tally Check:"
    Write-Host "  $ActiveTally"
    Write-Host "  $LifecycleTally"
    Write-Host "  $LatestSuccessReconcileLine"
}

function Write-SingleWorknotesSummary([string]$Folder, [int]$RunLimit) {
    if ([string]::IsNullOrWhiteSpace($Folder) -or !(Test-Path $Folder)) { return }

    $statePath = Join-Path $Folder "state.json"
    $state = Read-JsonFile $statePath
    $currentPath = Join-Path $Folder "current_failures.csv"
    $clearedPath = Join-Path $Folder "cleared_by_success.csv"
    $lifecyclePath = Join-Path $Folder "incident_lifecycle.csv"

    $currentImported = Import-ReportCsv $currentPath
    $clearedImported = Import-ReportCsv $clearedPath

    $reconciled = Reconcile-LatestSuccessfulBackupRows -Current $currentImported -Cleared $clearedImported

    $current = Sort-ActiveRows $reconciled.Current
    $cleared = Sort-ClearedRows $reconciled.Cleared
    $lifecycle = Sort-LifecycleRows $reconciled.Lifecycle
    $movedToCleared = @($reconciled.MovedToCleared)

    Save-SortedCsv -Path $currentPath -Rows $current
    Save-SortedCsv -Path $clearedPath -Rows $cleared
    Save-SortedCsv -Path $lifecyclePath -Rows $lifecycle
    Update-StateWithReconciledRows -State $state -StatePath $statePath -Current $current -Cleared $cleared -Lifecycle $lifecycle -MovedToCleared $movedToCleared

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

    $activeCount = $current.Count
    $clearedCount = $cleared.Count
    $lifecycleCount = $lifecycle.Count
    $activeBreakdownTotal = $newFailures.Count + $older.Count + $carried.Count + $refailed.Count + $running.Count + $cancelled.Count + $unknown.Count
    $expectedLifecycleTotal = $activeCount + $clearedCount

    $activeTally = if ($activeBreakdownTotal -eq $activeCount) {
        "Active breakdown tally: $activeBreakdownTotal = $activeCount active/unresolved rows in current_failures.csv."
    } else {
        "Active breakdown tally requires review: child status total $activeBreakdownTotal does not match $activeCount active/unresolved rows in current_failures.csv."
    }

    $lifecycleTally = if ($expectedLifecycleTotal -eq $lifecycleCount) {
        "Lifecycle tally: $activeCount active/unresolved + $clearedCount cleared = $lifecycleCount total lifecycle rows."
    } else {
        "Lifecycle tally requires review: $activeCount active/unresolved + $clearedCount cleared = $expectedLifecycleTotal, but incident_lifecycle.csv has $lifecycleCount rows. Review incident_lifecycle.csv for row-level detail."
    }

    $latestSuccessReconcileLine = if ($movedToCleared.Count -gt 0) {
        "Latest-success reconciliation: $($movedToCleared.Count) row(s) were removed from current_failures.csv and moved to cleared_by_success.csv because LatestRunStatus was Succeeded/SucceededWithWarning after LastFailedET."
    } else {
        "Latest-success reconciliation: no active rows had a later successful LatestRunStatus."
    }

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
CSV Headers: $($script:CsvColumns -join ', ')
Sort Order: Cluster ascending; active failures by status priority and LastFailedET descending; cleared rows by ClearedET descending; lifecycle rows by status priority and latest activity descending.

Count Source:
- Active / unresolved count comes from current_failures.csv.
- Cleared count comes from cleared_by_success.csv.
- Total lifecycle count comes from incident_lifecycle.csv.

Summary Counts:
- Active / unresolved failures: $activeCount
  - Newly failed this check: $($newFailures.Count)
  - Older/current still failing: $($older.Count)
  - Carried forward still failing: $($carried.Count)
  - Re-failed after earlier clear: $($refailed.Count)
  - Running / awaiting completion: $($running.Count)
  - Cancelled after failure: $($cancelled.Count)
  - Needs review / not verified: $($unknown.Count)

- Cleared by later successful backup: $clearedCount
- Total lifecycle rows tracked: $lifecycleCount

Tally Check:
- $activeTally
- $lifecycleTally
- $latestSuccessReconcileLine

Action Summary:
- Work active/unresolved rows from current_failures.csv.
- Use incident_lifecycle.csv for the complete sortable lifecycle view.
- Rows counted as Needs review / not verified are unresolved until a later successful backup is verified.
- Rows with LatestRunStatus Succeeded or SucceededWithWarning after LastFailedET are not kept as active failures.

Incomplete Collection:
$(Format-Warnings $warnings)

$followUp

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
CSV Headers: $($script:CsvColumns -join ', ')

Closure Counts:
- Active / unresolved failures: $activeCount
  - Running / awaiting completion: $($running.Count)
  - Cancelled after failure: $($cancelled.Count)
  - Needs review / not verified: $($unknown.Count)
- Cleared by later successful backup: $clearedCount
- Total lifecycle rows tracked: $lifecycleCount

Tally Check:
- $activeTally
- $lifecycleTally
- $latestSuccessReconcileLine

Carry Forward / Handoff:
$(if ($activeCount -eq 0) { "No active backup failures remain based on the latest saved state." } else { "$activeCount active/unresolved rows remain in current_failures.csv and should be carried forward or separately tracked." })

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

    Write-FinalMonitorSummary `
        -ApiStatus $apiStatus `
        -ActiveCount $activeCount `
        -ClearedCount $clearedCount `
        -LifecycleCount $lifecycleCount `
        -NewCount $newFailures.Count `
        -OlderCount $older.Count `
        -CarriedCount $carried.Count `
        -RefailedCount $refailed.Count `
        -RunningCount $running.Count `
        -CancelledCount $cancelled.Count `
        -UnknownCount $unknown.Count `
        -WarningCount $warnings.Count `
        -ActiveTally $activeTally `
        -LifecycleTally $lifecycleTally `
        -LatestSuccessReconcileLine $latestSuccessReconcileLine
}

$target = Join-Path $PSScriptRoot "Get-CohesityBackupFailureWindowConsolidator.ps1"
if (!(Test-Path $target)) {
    throw "Main implementation script not found: $target"
}

$targetParams = @{}
foreach ($k in $PSBoundParameters.Keys) {
    if ($k -ne "ShowGrid") { $targetParams[$k] = $PSBoundParameters[$k] }
}

Write-Host ""
Write-Host "Collection stage will run first. The final normalized summary printed after post-processing is the source of truth and matches worknotes_summary.txt."
Write-Host ""

& $target @targetParams
$mainExitCode = $LASTEXITCODE

try {
    $folder = Get-ReportFolder -Root $OutputRoot -Inc $IncidentNumber
    if ($folder) {
        Write-SingleWorknotesSummary -Folder $folder -RunLimit $NumRuns
        Write-Host ""
        Write-Host "Text outputs normalized with tally-focused worknotes summary, latest-success reconciliation, clusterwise sorting, and no row truncation:"
        Write-Host (Join-Path $folder "worknotes_summary.txt")
        Write-Host (Join-Path $folder "closing_summary.txt")
        if ($ShowGrid) { Open-OutputGrid -Folder $folder }
    } else {
        Write-Warning "Unable to locate incident output folder for text normalization."
    }
} catch {
    Write-Warning "Text output normalization failed: $($_.Exception.Message)"
}

exit $mainExitCode
