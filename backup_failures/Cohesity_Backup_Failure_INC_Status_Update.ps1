<#
.SYNOPSIS
Production entry point for Cohesity Backup Failure INC status updates.

.DESCRIPTION
Run this wrapper for normal operation.
- If IncidentNumber is not supplied, the main collector asks once and stores it for the current 18:00 ET window.
- If ClusterName is not supplied, all clusters are scanned.
- OutputRoot defaults to the normal production folder.
- NumRuns is forced to 20 by default for production balance.
- RequestTimeoutSec is forced to 120 seconds by default for the main collector.
- Post-run cleanup suppresses stale PG/run-level rows when object-level evidence exists.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$LegacyFailureOutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailures",
    [string]$HelperPath = ("X:\PowerShell\Cohesity_API_Scripts\Common\" + "Api" + "KeyAesHelper.ps1"),
    [string]$EncryptedFile = ("X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_" + "api" + "key.enc"),
    [string]$ClusterName = "",
    [int]$NumRuns = 20,
    [string]$IncidentNumber = "",
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = "",
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35,
    [int]$RequestTimeoutSec = 120,
    [switch]$ShowGrid
)

function Clean-Text($Value) {
    if ($null -eq $Value) { return "" }
    return ([string]$Value).Trim()
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

function Test-BlankObjectRow($Row) {
    $objectName = Clean-Text $Row.ObjectName
    $objectType = Clean-Text $Row.ObjectType
    return (!$objectName -and !$objectType)
}

function Get-RowGroupKey($Row) {
    $cluster = Clean-Text $Row.Cluster
    $pg = Clean-Text $Row.ProtectionGroup
    $env = Clean-Text $Row.Environment
    $runType = Clean-Text $Row.RunType
    return ("{0}|{1}|{2}|{3}" -f $cluster, $pg, $env, $runType)
}

function Remove-InaccuratePgLevelRows($Rows) {
    $rowsList = @($Rows)
    if ($rowsList.Count -eq 0) { return @() }

    $groupsWithObjectRows = @{}
    foreach ($row in $rowsList) {
        if (!(Test-BlankObjectRow $row)) {
            $groupsWithObjectRows[(Get-RowGroupKey $row)] = $true
        }
    }

    $badBlankStatuses = @(
        "NewlyFailedThisCheck",
        "OlderStillFailing",
        "CurrentStillFailing",
        "CarriedForwardStillFailing",
        "ReFailedAfterClear"
    )

    $kept = @()
    foreach ($row in $rowsList) {
        if (!(Test-BlankObjectRow $row)) {
            $kept += $row
            continue
        }

        $groupKey = Get-RowGroupKey $row
        $status = Clean-Text $row.Status

        # If object-level evidence exists for the same cluster/PG/environment/run type,
        # the blank PG/run-level row is inaccurate and must not be shown.
        if ($groupsWithObjectRows.ContainsKey($groupKey)) { continue }

        # Old state created PG-level carried-forward/newly-failed rows before object promotion was fixed.
        # Do not retain those stale blank rows. A true no-object run-level row is emitted fresh as review status.
        if ($badBlankStatuses -contains $status) { continue }

        $kept += $row
    }

    return @($kept)
}

function Write-FilteredCsv([string]$Path) {
    if (!(Test-Path $Path)) { return }

    $header = Get-Content -Path $Path -First 1 -ErrorAction SilentlyContinue
    if (!$header) { return }

    $rows = @(Import-Csv -Path $Path)
    if ($rows.Count -eq 0) { return }

    $filtered = @(Remove-InaccuratePgLevelRows $rows)
    if ($filtered.Count -eq $rows.Count) { return }

    if ($filtered.Count -eq 0) {
        $header | Set-Content -Path $Path -Encoding UTF8
        return
    }

    $columns = @($rows[0].PSObject.Properties.Name)
    $filtered | Select-Object -Property $columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
}

function Update-StateJson([string]$Folder) {
    $statePath = Join-Path $Folder "state.json"
    if (!(Test-Path $statePath)) { return $null }

    try {
        $state = Get-Content -Path $statePath -Raw | ConvertFrom-Json
    } catch {
        Write-Warning "Unable to read state.json for PG-level cleanup."
        return $null
    }

    foreach ($name in @("CurrentOpenFailures", "ClearedBySuccess", "LastRunClearedBySuccess", "LifecycleRows")) {
        if ($state.PSObject.Properties[$name]) {
            $state.$name = @(Remove-InaccuratePgLevelRows @($state.$name))
        }
    }

    $state | ConvertTo-Json -Depth 100 | Set-Content -Path $statePath -Encoding UTF8
    return $state
}

function Format-Rows($Rows, [string[]]$Columns) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add(($Columns -join " | "))
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add("- None")
        return ($lines -join [Environment]::NewLine)
    }
    foreach ($row in $list) {
        $values = @()
        foreach ($column in $Columns) {
            $prop = $row.PSObject.Properties[$column]
            if ($prop) { $values += (Clean-Text $prop.Value) } else { $values += "" }
        }
        $lines.Add(($values -join " | "))
    }
    return ($lines -join [Environment]::NewLine)
}

function Rewrite-TextOutputs([string]$Folder, $State) {
    $lifecyclePath = Join-Path $Folder "incident_lifecycle.csv"
    if (!(Test-Path $lifecyclePath)) { return }

    $rows = @(Import-Csv -Path $lifecyclePath)
    $activeStatuses = @("NewlyFailedThisCheck", "OlderStillFailing", "CurrentStillFailing", "CarriedForwardStillFailing", "ReFailedAfterClear", "RunningAtLatestCheck", "CancelledAfterFailure", "UnknownNeedsReview")
    $currentRows = @($rows | Where-Object { $activeStatuses -contains (Clean-Text $_.Status) })
    $newlyClearedRows = @($rows | Where-Object { (Clean-Text $_.Status) -eq "NewlyClearedThisCheck" })
    $previouslyClearedCount = @($rows | Where-Object { (Clean-Text $_.Status) -eq "ClearedByLaterSuccess" }).Count

    $failureColumns = @("Cluster", "ProtectionGroup", "Environment", "Host", "ObjectName", "ObjectType", "RunType", "Status", "OldestFailedET", "NewestFailedET", "LatestSuccessET", "FailureRuns", "Message")
    $successColumns = @("Cluster", "ProtectionGroup", "Environment", "Host", "ObjectName", "ObjectType", "RunType", "LatestSuccessET", "Message")
    $failureText = Format-Rows $currentRows $failureColumns
    $successText = Format-Rows $newlyClearedRows $successColumns

    $incident = ""
    $windowLabel = ""
    $generated = ""
    $runs = $NumRuns
    if ($State) {
        $incident = Clean-Text $State.IncidentNumber
        $windowLabel = Clean-Text $State.WindowLabel
        $generated = Clean-Text $State.LastRunET
        if ($State.NumRuns) { $runs = $State.NumRuns }
    }
    if (!$incident) { $incident = Split-Path $Folder -Leaf }

    $work = New-Object System.Collections.Generic.List[string]
    $work.Add("Cohesity Backup Failure Incident Update")
    $work.Add("")
    $work.Add(("Incident: {0}" -f $incident))
    if ($windowLabel) { $work.Add(("Compute Window: {0}" -f $windowLabel)) }
    if ($generated) { $work.Add(("Generated At: {0} ET" -f $generated)) }
    $work.Add("Cohesity API Collection Status: Complete")
    $work.Add(("Scope: latest {0} runs per protection group/run type." -f $runs))
    $work.Add("")
    $work.Add("Summary Counts:")
    $work.Add(("- Active / unresolved failures: {0}" -f $currentRows.Count))
    $work.Add(("- Newly cleared this check: {0}" -f $newlyClearedRows.Count))
    $work.Add(("- Previously cleared rows retained in lifecycle CSV: {0}" -f $previouslyClearedCount))
    $work.Add(("- Total lifecycle rows tracked: {0}" -f $rows.Count))
    $work.Add("")
    $work.Add("Failure Section:")
    $work.Add($failureText)
    $work.Add("")
    $work.Add("Success Section:")
    $work.Add($successText)
    $work.Add("")
    ($work -join [Environment]::NewLine) | Set-Content -Path (Join-Path $Folder "worknotes_summary.txt") -Encoding UTF8

    $closing = New-Object System.Collections.Generic.List[string]
    $closing.Add("Backup Failure Incident Closure Summary")
    $closing.Add("")
    $closing.Add(("Incident: {0}" -f $incident))
    if ($windowLabel) { $closing.Add(("Compute Window: {0}" -f $windowLabel)) }
    if ($generated) { $closing.Add(("Generated At: {0} ET" -f $generated)) }
    $closing.Add("")
    $closing.Add("Failure Section:")
    $closing.Add($failureText)
    $closing.Add("")
    $closing.Add("Success Section:")
    $closing.Add($successText)
    $closing.Add("")
    ($closing -join [Environment]::NewLine) | Set-Content -Path (Join-Path $Folder "closing_summary.txt") -Encoding UTF8
}

function Repair-OutputRows([string]$Folder) {
    if (!$Folder -or !(Test-Path $Folder)) { return }

    foreach ($file in @("current_failures.csv", "cleared_by_success.csv", "incident_lifecycle.csv", "incident_lifecycle_raw.csv")) {
        Write-FilteredCsv (Join-Path $Folder $file)
    }

    $state = Update-StateJson $Folder
    Rewrite-TextOutputs -Folder $Folder -State $state
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

# Always pass production defaults even when the caller does not specify them.
# PowerShell does not include defaulted parameters in $PSBoundParameters.
if (!$targetParams.ContainsKey("NumRuns")) {
    $targetParams["NumRuns"] = $NumRuns
}
if (!$targetParams.ContainsKey("RequestTimeoutSec")) {
    $targetParams["RequestTimeoutSec"] = $RequestTimeoutSec
}

Write-Host ""
Write-Host "Running main Cohesity backup failure collector."
Write-Host ("OutputRoot        : {0}" -f $OutputRoot)
Write-Host ("NumRuns           : {0}" -f $targetParams["NumRuns"])
Write-Host ("RequestTimeoutSec : {0}" -f $targetParams["RequestTimeoutSec"])
if ($ClusterName) { Write-Host ("ClusterName       : {0}" -f $ClusterName) } else { Write-Host "ClusterName       : ALL CLUSTERS" }
if ($IncidentNumber) { Write-Host ("IncidentNumber    : {0}" -f $IncidentNumber) } else { Write-Host "IncidentNumber    : prompt/reuse current window" }
Write-Host ""

& $target @targetParams
$mainExitCode = $LASTEXITCODE

$folder = Get-ReportFolder -Root $OutputRoot -Inc $IncidentNumber
if ($folder) {
    Repair-OutputRows $folder
    Write-Host ""
    Write-Host "Final operator-facing files:"
    Write-Host (Join-Path $folder "worknotes_summary.txt")
    Write-Host (Join-Path $folder "incident_lifecycle.csv")
    Write-Host (Join-Path $folder "closing_summary.txt")
    $clearedPath = Join-Path $folder "cleared_by_success.csv"
    if (Test-Path $clearedPath) { Write-Host $clearedPath }
    Write-Host "Script memory retained, do not edit:"
    Write-Host (Join-Path $folder "state.json")
    if ($ShowGrid) { Open-Grid $folder }
} else {
    Write-Warning "Unable to locate incident output folder."
}

exit $mainExitCode
