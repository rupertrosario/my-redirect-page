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
    [switch]$ShowGrid
)

$script:LifecycleColumns = @("Cluster","ProtectionGroup","Environment","Host","ObjectName","ObjectType","RunType","Status","OldestFailedET","NewestFailedET","LatestSuccessET","FailureRuns","Message")
$script:SuccessColumns = @("Cluster","ProtectionGroup","Environment","RunType","LatestSuccessET")

function Clean($v) {
    if ($null -eq $v) { return "" }
    if ($v -is [array]) { $v = $v -join " | " }
    return (([string]$v -replace "[\r\n]+", " ") -replace "\s+", " ").Trim()
}

function Read-JsonFile([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    try { $raw | ConvertFrom-Json } catch { $null }
}

function Write-JsonFile($Object, [string]$Path) {
    $Object | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Import-ReportCsv([string]$Path) {
    if (!(Test-Path $Path)) { return @() }
    try { @(Import-Csv -Path $Path) } catch { @() }
}

function Save-Csv([string]$Path, $Rows, [string[]]$Columns) {
    $list = @($Rows)
    if ($list.Count -eq 0) {
        ($Columns -join ",") | Set-Content -Path $Path -Encoding UTF8
    } else {
        $list | Select-Object $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Set-Prop($o, [string]$n, $v) {
    if ($null -eq $o) { return }
    if ($o.PSObject.Properties[$n]) { $o.$n = $v }
    else { $o | Add-Member -MemberType NoteProperty -Name $n -Value $v -Force }
}

function Parse-DateText([string]$Text) {
    $t = Clean $Text
    if (!$t) { return $null }
    $formats = @("yyyy-MM-dd HH:mm:ss","yyyy-MM-dd H:mm:ss","yyyy-MM-dd HH:mm","yyyy-MM-dd H:mm","M/d/yyyy H:mm:ss","M/d/yyyy HH:mm:ss","M/d/yyyy H:mm","M/d/yyyy HH:mm","M/d/yyyy h:mm:ss tt","M/d/yyyy hh:mm:ss tt","M/d/yyyy h:mm tt","M/d/yyyy hh:mm tt")
    foreach ($f in $formats) { try { return [datetime]::ParseExact($t, $f, [Globalization.CultureInfo]::InvariantCulture) } catch {} }
    try { [datetime]::Parse($t, [Globalization.CultureInfo]::InvariantCulture) } catch { $null }
}

function Date-Sort($v) {
    $d = Parse-DateText (Clean $v)
    if ($d) { return $d.ToString("yyyy-MM-dd HH:mm:ss") }
    $t = Clean $v
    if ($t) { return $t }
    return "0000-00-00 00:00:00"
}

function Get-ReportFolder([string]$Root, [string]$Inc) {
    if ($Inc) {
        $candidate = Join-Path $Root $Inc.Trim().ToUpper()
        if (Test-Path $candidate) { return $candidate }
    }
    $latest = Get-ChildItem -Path $Root -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -ne "Archive" } | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($latest) { return $latest.FullName }
    return ""
}

function Row-Key($r) {
    $key = Clean $r.ObjectKey
    if ($key) { return $key }
    @((Clean $r.Cluster),(Clean $r.Environment),(Clean $r.ProtectionGroup),(Clean $r.Host),(Clean $r.ObjectName),(Clean $r.RunType)) -join "|"
}

function Is-Success([string]$s) { (Clean $s) -in @("Succeeded","SucceededWithWarning","kSucceeded","kSucceededWithWarning") }
function Is-Cleared([string]$s) { (Clean $s) -in @("NewlyClearedThisCheck","ClearedByLaterSuccess") }

function Latest-Success-Clears($r) {
    if ($null -eq $r) { return $false }
    if (Is-Cleared $r.Status) { return $false }
    if (!(Is-Success $r.LatestRunStatus)) { return $false }
    $seen = Parse-DateText (Clean $r.LastSeenET)
    $failed = Parse-DateText (Clean $r.LastFailedET)
    if ($null -eq $seen -or $null -eq $failed) { return $false }
    $seen -gt $failed
}

function Merge-Latest($Rows) {
    $map = @{}
    foreach ($r in @($Rows)) {
        if ($null -eq $r) { continue }
        $key = Row-Key $r
        if (!$key) { continue }
        if (!$map.ContainsKey($key)) { $map[$key] = $r; continue }
        $oldSort = Date-Sort $(if ($map[$key].ClearedET) { $map[$key].ClearedET } elseif ($map[$key].LastFailedET) { $map[$key].LastFailedET } else { $map[$key].FirstFailedET })
        $newSort = Date-Sort $(if ($r.ClearedET) { $r.ClearedET } elseif ($r.LastFailedET) { $r.LastFailedET } else { $r.FirstFailedET })
        if ($newSort -ge $oldSort) { $map[$key] = $r }
    }
    @($map.Values)
}

function Reconcile($Current, $Cleared) {
    $active = @()
    $moved = @()
    foreach ($r in @($Current)) {
        if (Latest-Success-Clears $r) {
            $c = $r | Select-Object *
            Set-Prop $c "Status" "NewlyClearedThisCheck"
            Set-Prop $c "ClearedET" (Clean $c.LastSeenET)
            $moved += $c
        } else {
            $active += $r
        }
    }
    $finalCleared = Merge-Latest @($Cleared + $moved)
    [pscustomobject]@{
        Active = @($active)
        Cleared = @($finalCleared)
        Lifecycle = @(Merge-Latest @($active + $finalCleared))
        Moved = @($moved)
    }
}

function Display-ObjectName($r) {
    $name = Clean $r.ObjectName
    $type = Clean $r.ObjectType
    $pg = Clean $r.ProtectionGroup
    if (!$name) { return "" }
    if ($type -eq "ProtectionGroup") { return "" }
    if (($name -eq $pg) -and (!$type -or $type -eq "ProtectionGroup")) { return "" }
    return $name
}

function Display-ObjectType($r, [string]$DisplayName) {
    $type = Clean $r.ObjectType
    if (!$DisplayName) { return "" }
    if ($type -eq "ProtectionGroup") { return "" }
    return $type
}

function Convert-LifecycleRows($Rows) {
    foreach ($r in @($Rows)) {
        $dn = Display-ObjectName $r
        [pscustomobject]@{
            Cluster = Clean $r.Cluster
            ProtectionGroup = Clean $r.ProtectionGroup
            Environment = Clean $r.Environment
            Host = Clean $r.Host
            ObjectName = $dn
            ObjectType = Display-ObjectType $r $dn
            RunType = Clean $r.RunType
            Status = Clean $r.Status
            OldestFailedET = Clean $r.FirstFailedET
            NewestFailedET = Clean $r.LastFailedET
            LatestSuccessET = Clean $r.ClearedET
            FailureRuns = Clean $r.ConsecutiveFailureCount
            Message = Clean $r.Message
        }
    }
}

function Format-Rows($Rows, [string[]]$Columns) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add(($Columns -join " | "))
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add("- None")
        return ($lines -join [Environment]::NewLine)
    }
    foreach ($r in $list) {
        $values = @()
        foreach ($c in $Columns) {
            $prop = $r.PSObject.Properties[$c]
            if ($prop) { $values += (Clean $prop.Value) } else { $values += "" }
        }
        $lines.Add(($values -join " | "))
    }
    $lines -join [Environment]::NewLine
}

function Format-Warnings($Warnings) {
    $list = @($Warnings | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    if ($list.Count -eq 0) { return "- None" }
    ($list | ForEach-Object { "- $(Clean $_)" }) -join [Environment]::NewLine
}

function Rerun-Command([string]$Incident, [string]$Cluster, [int]$RunLimit) {
    $line = '.\Cohesity_Backup_Failure_INC_Status_Update.ps1'
    if ($Incident) { $line += ' -IncidentNumber "' + $Incident + '"' }
    if ($Cluster) { $line += ' -ClusterName "' + $Cluster + '"' }
    if ($RunLimit -ne 30) { $line += ' -NumRuns ' + $RunLimit }
    "cd X:\PowerShell\Cohesity_API_Scripts\backup_failures`n$line"
}

function Open-Grid([string]$Folder) {
    if (!(Get-Command Out-GridView -ErrorAction SilentlyContinue)) { Write-Warning "Out-GridView is not available. CSV files were still generated."; return }
    $p = Join-Path $Folder "incident_lifecycle.csv"
    if (!(Test-Path $p)) { Write-Warning "incident_lifecycle.csv was not found."; return }
    $rows = Import-ReportCsv $p
    if ($rows.Count -eq 0) { Write-Warning "incident_lifecycle.csv has no rows."; return }
    $rows | Out-GridView -Title "Cohesity - Incident Lifecycle"
}

function Remove-TemporaryOutputs([string]$Folder) {
    foreach ($name in @("current_failures.csv","cleared_by_success.csv","worknotes.txt","summary.txt")) {
        $p = Join-Path $Folder $name
        if (Test-Path $p) { Remove-Item -Path $p -Force -ErrorAction SilentlyContinue }
    }
}

function Write-FinalOutputs([string]$Folder, [int]$RunLimit) {
    $statePath = Join-Path $Folder "state.json"
    $state = Read-JsonFile $statePath
    $currentPath = Join-Path $Folder "current_failures.csv"
    $clearedPath = Join-Path $Folder "cleared_by_success.csv"
    $lifecyclePath = Join-Path $Folder "incident_lifecycle.csv"

    $reconciled = Reconcile (Import-ReportCsv $currentPath) (Import-ReportCsv $clearedPath)
    $active = @($reconciled.Active)
    $cleared = @($reconciled.Cleared)
    $lifecycle = @($reconciled.Lifecycle)
    $moved = @($reconciled.Moved)

    if ($state) {
        Set-Prop $state "CurrentOpenFailures" $active
        Set-Prop $state "ClearedBySuccess" $cleared
        Set-Prop $state "LifecycleRows" $lifecycle
        Set-Prop $state "LatestSuccessReconciledClearCount" $moved.Count
        $lastCleared = @()
        if ($state.PSObject.Properties["LastRunClearedBySuccess"]) { $lastCleared += @($state.LastRunClearedBySuccess) }
        $lastCleared += $moved
        Set-Prop $state "LastRunClearedBySuccess" @(Merge-Latest $lastCleared)
        Write-JsonFile $state $statePath
    }

    $lifecycleExport = @(Convert-LifecycleRows $lifecycle | Sort-Object Cluster,ProtectionGroup,Environment,@{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
    Save-Csv $lifecyclePath $lifecycleExport $script:LifecycleColumns

    $incident = if ($state -and $state.IncidentNumber) { Clean $state.IncidentNumber } else { Split-Path $Folder -Leaf }
    $windowLabel = if ($state -and $state.WindowLabel) { Clean $state.WindowLabel } else { "" }
    $generated = if ($state -and $state.LastRunET) { Clean $state.LastRunET } else { (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }
    $warnings = if ($state -and $state.Warnings) { @($state.Warnings) } else { @() }

    $activeExport = @(Convert-LifecycleRows $active | Sort-Object @{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
    $new = @($active | Where-Object Status -eq "NewlyFailedThisCheck")
    $older = @($active | Where-Object { $_.Status -eq "OlderStillFailing" -or $_.Status -eq "CurrentStillFailing" })
    $carried = @($active | Where-Object Status -eq "CarriedForwardStillFailing")
    $refailed = @($active | Where-Object Status -eq "ReFailedAfterClear")
    $running = @($active | Where-Object Status -eq "RunningAtLatestCheck")
    $cancelled = @($active | Where-Object Status -eq "CancelledAfterFailure")
    $unknown = @($active | Where-Object Status -eq "UnknownNeedsReview")
    $activeBreakdown = $new.Count + $older.Count + $carried.Count + $refailed.Count + $running.Count + $cancelled.Count + $unknown.Count

    $newlyCleared = @($lifecycleExport | Where-Object Status -eq "NewlyClearedThisCheck" | Sort-Object @{Expression={Date-Sort $_.LatestSuccessET};Descending=$true})
    $previouslyCleared = @($lifecycleExport | Where-Object Status -eq "ClearedByLaterSuccess")
    $expectedLifecycle = $active.Count + $newlyCleared.Count + $previouslyCleared.Count

    $activeTally = if ($activeBreakdown -eq $active.Count) { "Active breakdown tally: $activeBreakdown = $($active.Count) active/unresolved lifecycle rows." } else { "Active breakdown tally requires review: child status total $activeBreakdown does not match $($active.Count) active/unresolved lifecycle rows." }
    $lifecycleTally = if ($expectedLifecycle -eq $lifecycleExport.Count) { "Lifecycle tally: $($active.Count) active/unresolved + $($newlyCleared.Count) newly cleared this check + $($previouslyCleared.Count) previously cleared retained = $($lifecycleExport.Count) total lifecycle rows." } else { "Lifecycle tally requires review: $($active.Count) active/unresolved + $($newlyCleared.Count) newly cleared this check + $($previouslyCleared.Count) previously cleared retained = $expectedLifecycle, but incident_lifecycle.csv has $($lifecycleExport.Count) rows." }
    $successRecon = if ($moved.Count -gt 0) { "Latest-success reconciliation: $($moved.Count) active row(s) moved to NewlyClearedThisCheck because a later successful backup was found." } else { "Latest-success reconciliation: no active rows had a later successful backup." }

    $apiStatus = if ($warnings.Count -gt 0) { "Incomplete - $($warnings.Count) collection warning(s) recorded. See Incomplete Collection section." } else { "Complete - all collected scopes returned without recorded lookup warnings." }
    $followUp = if ($warnings.Count -gt 0) { "Retry Failed Collection Scope:`nRun the command below to refresh the incident output after the timed-out Cohesity API scope is available.`n`n$(Rerun-Command $incident $ClusterName $RunLimit)`n`nAfter the rerun completes, use the refreshed worknotes_summary.txt and incident_lifecycle.csv for the incident update." } else { "Retry Failed Collection Scope:`n- Not required for this run." }

    $failureText = Format-Rows $activeExport $script:LifecycleColumns
    $successText = Format-Rows (@($newlyCleared | Select-Object $script:SuccessColumns)) $script:SuccessColumns

    @"
Cohesity Backup Failure Incident Update

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Cohesity API Collection Status: $apiStatus
Scope: latest $RunLimit runs per protection group/run type.

Do Not Edit Generated Files:
- Do not manually edit incident_lifecycle.csv, worknotes_summary.txt, closing_summary.txt, or state.json.
- If the output looks incorrect, stale, or incomplete, rerun the script and use the refreshed files.

Summary Counts:
- Active / unresolved failures: $($active.Count)
- Newly cleared this check: $($newlyCleared.Count)
- Previously cleared rows retained in lifecycle CSV: $($previouslyCleared.Count)
- Total lifecycle rows tracked: $($lifecycleExport.Count)

Tally Check:
- $activeTally
- $lifecycleTally
- $successRecon

Team Focus:
- Focus on OlderStillFailing and UnknownNeedsReview rows in the Failure section.
- Success section only lists rows newly cleared in this check.

Failure Section:
$failureText

Success Section:
$successText

Incomplete Collection:
$(Format-Warnings $warnings)

$followUp

Files to Attach / Update:
- worknotes_summary.txt
- incident_lifecycle.csv
- closing_summary.txt, for closure or handoff

Script Memory:
- state.json is required by the script for lifecycle tracking. Do not manually edit or attach it.
"@ | Set-Content -Path (Join-Path $Folder "worknotes_summary.txt") -Encoding UTF8

    @"
Backup Failure Incident Closure Summary

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Cohesity API Collection Status: $apiStatus
Scope: latest $RunLimit runs per protection group/run type.

Do Not Edit Generated Files:
- Do not manually edit incident_lifecycle.csv, worknotes_summary.txt, closing_summary.txt, or state.json.
- If the output looks incorrect, stale, or incomplete, rerun the script and use the refreshed files.

Closure Counts:
- Active / unresolved failures: $($active.Count)
- Newly cleared this check: $($newlyCleared.Count)
- Previously cleared rows retained in lifecycle CSV: $($previouslyCleared.Count)
- Total lifecycle rows tracked: $($lifecycleExport.Count)

Tally Check:
- $activeTally
- $lifecycleTally
- $successRecon

Failure Section:
$failureText

Success Section:
$successText

Carry Forward / Handoff:
$(if ($active.Count -eq 0) { "No active backup failures remain based on the latest saved state." } else { "$($active.Count) active/unresolved rows remain in incident_lifecycle.csv and should be carried forward or separately tracked." })

Incomplete Collection:
$(Format-Warnings $warnings)

Evidence Files:
- incident_lifecycle.csv
- worknotes_summary.txt
- closing_summary.txt

Script Memory:
- state.json is required by the script for lifecycle tracking. Do not manually edit or attach it.
"@ | Set-Content -Path (Join-Path $Folder "closing_summary.txt") -Encoding UTF8

    Remove-TemporaryOutputs $Folder

    Write-Host ""
    Write-Host "Final Normalized Summary (matches worknotes_summary.txt):"
    Write-Host "Cohesity API Collection Status : $apiStatus"
    Write-Host "Active / Unresolved Failures   : $($active.Count)"
    Write-Host "Newly Cleared This Check       : $($newlyCleared.Count)"
    Write-Host "Previously Cleared Retained    : $($previouslyCleared.Count)"
    Write-Host "Total Lifecycle Rows           : $($lifecycleExport.Count)"
    Write-Host "Incomplete Collection Warnings : $($warnings.Count)"
    Write-Host "Tally Check:"
    Write-Host "  $activeTally"
    Write-Host "  $lifecycleTally"
    Write-Host "  $successRecon"
}

$target = Join-Path $PSScriptRoot "Get-CohesityBackupFailureWindowConsolidator.ps1"
if (!(Test-Path $target)) { throw "Main implementation script not found: $target" }

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
        Write-FinalOutputs -Folder $folder -RunLimit $NumRuns
        Write-Host ""
        Write-Host "Final operator-facing files:"
        Write-Host (Join-Path $folder "worknotes_summary.txt")
        Write-Host (Join-Path $folder "incident_lifecycle.csv")
        Write-Host (Join-Path $folder "closing_summary.txt")
        Write-Host "Script memory retained, do not edit:"
        Write-Host (Join-Path $folder "state.json")
        if ($ShowGrid) { Open-Grid $folder }
    } else {
        Write-Warning "Unable to locate incident output folder for text normalization."
    }
} catch {
    Write-Warning "Text output normalization failed: $($_.Exception.Message)"
}

exit $mainExitCode
