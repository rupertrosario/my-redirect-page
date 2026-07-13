# THIS FILE MUST ALWAYS BE OVERWRITTEN. DO NOT CREATE NEW INSTRUCTION FILES.

# CURRENT INSTRUCTIONS - Backup Failure workflow

This is the only active instruction file for `backup_failures/`.

Whenever instructions change, overwrite this same file: `backup_failures/00_CURRENT_INSTRUCTIONS.md`.

Do not create numbered instruction files for normal guidance. Do not use old notes, patch files, TXT exports, or downloaded V2/V3/V4/V5 copies as instruction source of truth.

## Hard operating rule

The operator cannot run Git commands on the target machine.

Do not instruct the operator to run Git commands such as `git fetch`, `git pull`, `git clean`, `git status`, `git diff`, `git add`, `git commit`, or `git push`.

The assistant must push repository changes directly through GitHub when needed. For the operator, provide only:

```text
1. Downloadable file links from chat.
2. PowerShell copy-paste commands.
3. PowerShell validation commands.
```

## Repository scope

- Repo: `rupertrosario/my-redirect-page`
- Branch: `Cohesity_Automations`
- Folder: `backup_failures/`

## Folder should stay simple

Target working set:

```text
backup_failures/
  00_CURRENT_INSTRUCTIONS.md
  Cohesity_Backup_Failure_INC_Status_Update.ps1
  Get-CohesityBackupFailureWindowConsolidator.ps1
  Format-CohesityBackupFailureReport.ps1
```

Any local files such as older downloaded V2/V3/V4/V5 copies, old numbered instruction files, text exports, or temporary patch notes should not be used as source of truth.

## Local cleanup command - PowerShell only

Do not delete first. Move clutter to a local archive folder.

```powershell
$folder = "X:\PowerShell\Cohesity_API_Scripts\backup_failures"
$archive = Join-Path $folder ("_local_clutter_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Path $archive -Force | Out-Null

$keep = @(
  "00_CURRENT_INSTRUCTIONS.md",
  "Cohesity_Backup_Failure_INC_Status_Update.ps1",
  "Get-CohesityBackupFailureWindowConsolidator.ps1",
  "Format-CohesityBackupFailureReport.ps1"
)

Get-ChildItem $folder -File |
  Where-Object { $keep -notcontains $_.Name } |
  Move-Item -Destination $archive -Force

"Moved local clutter to: $archive"
Get-ChildItem $folder | Select Name | Sort-Object Name
```

## Current required consolidator level

Use the V6 consolidator file from chat:

```text
Get-CohesityBackupFailureWindowConsolidator_ENV_FIRST_FIXED_V6.ps1
```

V6 required markers:

```text
Recovery identity rule
ObjectKey must not include RunType
Recovery-aware rule
V5 state-reconciliation identity rule
V5 final reconciliation rule
V6 latest-success-over-running rule
V6 success-suppresses-running rule
V6 final active suppression rule
```

## Current lifecycle rule

The script must answer this question for each protected object:

```text
Is this object still in a bad terminal backup state after checking the latest backup runs?
```

The script must not report an object only because it had an older failed/cancelled/running run.

Correct lifecycle rules:

| Evidence | Expected result |
|---|---|
| Failed backup, no later success | Active/current problem |
| Cancelled backup, no later success | Active/current problem |
| Running backup, no success for same object | Active/running review only |
| Failed backup, later success | Cleared / recovered |
| Cancelled backup, later success | Cleared / recovered |
| Running backup, later success and no later terminal failure/cancelled | Not active |
| Success only | Not reported as problem |
| Previously open in state, now success | Cleared / recovered |
| Previously open in old state format, same object now success | Do not carry old row forward |

Important V6 rule:

```text
A same-object Running row must not override a successful completed backup.
If the same protected object has a success and no later terminal Failure/Cancelled, suppress Running from active output.
```

## Correct object identity rule

Object lifecycle matching should be based on:

```text
Cluster + ProtectionGroup + Environment + ObjectIdentity
```

Object lifecycle matching should not depend on `RunType`.

For state reconciliation, old saved rows may have old `ObjectKey` values that included `RunType`. V5/V6 must suppress those stale old rows by comparing protected-object identity from row fields, not by trusting only the saved `ObjectKey` string.

## Verification before test - positive checks only

Do not ask the operator to search for old strings.

Use only positive checks for the intended current logic:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Get-FileHash .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Algorithm SHA256

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Recovery identity rule","ObjectKey must not include RunType","Recovery-aware rule","V5 state-reconciliation identity rule","V5 final reconciliation rule","V6 latest-success-over-running rule","V6 success-suppresses-running rule","V6 final active suppression rule"
```

Expected for the intended V6 file: all markers must match.

## Full all-cluster validation run

The operator wants to validate against everything.

Use direct consolidator execution, not the wrapper, so `BaselineNumRuns` can be controlled.

Do not pass `-ClusterName`. Omitting `-ClusterName` means all clusters/all environments.

Recommended all-cluster validation command:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -NumRuns 5 `
  -BaselineNumRuns 5 `
  -RequestTimeoutSec 120
```

Then run formatter:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"

.\Format-CohesityBackupFailureReport.ps1 -ReportFolder $latest.FullName
```

If the all-cluster run is still too slow but must be complete, run it overnight with `-NumRuns 10 -BaselineNumRuns 10` only after V6 logic is confirmed with `5/5`.

## Full-run contradiction check

After the formatter runs, check whether the known bug still exists.

This check finds protected objects that have a success but are still shown as active Running/Cancelled/Failure in the same output.

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

$life = Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv")

$badStatuses = @('Failure','Cancelled','CancelledAfterFailure','Running','RunningAtLatestCheck','CurrentStillFailing','CarriedForwardStillFailing','CarriedForward')
$successStatuses = @('Success','NewlyClearedThisCheck','ClearedByLaterSuccess')

$contradictions = $life |
  Group-Object Cluster,ProtectionGroup,Environment,ObjectName,ObjectType |
  ForEach-Object {
    $rows = @($_.Group)
    $hasSuccess = @($rows | Where-Object { $successStatuses -contains $_.Status -or $_.StatusChange -eq 'Cleared' }).Count -gt 0
    $activeBad = @($rows | Where-Object { $badStatuses -contains $_.Status -or $badStatuses -contains $_.StatusChange })

    if ($hasSuccess -and $activeBad.Count -gt 0) {
      $activeBad
    }
  }

$contradictions |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LastSeenET,LatestSuccessET,Message |
  Format-Table -AutoSize

"Contradiction count: $(@($contradictions).Count)"
```

Expected:

```text
Contradiction count: 0
```

## Expected result for cancelled/running then success

For the same protected object:

```text
Cancelled + later Success = cleared/recovered, not active.
Running + Success and no later terminal Failure/Cancelled = not active.
Failed + later Success = cleared/recovered, not active.
```

The object should not remain active in:

```text
current_failures.csv
incident_lifecycle.csv active/problem section
```

as:

```text
Cancelled
CancelledAfterFailure
Failure
Running
CarriedForward
```

when the same protected object has success and no later terminal Failure/Cancelled.

## Performance rule

For all-cluster validation, use:

```text
NumRuns = 5
BaselineNumRuns = 5
All clusters = omit ClusterName
```

Only increase to 10/10 after the 5/5 validation passes.

Do not use 15/30 until the lifecycle logic is proven and runtime is acceptable.

## GitHub/source-of-truth rule

Do not claim the full consolidator is pushed unless it has been explicitly fetched from GitHub after update and verified.

For now, the downloadable V6 consolidator from chat is the file the operator should copy over the local consolidator.

## Operating rule going forward

When instructions change:

1. Overwrite `backup_failures/00_CURRENT_INSTRUCTIONS.md`.
2. Do not create a new numbered instruction file.
3. Keep chat instructions and this file aligned.
