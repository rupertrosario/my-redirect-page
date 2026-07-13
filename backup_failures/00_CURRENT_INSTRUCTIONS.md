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

## Current state

We are correcting the consolidator based on observed output.

Observed issue:

```text
Success exists for the same protected object, but the output still shows Running.
```

Correct rule:

```text
Running is not a terminal failure.
A same-object successful completed backup must suppress Running unless there is a later terminal Failure or Cancelled.
```

The active problem is the consolidator object-state reconciliation, not the wrapper and not the formatter.

## Required consolidator fix level

Use the V6 consolidator file from chat:

```text
Get-CohesityBackupFailureWindowConsolidator_ENV_FIRST_FIXED_V6.ps1
```

V6 includes all prior V5 rules and adds these required rules:

```text
V6 latest-success-over-running rule
V6 success-suppresses-running rule
V6 final active suppression rule
```

## Files and ownership

### Wrapper

File: `backup_failures/Cohesity_Backup_Failure_INC_Status_Update.ps1`

Rules:
- Keep wrapper small.
- Wrapper should call the consolidator.
- Wrapper should run formatter only after consolidator succeeds.
- Do not add collection logic, patching logic, or lifecycle logic to wrapper.

### Formatter

File: `backup_failures/Format-CohesityBackupFailureReport.ps1`

Rules:
- Formatter only formats consolidator output.
- Formatter should not call Cohesity APIs.
- Formatter should not decide object lifecycle.
- Formatter may normalize legacy/new statuses for reporting.

### Consolidator

File: `backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1`

Rules:
- Consolidator owns Cohesity API collection.
- Consolidator owns object identity.
- Consolidator owns failure/running/cancelled/success lifecycle logic.
- Consolidator owns state comparison.
- Consolidator must report object-level backup state, not only protection-group-level failure.
- Consolidator must not carry forward an old cancelled/failed/running row when the same protected object has newer success.
- Consolidator must not report Running as active when the same protected object has a successful completed backup and no later terminal Failure/Cancelled.

## Verification before test - positive checks only

Do not ask the operator to search for old strings.

Use only positive checks for the intended current logic:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

Get-FileHash .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Algorithm SHA256

Select-String .\Get-CohesityBackupFailureWindowConsolidator.ps1 -Pattern `
"Recovery identity rule","ObjectKey must not include RunType","Recovery-aware rule","V5 state-reconciliation identity rule","V5 final reconciliation rule","V6 latest-success-over-running rule","V6 success-suppresses-running rule","V6 final active suppression rule"
```

Expected for the intended V6 file:

```text
Recovery identity rule                  = match
ObjectKey must not include RunType      = match
Recovery-aware rule                     = match
V5 state-reconciliation identity rule   = match
V5 final reconciliation rule            = match
V6 latest-success-over-running rule     = match
V6 success-suppresses-running rule      = match
V6 final active suppression rule        = match
```

If these markers are missing, the operator is not using the intended latest consolidator file.

## Correct lifecycle rules

| Evidence | Expected result |
|---|---|
| Failed backup, no later success | Active/current problem |
| Cancelled backup, no later success | Active/current problem |
| Running backup, no same-object success in lookback/state | Active/pending review |
| Running backup, same-object success exists and no later terminal Failure/Cancelled | Not active |
| Failed backup, later success | Cleared / recovered |
| Cancelled backup, later success | Cleared / recovered |
| Running backup, later success | Cleared / recovered if it was previously open |
| Success only | Not reported as problem |
| Previously open in state, now success | Cleared / recovered |
| Previously open in old state format, same object now success | Do not carry old row forward |

## Correct object identity rule

Object lifecycle matching should be based on:

```text
Cluster + ProtectionGroup + Environment + ObjectIdentity
```

Object lifecycle matching should not depend on `RunType`.

For state reconciliation, old saved rows may have old `ObjectKey` values that included `RunType`. V5/V6 must suppress those stale old rows by comparing protected-object identity from row fields, not by trusting only the saved `ObjectKey` string.

## Running vs success rule

Use this precedence:

```text
Later terminal Failure/Cancelled after success = active problem.
Success with no later terminal Failure/Cancelled = healthy / cleared.
Running after success = do not reopen or keep active.
Running with no success and no final result = active/pending review.
```

Reason:

```text
Running is not final evidence of failure. The last completed successful backup should not be overridden by a currently running attempt.
```

## Testing rule

Do not test lifecycle logic with all clusters and 15/30 runs.

Use one known cluster and one known protection group/object where the problem is visible.

Run the consolidator directly for a small test:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures

.\Get-CohesityBackupFailureWindowConsolidator.ps1 `
  -IncidentNumber "INC999998" `
  -OutputRoot "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" `
  -ClusterName "<EXACT_CLUSTER_NAME>" `
  -NumRuns 3 `
  -BaselineNumRuns 3 `
  -RequestTimeoutSec 60
```

Then run formatter:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

.\Format-CohesityBackupFailureReport.ps1 -ReportFolder $latest.FullName
```

## Inspect one known object

Replace `<OBJECT_NAME>` with the object that had success but still appeared as Running:

```powershell
$latest = Get-ChildItem "X:\PowerShell\Data\Cohesity\BackupFailureWindow_Test" -Directory |
  Where-Object { $_.Name -ne "Archive" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

"Latest folder: $($latest.FullName)"

$objectName = "<OBJECT_NAME>"

"--- lifecycle ---"
Import-Csv (Join-Path $latest.FullName "incident_lifecycle.csv") |
  Where-Object { $_.ObjectName -like "*$objectName*" } |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LastSeenET,LatestSuccessET,Message |
  Format-Table -AutoSize

"--- cleared ---"
Import-Csv (Join-Path $latest.FullName "cleared_by_success.csv") |
  Where-Object { $_.ObjectName -like "*$objectName*" } |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LatestSuccessET,Message |
  Format-Table -AutoSize

"--- current failures ---"
Import-Csv (Join-Path $latest.FullName "current_failures.csv") |
  Where-Object { $_.ObjectName -like "*$objectName*" } |
  Select Cluster,ProtectionGroup,Environment,ObjectName,ObjectType,RunType,Status,StatusChange,LastSeenET,Message |
  Format-Table -AutoSize
```

## Expected result for success plus running

The object should not remain active in:

```text
current_failures.csv
```

as:

```text
Running
Existing
CarriedForward
```

unless there is a later terminal Failure or Cancelled after the successful backup.

## Performance rule

For testing:

```text
NumRuns = 3
BaselineNumRuns = 3
One cluster only
```

For production defaults, do not assume 15/30 is acceptable. Decide after lifecycle logic is proven.

Candidate production defaults after validation:

```text
Incremental NumRuns = 5
BaselineNumRuns = 10 or 15
```

## GitHub/source-of-truth rule

Do not claim the full consolidator is pushed unless it has been explicitly fetched from GitHub after update and verified.

For now, the downloadable V6 consolidator from chat is the file the operator should copy over the local consolidator.

## Operating rule going forward

When instructions change:

1. Overwrite `backup_failures/00_CURRENT_INSTRUCTIONS.md`.
2. Do not create a new numbered instruction file.
3. Keep chat instructions and this file aligned.
