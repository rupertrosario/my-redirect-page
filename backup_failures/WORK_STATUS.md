# Backup Failures - Work Status

## Current Scope

Folder in scope:

```text
backup_failures/
```

Files currently in scope:

```text
backup_failures/cohesity_backup_failures.js
backup_failures/compute_window.js
backup_failures/Test-CohesityHeliosConnection.ps1
backup_failures/RUNBOOK.md
backup_failures/WORK_STATUS.md
```

Do not bulk-edit older Cohesity scripts outside this folder.

## Current Decision Rules

1. Run/test instructions are pushed to Git.
2. `RUNBOOK.md` is the one current run/test instruction file and may be overwritten each time instructions change.
3. `WORK_STATUS.md` tracks status, decisions, current scope, and next fixes.
4. Existing scripts outside `backup_failures/` are not changed unless explicitly requested.
5. Use controlled development.
6. Cohesity production calls must be GET-only unless explicitly changed.
7. No registry, state files, incident locking, Excel, or SNOW update logic at this stage.

## API Key Standard For PowerShell Scripts

Use:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

Do not directly read the API key as plain text in new PowerShell scripts.

## Current Completed Work

- Cleaned earlier experimental backup failure files.
- Kept the two Dynatrace JavaScript references:
  - `cohesity_backup_failures.js`
  - `compute_window.js`
- Added PowerShell script:
  - `Test-CohesityHeliosConnection.ps1`
- Phase 1 connection test completed by user.
- Updated the PowerShell script to include:
  - 18:00 ET compute window.
  - Cluster selection menu.
  - Protection group discovery for selected cluster(s).
  - Recent run lookup per protection group.
  - Object-level latest-uncleared failure logic.
  - Object success clear tracking.
  - SQL/Oracle database and host-level failure handling.
  - Run-level fallback row when no object failure is captured from a failed run.
  - Diagnostic counters for failed runs and captured rows.
  - CSV output.
- Standardized run lookup to 30 runs per protection group.
- Updated current runbook for the 30-run standard.

## Current Script Behavior

Current script:

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

Menu behavior:

```text
[0] All clusters
[1..N] One selected cluster
[X] Exit
```

Run lookup:

```text
numRuns=30
```

Output:

```text
Console summary
Latest uncleared failure preview
CSV under X:\PowerShell\Data\Cohesity\BackupFailures
```

## Current Failure Logic

The script now reports latest uncleared object-level failures.

It does not simply dump every failed attempt.

Process per protection group:

1. Pull 30 recent runs with object details.
2. Sort runs newest to oldest.
3. For each object, mark newer successful snapshots as cleared.
4. Capture the first/newest failure not cleared by a newer success.
5. Skip older failures for that same object.
6. Add run-level fallback row if a failed run has no captured object failure.
7. Filter final rows to the current 18:00 ET compute window.

Object key priority:

```text
object.id
fallback: environment|objectType|name|sourceId
```

## Current Test Step

Run the script without parameters.

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

Then choose one cluster number from the menu.

Do not choose `[0] All clusters` yet.

## What User Should Check On Client Network

The user cannot paste full output from the client network. They only need to manually report:

```text
Menu: yes/no
Runs/PG shows 30: yes/no
Selected cluster processed: yes/no
PG count shown: yes/no
FailedRunsSeen: number
FailedRunsInWindow: number
ObjectsWithFailedAttempt: number
ObjectRowsCaptured: number
RunFallbackRowsCaptured: number
LatestUnclearedRows: number
CSV saved: yes/no
Error: exact short error if any
```

## Next Fixes / Next Build Steps

After the one-cluster run:

1. Fix any PowerShell syntax/runtime issue.
2. Confirm cluster menu works.
3. Confirm PG count is reasonable.
4. Confirm failed-run counters and latest-uncleared row count are reasonable.
5. Confirm CSV path is created.
6. Review whether failure rows are correct.
7. Improve classification if needed:
   - Still failing.
   - Recovered in window.
   - New failure.
   - Re-failed.
   - Consecutive failure.
8. Only after one-cluster output is trusted, test `[0] All clusters`.

## Current Stop Point

Waiting for user to copy/run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

choose one cluster, and manually report the checklist.
