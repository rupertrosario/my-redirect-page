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
- Output is now being generated.
- Current stage moved to output validation.

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

The script reports latest uncleared object-level failures.

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

## Current Validation Step

Validate output correctness for one selected cluster before running all clusters.

User should check 3 to 5 output rows against Cohesity UI:

```text
CSV created: yes/no
Checked rows: number
PG/object names match UI: yes/no
EndTimeET window correct: yes/no
Messages useful: yes/no
Any false positives: yes/no
Any missed known failure: yes/no
Run-level fallback rows acceptable: yes/no/not present
Error: exact short error if any
```

## Next Fixes / Next Build Steps

After output validation:

1. Fix any false positive or missed failure.
2. Confirm final column names.
3. Rename the script from test name to report name.
4. Run one full all-cluster test using `[0] All clusters`.
5. Only after all-cluster output is trusted, add final reporting polish.
6. SNOW/work-note formatting remains later, not now.

## Current Stop Point

Waiting for user to validate a few CSV rows against Cohesity UI and report the compact checklist.
