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
  - Object-level failedAttempt extraction.
  - Run-level failed row when object details are missing.
  - CSV output.
- Updated current runbook for simple no-parameter one-cluster testing.

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

Output:

```text
Console summary
Failure preview
CSV under X:\PowerShell\Data\Cohesity\BackupFailures
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
Selected cluster processed: yes/no
PG count shown: yes/no
Failure rows count: number or not shown
CSV saved: yes/no
Error: exact short error if any
```

## Next Fixes / Next Build Steps

After the one-cluster run:

1. Fix any PowerShell syntax/runtime issue.
2. Confirm cluster menu works.
3. Confirm PG count is reasonable.
4. Confirm runs are retrieved.
5. Confirm CSV path is created.
6. Review whether failure rows are correct.
7. Then improve classification if needed:
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
