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
5. Use one-step-at-a-time development.
6. Cohesity production calls must be GET-only unless explicitly changed.
7. No registry, state, incident locking, CSV, Excel, or SNOW logic until the basic steps work.

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
- Added first PowerShell connection test:
  - `Test-CohesityHeliosConnection.ps1`
- Added current runbook:
  - `RUNBOOK.md`
- Added actual work status file:
  - `WORK_STATUS.md`

## Current Test Step

Run only the connection test.

Expected result:

```text
Cluster count: <number>
ClusterName / ClusterId table
```

## Next Fixes / Next Build Steps

After connection test works:

1. Add compute-window print test in PowerShell.
2. List protection groups for one manually selected cluster.
3. List runs for one manually selected protection group.
4. Only then add failure classification logic.
5. Only after logic works, add CSV output.
6. Only after CSV works, add incident/work-note formatting.

## Current Stop Point

Waiting for user to run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

and share the result or exact error.
