# Backup Failures - Work Status

## Current Scope

Folder in scope:

```text
backup_failures/
```

Files currently in scope:

```text
backup_failures/Get-CohesityBackupFailures.ps1
backup_failures/Get-CohesityBackupFailures_AES.ps1
backup_failures/Test-CohesityHeliosConnection.ps1
backup_failures/cohesity_backup_failures.js
backup_failures/compute_window.js
backup_failures/RUNBOOK.md
backup_failures/WORK_STATUS.md
```

## Current Source Of Truth

The real backup-failure logic is in:

```text
backup_failures/Get-CohesityBackupFailures.ps1
```

It was restored from the older Git file:

```text
all_fail_do_not_delete
```

Current restored line count:

```text
1125 lines
```

Header:

```text
Cohesity Backup Failures - Multi-Cluster via Helios
READ-ONLY / GET-only
```

## Current Test Entry Point

Use the AES runner for testing:

```text
backup_failures/Get-CohesityBackupFailures_AES.ps1
```

The AES runner loads the real script, replaces only the old API-key block in memory, and runs the original logic with:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

It does not write the API key to disk.

## Scratch / Do Not Validate Against This

The following file is scratch only and is no longer the validation target:

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

It was a side-track test script and should not be used to judge backup-failure logic.

## Current Decision Rules

1. Run/test instructions are pushed to Git.
2. `RUNBOOK.md` is the one current run/test instruction file and may be overwritten each time instructions change.
3. `WORK_STATUS.md` tracks status, decisions, current scope, and next fixes.
4. The restored real script is validated before any refactor.
5. Cohesity production calls must remain GET-only unless explicitly changed.
6. No SNOW update logic yet.
7. No failure-logic rewrite until the restored script is tested.
8. Current AES change is isolated to the runner so the failure logic remains untouched.

## Current Completed Work

- Identified the correct 1125-line script:
  - `all_fail_do_not_delete`
- Restored it into current branch as:
  - `backup_failures/Get-CohesityBackupFailures.ps1`
- Added AES runner:
  - `backup_failures/Get-CohesityBackupFailures_AES.ps1`
- Updated current runbook to point to the AES runner.
- Marked `Test-CohesityHeliosConnection.ps1` as scratch.

## Current Test Step

Copy both files to the same local folder:

```text
X:\PowerShell\Cohesity_API_Scripts\Get-CohesityBackupFailures.ps1
X:\PowerShell\Cohesity_API_Scripts\Get-CohesityBackupFailures_AES.ps1
```

Run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Get-CohesityBackupFailures_AES.ps1
```

First validation should use the same menu option that previously showed the known failure.

Suggested first test:

```text
10. Consolidated (All Environments) - Silent
```

## What User Should Report

```text
AES runner started: yes/no
Real script menu shown: yes/no
Option tested: number
Known failure reported: yes/no
CSV created: yes/no
Error: exact short error if any
```

## Next Fixes / Next Build Steps

After the AES runner test:

1. If it reports the known failure, decide whether to permanently replace the API-key block in the real script.
2. Keep failure logic intact.
3. Confirm CSV/output behavior.
4. Then decide whether to keep menu behavior or add a cluster selector.
5. Only later consider cleanup/refactor.
6. SNOW/work-note formatting remains later, not now.

## Current Stop Point

Waiting for user to run the AES runner and report whether the known failure appears.
