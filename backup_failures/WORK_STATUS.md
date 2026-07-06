# Backup Failures - Work Status

## Current Scope

Folder in scope:

```text
backup_failures/
```

Files currently in scope:

```text
backup_failures/Get-CohesityBackupFailures.ps1
backup_failures/Test-CohesityHeliosConnection.ps1
backup_failures/cohesity_backup_failures.js
backup_failures/compute_window.js
backup_failures/RUNBOOK.md
backup_failures/WORK_STATUS.md
```

## Current Source Of Truth

The real backup-failure script is now:

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

## Current Completed Work

- Identified the correct 1125-line script:
  - `all_fail_do_not_delete`
- Restored it into current branch as:
  - `backup_failures/Get-CohesityBackupFailures.ps1`
- Updated current runbook to point to the restored real script.
- Marked `Test-CohesityHeliosConnection.ps1` as scratch.

## API Key Standard For Future Change

The project standard remains:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

Do not apply this change until the restored real script is confirmed working.

## Current Test Step

Copy and run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Get-CohesityBackupFailures.ps1
```

First validation should use the same menu option that previously showed the known failure.

Suggested first test:

```text
10. Consolidated (All Environments) - Silent
```

## What User Should Report

```text
Real script menu shown: yes/no
Option tested: number
Known failure reported: yes/no
CSV created: yes/no
Error: exact short error if any
```

## Next Fixes / Next Build Steps

After the restored script is tested:

1. If it reports the known failure, apply only the AES API-key standard.
2. Keep logic intact.
3. Then confirm CSV/output behavior.
4. Then decide whether to keep menu behavior or add a cluster selector.
5. Only later consider cleanup/refactor.
6. SNOW/work-note formatting remains later, not now.

## Current Stop Point

Waiting for user to run the restored real script and report whether the known failure appears.
