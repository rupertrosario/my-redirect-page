# Backup Failures - Current Runbook

## Purpose

This file is the single current run/test instruction file for the `backup_failures` workstream.

It will be overwritten whenever run/test instructions change, so there is only one current instruction source.

## Current Script Under Test

Use the restored real backup-failure script:

```text
backup_failures/Get-CohesityBackupFailures.ps1
```

This was restored from the older Git file:

```text
all_fail_do_not_delete
```

Do not use the scratch/test script for validation now:

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

## Local Copy Target

Copy the restored script to your local script folder:

```text
X:\PowerShell\Cohesity_API_Scripts\Get-CohesityBackupFailures.ps1
```

## Run Command

Run:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Get-CohesityBackupFailures.ps1
```

## Expected Menu

The restored script should show:

```text
COHESITY BACKUP FAILURES - MAIN MENU
1.  All Environments
2.  Oracle
3.  SQL
4.  Physical (File System)
5.  NAS / GenericNas
6.  Hyper-V
7.  Acropolis (AHV)
8.  Remote Adapter
9.  Isilon
10. Consolidated (All Environments) - Silent
11. Exit
```

## First Test

For first validation, use the same option that previously showed the known failure.

Preferred first test:

```text
10. Consolidated (All Environments) - Silent
```

Do not compare this with `Test-CohesityHeliosConnection.ps1`. That script is scratch and no longer the source of truth.

## Current Scope

Only validate that the restored real script runs and reports the known failure.

Do not change failure logic yet.

Do not add SNOW logic yet.

Do not refactor the script yet.

## What To Tell Back Here

No full output paste is needed.

Type only this manually:

```text
Real script menu shown: yes/no
Option tested: number
Known failure reported: yes/no
CSV created: yes/no
Error: exact short error if any
```

## Current Notes

The restored script is GET-only per header.

The restored script currently still has its original API-key section. AES-helper conversion is the next controlled change after the real script is confirmed working.
