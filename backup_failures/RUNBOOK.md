# Backup Failures - Current Runbook

## Purpose

This file is the single current run/test instruction file for the `backup_failures` workstream.

It will be overwritten whenever run/test instructions change, so there is only one current instruction source.

## Current Script Under Test

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

This script has now moved beyond connection-only testing. It now does a simple cluster-selected backup-failure run.

## Local Copy Target

Copy the script to:

```text
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

## Standard Run Command

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

## Safer Test Run Command

Use this first if you want to limit load:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1 -MaxProtectionGroups 5 -MaxRunsPerProtectionGroup 10
```

## What The Script Does

1. Uses `ApiKeyAesHelper.ps1`.
2. Reads the encrypted API key from `Common\Secure\cohesity_apikey.enc`.
3. Computes the current 18:00 ET to next-day 18:00 ET window.
4. Calls Helios cluster list using GET.
5. Shows a cluster menu.
6. Allows:

```text
[0] All clusters
[1..N] One selected cluster
[X] Exit
```

7. For selected cluster(s), calls active protection groups using GET.
8. For each protection group, calls recent runs using GET.
9. Finds object-level failed attempts in the current compute window.
10. Adds run-level failure rows when the run is Failed but object failedAttempts are not returned.
11. Prints a summary and preview.
12. Saves one CSV unless `-NoCsv` is used.

## GET Endpoints Used

```text
GET https://helios.cohesity.com/v2/mcm/cluster-mgmt/info
GET https://helios.cohesity.com/v2/data-protect/protection-groups
GET https://helios.cohesity.com/v2/data-protect/protection-groups/{id}/runs
```

No Cohesity POST, PUT, PATCH, or DELETE calls are used.

## CSV Output Location

```text
X:\PowerShell\Data\Cohesity\BackupFailures
```

CSV name format:

```text
BackupFailures_<ClusterOrALL>_<WindowKey>_<Timestamp>.csv
```

## Output To Share Back

Share:

```text
Selected cluster
WindowKey
Summary table
Failure rows in window count
CSV saved path, if created
Any red error text
Line/char details, if PowerShell reports them
```

Do not share API key contents.
