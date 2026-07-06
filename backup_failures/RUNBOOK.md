# Backup Failures - Current Runbook

## Purpose

This file is the single current run/test instruction file for the `backup_failures` workstream.

It will be overwritten whenever run/test instructions change, so there is only one current instruction source.

## Current Script Under Test

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

The current test is not a limited PG test. It is a full check for one selected cluster.

## Local Copy Target

Copy the script to:

```text
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

## Run Command

Run the script without parameters:

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

When the cluster menu appears, choose one cluster number.

Do not choose `[0] All clusters` yet. Use `[0] All clusters` only after one-cluster testing is correct.

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

## What To Check On Client Network

You do not need to paste a full summary.

Just check these items on screen:

```text
1. Did the cluster menu appear?
2. Did your selected cluster start processing?
3. Did it show ProtectionGroupsChecked in the final Summary table?
4. Did it show Failure rows in window: <number>?
5. Did it print CSV saved: <path>?
6. Was there any red error?
```

## What To Tell Back Here

Type only this manually:

```text
Menu: yes/no
Selected cluster processed: yes/no
PG count shown: yes/no
Failure rows count: number or not shown
CSV saved: yes/no
Error: exact short error if any
```

Do not share API key contents.
