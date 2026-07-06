# Backup Failures - Current Runbook

## Purpose

This file is the single current run/test instruction file for the `backup_failures` workstream.

It will be overwritten whenever run/test instructions change, so there is only one current instruction source.

## Current Script Under Test

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

The current test is a full object-level latest-uncleared failure check for one selected cluster, with fallback rows and counters for missed failed-run cases.

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

## Run Lookup Count

The standard lookup is now:

```text
30 runs per protection group
```

The script prints this on screen as:

```text
Runs/PG   : 30
```

There should be no separate 10-run test command now.

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
8. For each protection group, calls recent runs using GET with `numRuns=30`.
9. Walks runs newest-to-oldest.
10. Tracks object success rows as clear events.
11. Captures only the latest object failure where a newer object success has not cleared it.
12. Handles SQL/Oracle database object failures and host-level discovery failures separately.
13. Adds run-level fallback rows when a run is Failed but no object failure could be captured.
14. Filters final rows to the current 18:00 ET compute window.
15. Prints a summary, counters, and preview.
16. Saves one CSV unless `-NoCsv` is used.

## Logic Definition

The script is not listing every failed attempt.

It reports:

```text
Latest uncleared object-level failure per object
```

An older object failure is skipped when a newer object success is found for the same object key.

Object key priority:

```text
object.id
fallback: environment|objectType|name|sourceId
```

Fallback rule:

```text
If the run is Failed but object failedAttempts[] are not present/captured,
write one RunLevelFailedNoObjectFailureCaptured row for that run type,
unless a newer successful run type already cleared it.
```

## GET Endpoints Used

```text
GET https://helios.cohesity.com/v2/mcm/cluster-mgmt/info
GET https://helios.cohesity.com/v2/data-protect/protection-groups
GET https://helios.cohesity.com/v2/data-protect/protection-groups/{id}/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true
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
2. Does it show Runs/PG   : 30?
3. Did your selected cluster start processing?
4. Did it show ProtectionGroupsChecked in the final Summary table?
5. What is FailedRunsSeen?
6. What is FailedRunsInWindow?
7. What is ObjectsWithFailedAttempt?
8. What is ObjectRowsCaptured?
9. What is RunFallbackRowsCaptured?
10. What is LatestUnclearedRows?
11. Did it print CSV saved: <path>?
12. Was there any red error?
```

## What To Tell Back Here

Type only this manually:

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

Do not share API key contents.
