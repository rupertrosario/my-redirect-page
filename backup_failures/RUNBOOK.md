# Backup Failures - Current Runbook

## Purpose

This file is the single current run/test instruction file for the `backup_failures` workstream.

It will be overwritten whenever run/test instructions change, so there is only one current instruction source.

## Current Script Under Test

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

The current test is now an output validation test for one selected cluster.

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

Do not choose `[0] All clusters` yet. Use `[0] All clusters` only after one-cluster output validation is correct.

## Run Lookup Count

The standard lookup is:

```text
30 runs per protection group
```

The script prints this on screen as:

```text
Runs/PG   : 30
```

## CSV Output Behavior

The script now always creates a CSV unless `-NoCsv` is used.

If failure rows exist, the CSV contains the failure rows.

If zero failure rows are found, the script still creates a header-only CSV. This gives evidence that the report ran and found no current rows.

CSV output location:

```text
X:\PowerShell\Data\Cohesity\BackupFailures
```

CSV name format:

```text
BackupFailures_<ClusterOrALL>_<WindowKey>_<Timestamp>.csv
```

The screen must show:

```text
CSV saved: <path>
```

## Current Validation Step

Because output is now being generated, the next step is to validate row correctness against Cohesity UI for one known failing cluster.

Check 3 to 5 rows from the CSV/output against Cohesity UI:

```text
1. Cluster name matches.
2. Protection group name matches.
3. Object/database/VM/host name matches the failed object in Cohesity.
4. EndTimeET is within the current 18:00 ET compute window.
5. FailedMessage is useful and not blank.
6. A newer success for the same object is not being reported as a failure.
7. RunLevelFailedNoObjectFailureCaptured rows are acceptable only when Cohesity does not expose object failedAttempts[].
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

## What To Tell Back Here

No full output paste is needed.

Type only this manually after validating a few rows:

```text
CSV saved line shown: yes/no
CSV file present in folder: yes/no
Checked rows: number
PG/object names match UI: yes/no
EndTimeET window correct: yes/no
Messages useful: yes/no
Any false positives: yes/no
Any missed known failure: yes/no
Run-level fallback rows acceptable: yes/no/not present
Error: exact short error if any
```

Do not share API key contents.
