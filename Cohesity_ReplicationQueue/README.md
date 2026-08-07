# Cohesity Replication Queue Summary

## Purpose

`replicationQueueSummary.ps1` is a reporting-enhanced version of the Cohesity community `replicationQueue.ps1` workflow.

It keeps the same Cohesity API approach used by the original script and adds a consolidated summary so you can quickly see:

- total replication tasks found
- successful replications
- currently running replications
- accepted work
- queued work
- failed, warning, and canceled replications
- remaining replication work still to go
- backup date
- protection group / job name
- active replication task ID
- active item count
- running, accepted, and queued item counts
- object-level replication status and percentage complete

The script does not change how replication is performed. The existing cancellation behavior from the source script is retained.

## Files

| File | Purpose |
|---|---|
| `replicationQueueSummary.ps1` | Replication queue collector and summary report |
| `cohesity-api.ps1` | Cohesity REST API helper required by the script |

`cohesity-api.ps1` must be in the same directory as `replicationQueueSummary.ps1`.

## Recommended Run

To show the active queue plus completed replication results:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -showFinished
```

Replace `mycluster`, `myusername`, and `local` with the values for your Cohesity environment.

### Active replication work only

Without `-showFinished`, finished replication rows are omitted from the task-detail table:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local
```

### Specific protection group

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -jobName "SQL_PROD" `
    -showFinished
```

### Multiple protection groups

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -jobName "SQL_PROD","VMWARE_PROD" `
    -showFinished
```

### Date filtering

Show replications older than 7 days:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -olderThan 7 `
    -showFinished
```

You can also use `-before`, `-after`, or `-newerThan` as supported by the original queue script.

## Live Progress

The queue scan now shows progress for every protection group so a long API call does not look like the script has silently stopped:

```text
Scanning replication queue on CLUSTER01...

[1/25] Getting tasks for SQL_PROD - querying up to 999 runs...
      Returned 999 backup runs; found 42 remote replication record(s).
[2/25] Getting tasks for VMWARE_PROD - querying up to 999 runs...
      Returned 620 backup runs; found 18 remote replication record(s).
```

After the initial queue scan, the summary is printed before the script performs the slower active-task detail lookups.

Active detail retrieval also shows progress:

```text
[Detail 1/18] SQL_PROD - 07/15/2026 01:00:00
[Detail 2/18] VMWARE_PROD - 07/16/2026 01:00:00
```

## Console Summary

Example:

```text
================ REPLICATION SUMMARY ================

Metric                    Count
------                    -----
Total Replication Tasks     120
Successful                   82
Running                       6
Accepted                      8
Queued                        4
Failed                        3
Warning                       1
Canceled                      0
Other Active                  0

Remaining to go: 18 task(s) - 6 Running, 8 Accepted, 4 Queued.
```

`Remaining to go` is intentionally shown as one sentence instead of another table metric. It counts replication records whose status is not one of the finished states:

```text
kCanceled
kSuccess
kFailure
kWarning
```

## Task Detail

For active replication tasks the script retrieves the replication task ID and active subtask information.

Example:

```text
BackupDate           ProtectionGroup ReplicationTaskId Status     Items RunningItems AcceptedItems QueuedItems Progress
----------           --------------- ----------------- ------     ----- ------------ ------------- ----------- --------
07/15/2026 01:00:00 SQL_PROD        1876543           kRunning      12            4             6           2 68%
07/16/2026 01:00:00 VMWARE_PROD     1876921           kRunning      24            7            12           5 42%
07/17/2026 01:00:00 ORACLE_PROD     1877055           kAccepted      8            0             8           0 0%
```

For completed copy runs, the original API flow may no longer return an active task object. In that case `ReplicationTaskId` is shown as `-`.

## Active Item Detail

For active remote copy tasks, each object/subtask is shown with its current status and percentage complete.

Example:

```text
BackupDate           ProtectionGroup ReplicationTaskId Status     Object          PercentComplete
----------           --------------- ----------------- ------     ------          ---------------
07/15/2026 01:00:00 SQL_PROD        1876543           kRunning   SQLSERVER01                  68
07/15/2026 01:00:00 SQL_PROD        1876543           kAccepted  SQLSERVER02                   0
```

## Output Files

Each run writes four CSV files in the current directory:

| File | Contents |
|---|---|
| `replicationQueue-<cluster>.csv` | Queue-level backup date, job, and status |
| `replicationQueue-<cluster>-activeObjects.csv` | Active object/subtask status and percent complete |
| `replicationQueue-<cluster>-summary.csv` | Consolidated status counts |
| `replicationQueue-<cluster>-taskDetails.csv` | Backup date, protection group, task ID, item counts, and progress |

## Status Interpretation

The script uses the same finished-state definition as the original `replicationQueue.ps1`:

```powershell
$finishedStates = @('kCanceled', 'kSuccess', 'kFailure', 'kWarning')
```

The summary reports:

| Summary | Status logic |
|---|---|
| Successful | `kSuccess` |
| Running | `kRunning` |
| Accepted | `kAccepted` |
| Queued | `kQueued` |
| Failed | `kFailure` |
| Warning | `kWarning` |
| Canceled | `kCanceled` |
| Other Active | Any other non-finished status |

## Cancellation Safety

The source script supports `-cancelAll`, `-cancelOutdated`, and `-commit`. These capabilities are retained.

Without `-commit`, cancellation commands run in test mode.

Do not use `-commit` until the queue output and selected filters have been reviewed.

## Source Reference

This script is based on the Cohesity community automation sample:

`bseltz-cohesity/scripts/powershell/replicationQueue`

The reporting additions are intended to make the current replication workload and remaining work easier to operationally track.
