# Cohesity Replication Queue Status

## Purpose

`replicationQueueSummary.ps1` is a minimal replication status checker based on the Cohesity community `replicationQueue.ps1` logic.

It is intentionally limited to status reporting:

- normal Cohesity username/password authentication
- authentication uses the standard `POST /login`
- after authentication, all replication/status operations are GET only
- no cancellation logic
- no PUT, PATCH, or DELETE operations
- no CSV files are created
- one compact replication summary only

## Files

Place these files in the same directory:

```text
replicationQueueSummary.ps1
cohesity-api.ps1
```

## Run

Use the Cohesity cluster VIP/name directly:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local
```

If no `-password` is supplied, `cohesity-api.ps1` uses its normal cached-password / interactive prompt behavior.

For an AD account, replace `local` with the appropriate domain.

This version is intended for a direct cluster connection, not `helios.cohesity.com`.

## Optional Protection Group Filter

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -jobName 'SQL_PROD'
```

Multiple protection groups can be supplied:

```powershell
-jobName 'SQL_PROD','VMWARE_PROD'
```

`-numRuns` controls how many recent runs are inspected per protection group and defaults to `999`.

## How Running Status Is Detected

The queue-level `copyRun.status` is not sufficient by itself.

A remote copy can still have a top-level status such as `kAccepted` while one or more replication subtasks are already `kRunning`.

The script therefore follows the same pattern as the original `replicationQueue.ps1`:

1. Read remote copy runs.
2. Treat `kCanceled`, `kSuccess`, `kFailure`, and `kWarning` as finished.
3. For every non-finished remote copy, GET the detailed backup run.
4. Read replication `activeCopySubTasks` where `snapshotTarget.type = 2`.
5. If any active replication subtask is `kRunning`, classify that replication as `kRunning` and calculate its percentage.
6. Otherwise report `kAccepted`, `kSkipped`, or the actual status returned by Cohesity.

This prevents an active replication from being incorrectly reported as only waiting.

## Console Output

Example:

```text
================ REPLICATION SUMMARY ================

Status       Count Detail
------       ----- ------
kSuccess        82 -
kRunning         6 47% avg
kAccepted       12 Waiting
kSkipped         4 -
kFailure         3 -
kWarning         1 -
kCanceled        0 -

Remaining to go: 18 - 6 kRunning, 12 kAccepted (waiting).
```

If Cohesity returns another status not listed above, the script adds that exact status to the summary as `Returned by API` rather than hiding it.

## API Behavior

Authentication:

```text
POST /login
```

Status collection after authentication uses GET requests for:

```text
cluster
protectionJobs
protectionRuns?jobId=<jobId>&numRuns=<numRuns>&excludeTasks=true
/backupjobruns?allUnderHierarchy=true&exactMatchStartTimeUsecs=<time>&id=<jobId>
```

The detailed `/backupjobruns` GET is executed for each non-finished remote replication so the script can read the actual active replication subtask status and running percentage.

## Source Reference

Based on:

```text
bseltz-cohesity/scripts/powershell/replicationQueue
```
