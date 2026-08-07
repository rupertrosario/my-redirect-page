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

You can also pass a password explicitly:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -password 'mypassword'
```

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

## Console Output

Collection progress is lightweight:

```text
Scanning replication queue on CLUSTER01...

[1/23] PG_SQL_PROD
[2/23] PG_VMWARE_PROD
...
[23/23] PG_FILESERVER
```

If replication is running, the script performs the same GET detail lookup used by the original queue script to calculate average running progress.

Example summary:

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

The last GET is used only when a `kRunning` replication exists and is needed to calculate running percentage.

## Source Reference

Based on:

```text
bseltz-cohesity/scripts/powershell/replicationQueue
```
