# Cohesity Replication Queue Status

## Purpose

`replicationQueueSummary.ps1` is a minimal read-only replication status checker based on the Cohesity community `replicationQueue.ps1` logic.

It is intentionally limited to status reporting:

- Cohesity data operations are GET only
- no cancellation logic
- no `POST`, `PUT`, `PATCH`, or `DELETE` data operations
- no CSV files are created
- no task-detail or object-detail console sections
- one compact replication summary only

API-key authentication is required because the Cohesity helper validates API-key authentication with GET requests. Username/password authentication is not used because that authentication path performs `POST /login`.

## Files

Place these files in the same directory:

```text
replicationQueueSummary.ps1
cohesity-api.ps1
```

## Run Against a Cluster

If the API key is already cached by `cohesity-api.ps1`:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local
```

Or provide the API key through the existing helper password parameter:

```powershell
.\replicationQueueSummary.ps1 `
    -vip mycluster `
    -username myusername `
    -domain local `
    -password '<API_KEY>'
```

## Run Through Helios

```powershell
.\replicationQueueSummary.ps1 `
    -vip helios.cohesity.com `
    -username helios `
    -clusterName 'CLUSTER01'
```

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

During collection the script prints only lightweight progress:

```text
Scanning replication queue on CLUSTER01...

[1/23] PG_SQL_PROD
[2/23] PG_VMWARE_PROD
...
[23/23] PG_FILESERVER
```

If replication is currently running, the script performs the same GET detail lookup used by the original queue script to calculate average running progress.

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

## GET Operations

The status workflow uses GET requests for:

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
