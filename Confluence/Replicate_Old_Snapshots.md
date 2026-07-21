# Replicate Existing Cohesity Snapshots

## Reference

https://github.com/bseltz-cohesity/scripts/tree/master/powershell/replicateOldSnapshots

> **This is a community-provided script and is not officially supported by Cohesity. Always run a preview and verify the selected snapshots before using `-commit`.**

> **When in doubt about snapshot selection, age calculation, retention, or use of resync, please reach out to me or BO before running the script with `-commit`.**

---

## 1. Summary

The `replicateOldSnapshots.ps1` script is used to **bulk replicate existing local Cohesity snapshots** to an already configured replication target.

Without this script, historical snapshots would need to be identified and replicated individually.

The standard process is:

```text
Raise CR → Run preview → Review snapshots → Run with -commit → Validate
```

The script can optionally:

- Filter snapshots using `-olderThan` and `-newerThan`.
- Set replica retention using `-keepFor`.
- Reprocess previously replicated snapshots using the full resync parameter.

> **Additional age-filter, retention, and resync examples are available at the end of this document.**

---

## 2. Sample Change Request

### Change Title

Bulk replication of existing Cohesity snapshots from `<SOURCE_CLUSTER>` to `<TARGET_CLUSTER>`.

### Reason for Change

Historical snapshots for `<PROTECTION_GROUP>` need to be replicated to `<TARGET_CLUSTER>`. The script allows the required snapshots to be selected and replicated in bulk instead of initiating replication individually.

### Implementation Plan

1. Log in using the privileged username `x_`.
2. Open PowerShell and navigate to `<SCRIPT_PATH>`.
3. Run the script **without `-commit`** to preview the eligible snapshots.
4. Review the snapshot dates, retention, and target cluster.
5. Attach the preview output to the CR.
6. Run the same command with `-commit`.
7. Monitor the replication tasks until completion.
8. Validate the expected snapshots on the target cluster.

### Preview Command

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>"
```

### Execution Command

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -commit
```

### Expected Impact

- Replication traffic will be generated between the source and target clusters.
- No interruption to existing backup jobs is expected.
- Only snapshots displayed and confirmed during the preview will be submitted.

### Validation Plan

- Confirm all submitted replication tasks complete successfully.
- Confirm the expected snapshots are visible on `<TARGET_CLUSTER>`.
- Confirm snapshot dates and expiration dates are correct.
- Add the replication result and validation evidence to the CR.

### Backout Plan

Replication tasks already submitted cannot be reversed by the script.

If an incorrect replica is created:

1. Stop any replication tasks that are still running, where possible.
2. Do not rerun the script.
3. Review the replicated snapshot on the target cluster.
4. Remove the incorrect replica only after the required review.
5. Record the actions taken in the CR.

### Risk

**Low to Medium**, depending on the number and size of snapshots selected.

The main risks are increased replication bandwidth, additional target capacity usage, and incorrect snapshot or retention selection. These risks are reduced by running the script in **preview mode before using `-commit`**.

---

## 3. Accessing the Script

The required scripts have already been downloaded and copied to:

```text
<SCRIPT_PATH>
```

The script must be executed using the privileged username:

```text
x_
```

Open PowerShell and navigate to the script directory:

```powershell
Set-Location "<SCRIPT_PATH>"
```

Confirm that both required files are present:

```powershell
Get-ChildItem .\replicateOldSnapshots.ps1, .\cohesity-api.ps1
```

Required files:

```text
replicateOldSnapshots.ps1
cohesity-api.ps1
```

---

## 4. How the Script Works

The script:

1. Connects to the source Cohesity cluster.
2. Confirms that the specified remote cluster is configured as a replication target.
3. Retrieves the selected protection group and its historical backup runs.
4. Applies `-olderThan` and `-newerThan` when age filtering is required.
5. Checks whether each snapshot has already replicated successfully.
6. Displays the snapshots eligible for replication.
7. Calculates replica retention.
8. Starts replication only when `-commit` is included.
9. Submits selected replication tasks in chronological order.

> **Without `-commit`, the script runs only in preview mode and does not create replication tasks.**

### Already Replicated Snapshots

If a snapshot has already replicated successfully to the specified target, the script displays:

```text
Already replicated <SNAPSHOT_DATE_TIME>
```

The snapshot is **ignored and will not be replicated again**.

To process an already replicated snapshot again, use the complete resync parameter:

```powershell
-resync_WARNING_READ_THE_README_YOU_PROBABLY_DONT_WANT_TO_DO_THIS
```

Use resync only when:

- The previously replicated backup was **deleted from the target cluster** and must be recreated.
- The existing replica retention must be **intentionally extended**.

> **Do not use resync only because the snapshot is shown as already replicated.**

> **When resync is combined with `-keepFor`, the replica retention may be extended again. Repeated executions may continue increasing the expiration date.**

---

## 5. Main Example — Replicate All Eligible Snapshots

This example processes all eligible snapshots for one protection group without applying an age filter or custom retention.

### Step 1: Preview

Run the script without `-commit`:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>"
```

Expected preview output:

```text
<PROTECTION_GROUP>

Would replicate <SNAPSHOT_DATE_TIME> for <RETENTION_DAYS> days
Would replicate <SNAPSHOT_DATE_TIME> for <RETENTION_DAYS> days
Already replicated <SNAPSHOT_DATE_TIME>
```

Review and confirm:

- The correct source and target clusters are used.
- The correct protection group is selected.
- The expected snapshots are listed.
- The displayed retention is correct.
- Already replicated snapshots are being skipped.

Paste the preview output into:

```text
<CR_NUMBER>
```

### Step 2: Execute

After reviewing the preview, run the same command with `-commit`:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -commit
```

Expected execution output:

```text
<PROTECTION_GROUP>

Replicating <SNAPSHOT_DATE_TIME> for <RETENTION_DAYS> days
Replicating <SNAPSHOT_DATE_TIME> for <RETENTION_DAYS> days

Performing replications in time order...
<SNAPSHOT_DATE_TIME>
<SNAPSHOT_DATE_TIME>
```

---

## 6. Important Date and Retention Notes

### Backup Date Versus Replication Date

The age filters are calculated using the **original backup date**, not the date on which replication is performed.

Example:

```text
Script execution date: 30 June
Original backup date: 1 May
Snapshot age: Approximately 60 days
```

To select the snapshot, calculate the difference between the script execution date and the original backup date.

> **This is important when selecting snapshots from a specific calendar date. Always verify the exact snapshot timestamp in preview mode.**

### `-keepFor` Calculation

The `-keepFor` value is also calculated from the **original backup date**, not the replication date.

Example:

```text
Original backup date: 1 May
Replication date: 30 June
-keepFor value: 90 days
Expected expiration: Approximately 30 July
```

This does **not** mean the replica will be retained for 90 days from 30 June.

> **Always calculate and confirm the expected expiration date before execution.**

---

## 7. Validation

After execution, verify:

### Source Cluster

- Replication tasks were created.
- Replication tasks completed successfully.
- Only the intended protection group was processed.
- The submitted snapshot count matches the preview.

### Target Cluster

- The expected snapshots are visible.
- Snapshot dates match the original backups.
- Replica expiration dates are correct.
- The snapshots can be browsed or selected for recovery.

Record the result in the CR:

```text
CR number: <CR_NUMBER>
Source cluster: <SOURCE_CLUSTER>
Target cluster: <TARGET_CLUSTER>
Protection group: <PROTECTION_GROUP>
Snapshots replicated: <SNAPSHOT_COUNT>
Execution result: <RESULT>
Validated by: <NAME>
Validation date: <DATE>
```

---

# Additional Examples

## A. Replicate Snapshots Older Than 30 Days

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -olderThan 30
```

Add `-commit` only after reviewing the preview.

---

## B. Replicate Snapshots from the Last 30 Days

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan 30
```

---

## C. Replicate Snapshots Between 30 and 60 Days Old

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan 60 `
    -olderThan 30
```

This selects snapshots from approximately:

```text
Execution date minus 60 days
to
Execution date minus 30 days
```

---

## D. Replicate a Snapshot from a Specific Date

The script accepts age in days rather than a calendar date.

Calculate the number of days between the execution date and the required backup date:

```powershell
$targetDate = Get-Date "<TARGET_BACKUP_DATE>"
$ageDays = [math]::Floor(((Get-Date) - $targetDate).TotalDays)

$newerThan = $ageDays + 1
$olderThan = $ageDays

Write-Host "Use -newerThan $newerThan -olderThan $olderThan"
```

Use the calculated values:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan <AGE_DAYS_PLUS_1> `
    -olderThan <AGE_DAYS>
```

Example for a snapshot approximately 45 days old:

```powershell
-newerThan 46 `
-olderThan 45
```

> **The filters use rolling 24-hour periods based on the script execution time. Confirm the exact snapshot timestamp in preview mode.**

---

## E. Apply Custom Retention with `-keepFor`

Preview:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan 60 `
    -olderThan 30 `
    -keepFor 90
```

Execute after validation:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan 60 `
    -olderThan 30 `
    -keepFor 90 `
    -commit
```

> **The 90-day retention is calculated from the original backup date.**

---

## F. Resync a Previously Replicated Snapshot

Use the complete parameter exactly as shown:

```powershell
-resync_WARNING_READ_THE_README_YOU_PROBABLY_DONT_WANT_TO_DO_THIS
```

Preview:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan <NEWER_THAN_DAYS> `
    -olderThan <OLDER_THAN_DAYS> `
    -resync_WARNING_READ_THE_README_YOU_PROBABLY_DONT_WANT_TO_DO_THIS
```

Execute after reviewing the preview:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan <NEWER_THAN_DAYS> `
    -olderThan <OLDER_THAN_DAYS> `
    -resync_WARNING_READ_THE_README_YOU_PROBABLY_DONT_WANT_TO_DO_THIS `
    -commit
```

> **Use resync only when the target replica was deleted or when its retention must intentionally be extended.**
