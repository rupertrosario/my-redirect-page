# Replicate Existing Cohesity Snapshots

## Reference

https://github.com/bseltz-cohesity/scripts/tree/master/powershell/replicateOldSnapshots

> **This is a community-provided script and is not officially supported by Cohesity. Always run a preview and verify the selected snapshots before using `-commit`.**

> **When in doubt about snapshot selection, age calculation, retention, or use of resync, please reach out to me or BO before running the script with `-commit`.**

---

## 1. Summary

The `replicateOldSnapshots.ps1` script is used to **bulk replicate existing local Cohesity snapshots** to an already configured replication target.

Without this script, historical snapshots would need to be selected and replicated individually.

The standard process is:

```text
Raise CR → Run preview → Review snapshots → Run with -commit → Validate
```

The script can optionally:

- Filter snapshots using `-olderThan` and `-newerThan`.
- Set replica retention using `-keepFor`.
- Reprocess previously replicated snapshots using the complete resync parameter.

> **Additional age-filter, retention, resync, and sample CR examples are available at the end of this document.**

---

## 2. Accessing the Script

The required scripts have already been downloaded and copied to:

```text
<SCRIPT_PATH>
```

Open PowerShell using **Run as different user** and run it as the privileged username:

```text
x_
```

Navigate to the script directory:

```powershell
Set-Location "<SCRIPT_PATH>"
```

Confirm that the required files are present:

```powershell
Get-ChildItem .\replicateOldSnapshots.ps1, .\cohesity-api.ps1, .\Get_Replication_Date_Filters.ps1
```

Files:

```text
replicateOldSnapshots.ps1
cohesity-api.ps1
Get_Replication_Date_Filters.ps1   # Optional helper for date-filter calculation
```

---

## 3. How the Script Works

The script:

1. Connects to the source Cohesity cluster.
2. Confirms that the specified remote cluster is configured as a replication target.
3. Retrieves the specified protection group and its historical backup runs.
4. Applies `-olderThan` and `-newerThan` when age filtering is required.
5. Checks whether each snapshot has already replicated successfully.
6. Displays snapshots that are eligible for replication.
7. Calculates the replica retention.
8. Starts replication only when `-commit` is included.
9. Submits selected replication tasks in chronological order.

> **Always provide `-jobName "<PROTECTION_GROUP>"`. This limits processing to the required protection group instead of checking every protection group on the cluster.**

> **Without `-commit`, the script runs only in preview mode and does not create replication tasks.**

### Script Output

The script uses the following messages:

| Message | Meaning |
|---|---|
| `Would replicate <DATE> for <DAYS> days` | The snapshot is eligible and would be submitted if `-commit` were added |
| `Already replicated <DATE>` | The snapshot has already replicated successfully and will be skipped |
| `Replicating <DATE> for <DAYS> days` | The command was run with `-commit` and the snapshot is being prepared for replication |
| `Performing replications in time order...` | The script is submitting the selected replications chronologically |

### Already Replicated Snapshots

If a snapshot has already replicated successfully to the specified target, the script displays:

```text
Already replicated <SNAPSHOT_DATE_TIME>
```

The snapshot is **ignored and will not be replicated again**.

To process an already replicated snapshot again, use the complete parameter:

```powershell
-resync_WARNING_READ_THE_README_YOU_PROBABLY_DONT_WANT_TO_DO_THIS
```

Use resync only when:

- The previously replicated backup was **deleted from the target cluster** and must be recreated.
- The existing replica retention must be **intentionally extended**.

> **Do not use resync only because the script displays `Already replicated`.**

> **When resync is combined with `-keepFor`, the replica retention may be extended again. Repeated executions may continue increasing the expiration date.**

---

## 4. Main Example — Replicate All Eligible Snapshots

This example processes all eligible snapshots for one protection group without applying an age filter or custom retention.

Use the applicable CR number throughout the activity:

```text
<CR_NUMBER>
```

### Step 1: Preview

Run the script **without `-commit`** and provide the exact protection group name:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>"
```

### Sample Preview Output

```text
<PROTECTION_GROUP>

Would replicate 07/01/2026 22:00:00 for 28 days
Would replicate 07/02/2026 22:00:00 for 29 days
Already replicated 07/03/2026 22:00:00
Would replicate 07/04/2026 22:00:00 for 31 days
```

This means:

- The snapshots from 1, 2, and 4 July are eligible for replication.
- The snapshot from 3 July has already replicated and will be skipped.
- No replication task has been created because `-commit` was not supplied.

Review and confirm:

- The correct source and target clusters are used.
- The correct protection group is specified.
- The expected snapshots are shown as `Would replicate`.
- Previously replicated snapshots are shown as `Already replicated`.
- The calculated retention is correct.

Attach the preview output to:

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

### Sample Execution Output

```text
<PROTECTION_GROUP>

Replicating 07/01/2026 22:00:00 for 28 days
Replicating 07/02/2026 22:00:00 for 29 days
Already replicated 07/03/2026 22:00:00
Replicating 07/04/2026 22:00:00 for 31 days

Performing replications in time order...
    07/01/2026 22:00:00
    07/02/2026 22:00:00
    07/04/2026 22:00:00
```

This means:

- Eligible snapshots are shown as `Replicating`.
- Previously replicated snapshots are still shown as `Already replicated` and skipped.
- Only the selected snapshots are submitted in chronological order.

---

## 5. Important Date and Retention Notes

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

## 6. Validation

After execution, verify:

### Source Cluster

- Replication tasks were created.
- Replication tasks completed successfully.
- Only the specified protection group was processed.
- The submitted snapshot count matches the preview.

### Target Cluster

- The expected snapshots are visible.
- Snapshot dates match the original backups.
- Replica expiration dates are correct.
- The snapshots can be browsed or selected for recovery.

Record the result against:

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

<details>
<summary><strong>A. Replicate Snapshots Older Than 30 Days</strong></summary>

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

</details>

<details>
<summary><strong>B. Replicate Snapshots from the Last 30 Days</strong></summary>

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan 30
```

</details>

<details>
<summary><strong>C. Replicate Snapshots Between 30 and 60 Days Old</strong></summary>

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

</details>

<details>
<summary><strong>D. Generate Filters for Specific Backup Dates</strong></summary>

The replication script accepts relative age values through `-newerThan` and `-olderThan`; it does not accept calendar dates directly.

Use the helper stored in the same script directory:

```text
Get_Replication_Date_Filters.ps1
```

> **Run the helper on the actual day the replication will be performed. The calculated values change depending on the run date.**

Run:

```powershell
.\Get_Replication_Date_Filters.ps1
```

The helper provides four options:

```text
1. Single backup date
2. Continuous date range
3. Random dates - maximum 5
4. Dates from a text file - use for more than 5
```

### Example 1: Single Backup Date

Assuming the helper is run on **21 July 2026**:

```text
Select option 1-4: 1
Enter backup date (yyyy-MM-dd): 2026-07-01

============================================================
Single backup date
Run date:          21 July 2026
Backup date range: 01 July 2026 to 01 July 2026
Number of days:    1
Use: -newerThan 21 -olderThan 20
```

Use the generated values in the replication preview:

```powershell
.\replicateOldSnapshots.ps1 `
    -vip "<SOURCE_CLUSTER>" `
    -username "x_" `
    -domain "<DOMAIN>" `
    -replicateTo "<TARGET_CLUSTER>" `
    -jobName "<PROTECTION_GROUP>" `
    -newerThan 21 `
    -olderThan 20
```

### Example 2: Continuous 10-Day Range

```text
Select option 1-4: 2
Enter first backup date (yyyy-MM-dd): 2026-07-01
Enter last backup date (yyyy-MM-dd): 2026-07-10

============================================================
Continuous backup date range
Run date:          21 July 2026
Backup date range: 01 July 2026 to 10 July 2026
Number of days:    10
Use: -newerThan 21 -olderThan 11
```

Use:

```powershell
-newerThan 21 `
-olderThan 11
```

### Example 3: Random Dates

```text
Select option 1-4: 3
Enter up to 5 dates separated by commas (yyyy-MM-dd): 2026-07-01, 2026-07-05, 2026-07-12
```

Example output:

```text
============================================================
Random backup date
Backup date range: 01 July 2026 to 01 July 2026
Use: -newerThan 21 -olderThan 20

============================================================
Random backup date
Backup date range: 05 July 2026 to 05 July 2026
Use: -newerThan 17 -olderThan 16

============================================================
Random backup date
Backup date range: 12 July 2026 to 12 July 2026
Use: -newerThan 10 -olderThan 9
```

> **Random dates are non-contiguous. Run each generated filter pair separately so snapshots between the selected dates are not included.**

### Example 4: Dates from a File

For more than five non-contiguous dates, create a text file with one date per line:

```text
2026-07-01
2026-07-05
2026-07-12
2026-07-18
2026-07-19
2026-07-20
```

Run:

```text
Select option 1-4: 4
Enter text-file path: C:\Scripts\backup-dates.txt
```

The helper generates one `-newerThan` and `-olderThan` pair for each date.

> **Run each generated pair separately in preview mode. Confirm that the required snapshot is displayed as `Would replicate` before adding `-commit`.**

</details>

<details>
<summary><strong>E. Apply Custom Retention with `-keepFor`</strong></summary>

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

</details>

<details>
<summary><strong>F. Resync a Previously Replicated Snapshot</strong></summary>

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

</details>

<details>
<summary><strong>Optional: Sample Change Request</strong></summary>

### Change Title

Bulk replication of existing Cohesity snapshots from `<SOURCE_CLUSTER>` to `<TARGET_CLUSTER>`.

### CR Number

```text
<CR_NUMBER>
```

### Reason for Change

Historical snapshots for `<PROTECTION_GROUP>` need to be replicated to `<TARGET_CLUSTER>`. The script allows the required snapshots to be selected and replicated in bulk instead of initiating replication individually.

### Implementation Plan

1. Open PowerShell using **Run as different user** with the privileged username `x_`.
2. Navigate to `<SCRIPT_PATH>`.
3. Run the script **without `-commit`**.
4. Specify `-jobName "<PROTECTION_GROUP>"` to limit processing to the required protection group.
5. Review the `Would replicate` and `Already replicated` results.
6. Attach the preview output to `<CR_NUMBER>`.
7. Run the same command with `-commit`.
8. Confirm eligible snapshots display `Replicating`.
9. Monitor replication tasks until completion.
10. Validate the expected snapshots on the target cluster.

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
- Only snapshots shown as `Would replicate` during the preview will be submitted.
- Snapshots shown as `Already replicated` will be skipped.

### Validation Plan

- Confirm all submitted replication tasks complete successfully.
- Confirm the expected snapshots are visible on `<TARGET_CLUSTER>`.
- Confirm snapshot dates and expiration dates are correct.
- Add the replication result and validation evidence to `<CR_NUMBER>`.

### Backout Plan

Replication tasks already submitted cannot be reversed by the script.

If an incorrect replica is created:

1. Stop any replication tasks that are still running, where possible.
2. Do not rerun the script.
3. Review the replicated snapshot on the target cluster.
4. Remove the incorrect replica only after the required review.
5. Record the actions taken in `<CR_NUMBER>`.

### Risk

**Low to Medium**, depending on the number and size of snapshots selected.

The main risks are:

- Increased replication bandwidth.
- Additional target capacity usage.
- Incorrect snapshot selection.
- Incorrect retention selection.

These risks are reduced by running the script in **preview mode before using `-commit`**.

</details>
