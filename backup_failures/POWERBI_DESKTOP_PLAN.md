# Power BI Desktop Plan — Cohesity Backup Failures

## Scope

This report uses only the CSV files already generated inside the incident folders under:

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow
```

Included:

- Current unresolved backup failures
- Successfully recovered backup objects
- Failure ageing and recurring failures
- Environment and run-type breakdowns

Excluded:

- Snapshot history
- Cluster health scoring
- Running backups
- Cancelled backups
- Additional export files or schema changes

## Existing CSV sources

### Current failures

```text
<IncidentFolder>\current_failures.csv
```

Use for:

- Current failure detail
- Active-failure count
- Distinct affected objects
- Failures by environment
- Failures by run type
- Aged and recurring failures

### Successful recoveries

```text
<IncidentFolder>\cleared_by_success.csv
```

Use for:

- Successfully recovered object detail
- Recovered-object count
- Recoveries by environment
- Recoveries by run type
- Last failure versus latest successful backup

## Existing CSV columns

Both cleaned CSVs contain:

```text
IncidentNumber
Status
StatusChange
Cluster
ProtectionGroup
Environment
Host
ObjectName
ObjectType
RunType
FirstFailedET
LastFailedET
LatestSuccessET
LastSeenET
FailureDates
ConsecutiveFailureDays
Message
```

No new CSV columns are required for the initial report.

## Power BI Desktop import

### Query 1 — CurrentFailures

1. Select **Get Data → Folder**.
2. Choose:

   ```text
   X:\PowerShell\Data\Cohesity\BackupFailureWindow
   ```

3. Open the folder preview and continue to Power Query.
4. Filter `Name` to:

   ```text
   current_failures.csv
   ```

5. Exclude rows where `Folder Path` contains:

   ```text
   \Archive\
   ```

6. Sort `Date modified` descending.
7. Keep the top row only.
8. Click the `Binary` value in the `Content` column.
9. Use the first row as headers only if Power Query shows `Column1`, `Column2`, and similar names.
10. Rename the query:

    ```text
    CurrentFailures
    ```

### Query 2 — SuccessfulRecoveries

Repeat the same steps, but filter `Name` to:

```text
cleared_by_success.csv
```

Rename the query:

```text
SuccessfulRecoveries
```

## Data types

Set these columns to **Date/Time**:

```text
FirstFailedET
LastFailedET
LatestSuccessET
LastSeenET
```

Set this column to **Whole Number**:

```text
ConsecutiveFailureDays
```

Keep identifiers, statuses, names and messages as **Text**.

## Report page 1 — Current Failures

### Cards

- Active failures: row count from `CurrentFailures`
- Affected objects: distinct count of `ObjectName`
- Aged failures: count where `ConsecutiveFailureDays > 3`
- Re-failed objects: count where `Status = ReFailedAfterClear`

### Charts

- Failures by `Environment`
- Failures by `RunType`
- Failures by `ConsecutiveFailureDays`

### Detail table

Use:

```text
Status
StatusChange
Cluster
ProtectionGroup
Environment
Host
ObjectName
ObjectType
RunType
FirstFailedET
LastFailedET
FailureDates
ConsecutiveFailureDays
Message
```

### Slicers

- Cluster
- Environment
- RunType
- Status

## Report page 2 — Successful Recoveries

### Cards

- Recovered objects: row count from `SuccessfulRecoveries`
- Newly cleared: count where `Status = NewlyClearedThisCheck`
- Recovered environments: distinct count of `Environment`

### Charts

- Recoveries by `Environment`
- Recoveries by `RunType`
- Recoveries by `StatusChange`

### Detail table

Use:

```text
Status
StatusChange
Cluster
ProtectionGroup
Environment
Host
ObjectName
ObjectType
RunType
LastFailedET
LatestSuccessET
Message
```

### Slicers

- Cluster
- Environment
- RunType
- Status

## Refresh process

1. Run the Cohesity backup-failure PowerShell script manually.
2. Confirm the latest incident folder contains:

   ```text
   current_failures.csv
   cleared_by_success.csv
   ```

3. Open the PBIX file.
4. Select **Home → Refresh**.
5. Power Query will select the newest matching CSV from the incident folders.

## Validation

Before building visuals, verify:

- `CurrentFailures` points to the newest `current_failures.csv`.
- `SuccessfulRecoveries` points to the newest `cleared_by_success.csv`.
- `Archive` files are excluded.
- CSV headers are promoted correctly.
- Date/time and whole-number types are correct.
- Blank CSVs load without changing the schema.
