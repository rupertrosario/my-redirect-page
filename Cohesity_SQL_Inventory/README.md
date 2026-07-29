# Cohesity SQL Inventory

## Current test

Use:

```text
Tests/Test-02-SQLDatabaseInventory.ps1
```

This test produces **one row per SQL database**. It does not use the raw field/value layout from Test 01.

The helper file must remain in the same folder:

```text
Tests/SQLInventory.Helpers.ps1
```

`Test-01-SQLProtectionGroups.ps1` remains available as the earlier raw API-inspection test.

## Run instructions

Open Windows PowerShell 5.1 and go to the SQL inventory folder:

```powershell
Set-Location "X:\PowerShell\Cohesity_API_Scripts\Cohesity_SQL_Inventory"
```

Run the default test:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1
```

The default run:

- checks clusters alphabetically;
- stops after the first cluster containing an active SQL protection group;
- inspects one protection group;
- reads the latest 20 protection-group runs;
- opens one `Out-GridView`;
- exports the same rows to CSV.

## Select a cluster and protection group

Specific cluster:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -ClusterName "CHS-PROD-01"
```

Specific protection group:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -ClusterName "CHS-PROD-01" -ProtectionGroupName "SQL-PROD-AAG"
```

Wildcards are supported:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -ClusterName "*PROD*" -ProtectionGroupName "*SQL*"
```

Inspect five protection groups on the first matching cluster:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -MaxProtectionGroups 5
```

Scan all clusters and up to five protection groups per cluster:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -ScanAllClusters -MaxProtectionGroups 5
```

Read more run history:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -RunHistoryCount 50
```

Include database objects that appeared in older returned runs but not in the latest run:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -IncludeHistoricalObjects
```

Save raw PG, policy, and run JSON for troubleshooting:

```powershell
.\Tests\Test-02-SQLDatabaseInventory.ps1 -SaveRawJson
```

## Row design

The row identity is:

```text
Cluster + ServerInstance + Database + ProtectionGroup
```

Each SQL database appears once for that source and protection group.

Examples:

| Cluster | ServerInstance | Database | ProtectionGroup |
|---|---|---|---|
| CHS-PROD-01 | SQLPRD01\MSSQLSERVER | FinanceDB | SQL-PROD |
| CHS-PROD-01 | SQLPRD01\INSTANCE1 | FinanceDB | SQL-PROD |

Those are two different database objects because they belong to different SQL instances.

## Main grid columns

### Database identity

- `Cluster`
- `ServerInstance`
- `Database`
- `AvailabilityGroup`
- `ReplicaRole`
- `ProtectionGroup`
- `PolicyName`

### Protection-group SQL settings

- `ProtectionType`
- `AAGBackupPreference`
- `BackupSystemDBs`
- `UserDBPreference`
- `LogChainBreakOOBIncremental`
- `AlertEmails`
- `AlertOnStatuses`

Alert target language and recipient type are not displayed.

The following unwanted raw fields are not displayed as independent columns:

- object ID;
- source ID;
- source type;
- object type;
- regular-expression indicator;
- `logBackupWithClause`;
- `newDatabaseAutoTriggerOobIncrBackup`.

## Exclusions

`ObjectExclusions` contains filters that match the current database object.

`OtherObjectExclusions` preserves configured exclusions that did not match any currently displayed database. To avoid repeating the same stale or unmatched exclusion on every database row, this value appears only on the first database row for that protection group.

The script uses the regular-expression flag internally for matching but does not display it.

## Backup success columns

The script reads multiple runs and adds:

- `LatestBackupType`
- `LatestBackupStatus`
- `LatestBackupStart`
- `LatestBackupEnd`
- `LatestFailureMessage`
- `LastFullStatus`
- `LastFullTime`
- `LastIncrementalStatus`
- `LastIncrementalTime`
- `LastLogStatus`
- `LastLogTime`
- `SeenInLatestRun`

Run types containing `regular` or `increment` are classified as incremental. Run types containing `full` or `log` are classified accordingly.

## Source, object, and policy details

The following columns retain additional returned parameters without creating extra database rows:

- `SourceSettings`
- `ObjectParameters`
- `PolicySettings`
- `RawObjectName`

The collector checks returned SQL, AAG, host, database, source, connection, schedule, backup, retention, and policy sections. Password, token, secret, credential, and API-key fields are excluded.

The source/object parameter paths are intentionally retained as `path=value` text during validation because Cohesity versions may return different nested SQL structures.

## API calls

Test 02 uses only `GET` requests:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true
GET /v2/data-protect/policies?ids={policyId}
GET /v2/data-protect/protection-groups/{pgId}/runs?numRuns={count}&includeObjectDetails=true
GET /v2/data-protect/objects/{objectId}
```

The target cluster is selected with the `accessClusterId` header.

No `POST`, `PUT`, `PATCH`, or `DELETE` request is used.

## Output files

Main CSV:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests\Test-02-SQLDatabaseInventory_YYYYMMDD_HHMMSS.csv
```

Issues CSV, when required:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests\Test-02-SQLDatabaseInventoryIssues_YYYYMMDD_HHMMSS.csv
```

Raw JSON files are created only when `-SaveRawJson` is used.

## Authentication

The script expects:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

The helper must provide:

```powershell
Get-CohesityApiKeyFromAes -EncryptedFile <path>
```

## Validation note

This test still requires execution in the user environment. A PowerShell runtime is not available in the repository-editing environment, so API response shape and parser behavior must be confirmed using the generated grid, issues CSV, and optional raw JSON files.
