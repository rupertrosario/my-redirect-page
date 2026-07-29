# Cohesity SQL Database Inventory

## Purpose

Collect a read-only SQL Server inventory from every Cohesity cluster visible through Helios and correlate discovered SQL databases with active protection groups and policies.

This is a **trial implementation** intended to validate the SQL source and object response structure returned by the Cohesity APIs in the target environment.

## Files

| File | Purpose |
|---|---|
| `Get-CohesitySQLInventory.ps1` | Multi-cluster PowerShell SQL inventory collector |
| `README.md` | Usage, output definition, API flow, and validation notes |

## Scope

The script attempts to inventory:

- SQL sources and hosts
- SQL instances
- Standalone SQL databases
- Availability Groups
- Availability Group databases
- SQL version
- Database recovery model
- Database state
- Database size
- TDE status
- Availability Group and replica-role information
- Active protection group
- Assigned policy
- Log-backup configuration
- Latest protection-run status and time
- Protected or unprotected status
- Collection and matching findings

## Read-Only API Flow

The collector uses only HTTP `GET` requests.

### 1. Discover Helios clusters

```text
GET /v2/mcm/cluster-mgmt/info
```

### 2. Discover registered SQL sources for each cluster

The request is scoped to the target cluster with:

```text
accessClusterId: <clusterId>
```

### 3. Traverse SQL source objects

```text
GET /v2/data-protect/sources/{sourceId}/objects
```

The trial requests these MSSQL object types:

```text
kRootContainer
kInstance
kDatabase
kAAGRootContainer
kAAG
kAAGDatabase
```

The script follows parent-child relationships until all reachable SQL objects have been processed.

### 4. Read active SQL protection groups

```text
GET /v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true&includeLastRunInfo=true
```

### 5. Resolve protection policies

```text
GET /v2/data-protect/policies?ids=<policyId>
```

No `POST`, `PUT`, `PATCH`, or `DELETE` request is used.

## Authentication

The script uses the shared AES API-key helper:

```powershell
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
```

The helper must expose:

```powershell
Get-CohesityApiKeyFromAes -EncryptedFile <path>
```

## Run Commands

### Standard console output

```powershell
.\Get-CohesitySQLInventory.ps1
```

### Interactive grid

```powershell
.\Get-CohesitySQLInventory.ps1 -GridView
```

### Save raw SQL object responses for troubleshooting

```powershell
.\Get-CohesitySQLInventory.ps1 -DumpRawJson
```

Raw JSON is written to:

```text
X:\PowerShell\Data\Cohesity\SQLInventory
```

The normal run does not create a CSV.

## Summary Output

The summary is displayed first.

| Metric | Meaning |
|---|---|
| Clusters discovered | Number of clusters returned by Helios |
| Clusters successfully queried | Clusters where the SQL inventory workflow completed |
| SQL sources | Registered SQL sources found across all clusters |
| SQL instances | SQL instance objects discovered |
| Database rows | Final SQL database or AAG-database inventory rows |
| Protected databases | Rows matched to active Cohesity protection |
| Unprotected databases | Rows not matched to active protection |
| Availability Group databases | Rows identified as `kAAGDatabase` |
| Standalone databases | Rows identified as `kDatabase` |
| Inventory issues | Cluster or API processing issues recorded separately |

## Database Inventory Output

| Column | Meaning |
|---|---|
| `Cluster` | Cohesity cluster name |
| `HostName` | SQL host associated with the database object |
| `InstanceName` | SQL Server instance name |
| `DatabaseName` | Database name |
| `ObjectType` | Cohesity SQL object type, such as `kDatabase` or `kAAGDatabase` |
| `SQLVersion` | SQL Server version returned by Cohesity |
| `RecoveryModel` | Database recovery model when exposed by the API |
| `DBState` | Database state when exposed by the API |
| `SizeGB` | Database logical size converted to GiB |
| `TDEEnabled` | Transparent Data Encryption status when exposed |
| `AvailabilityGroup` | Availability Group name when applicable |
| `ReplicaRole` | Primary, secondary, or API-returned replica role |
| `ProtectionGroup` | Active Cohesity protection group matched to the database |
| `PolicyName` | Assigned Cohesity policy |
| `LogBackup` | SQL log-backup setting derived from the protection group |
| `LastRunStatus` | Latest available protection-run status |
| `LastRunEndTime` | Latest available run completion time |
| `ProtectionStatus` | `Protected` or `Unprotected` |
| `Findings` | Missing data, unmatched protection, or collection notes |

## Sample Output

### Summary

```text
Metric                              Count
------                              -----
Clusters discovered                   23
Clusters successfully queried         23
SQL sources                            42
SQL instances                          67
Database rows                         428
Protected databases                   401
Unprotected databases                  27
Availability Group databases          118
Standalone databases                  310
Inventory issues                        0
```

### SQL database inventory

```text
Cluster      HostName            InstanceName  DatabaseName  ObjectType   SQLVersion       RecoveryModel  DBState  SizeGB  AvailabilityGroup  ReplicaRole  ProtectionGroup  PolicyName  LogBackup  LastRunStatus        ProtectionStatus  Findings
-------      --------            ------------  ------------  ----------   ----------       -------------  -------  ------  -----------------  -----------  ---------------  ----------  ---------  -------------        ----------------  --------
CHS-PROD-01  sqlprd01.company    MSSQLSERVER   FinanceDB     kDatabase    SQL Server 2022  FULL           ONLINE   842.60  N/A                N/A          SQL-PROD-AllDB   Gold-SQL    Enabled    Succeeded            Protected         None
CHS-PROD-01  sqlag01.company     MSSQLSERVER   OrdersDB      kAAGDatabase SQL Server 2019  FULL           ONLINE  1280.30  PROD-AAG-01        Primary      SQL-AAG-Prod     Gold-SQL    Enabled    SucceededWithWarning Protected         Latest run completed with warning
CHS-TEST-01  sqltest02.company   TEST01        SandboxDB     kDatabase    SQL Server 2019  SIMPLE         ONLINE    74.20  N/A                N/A          N/A              N/A         Disabled   N/A                  Unprotected       No active protection group match
```

The values above are illustrative and are not taken from a live cluster.

## Trial Limitations

- Cohesity SQL object payloads can differ by software version and registration method.
- Some fields may appear under different nested properties or may not be returned.
- The first execution should be treated as an API-shape validation run.
- `-DumpRawJson` should be used when instance, database, AAG, policy, or latest-run fields are missing.
- Protection matching should prefer Cohesity object identifiers. Name-based fallback matching can be ambiguous when identical database names exist on multiple instances.
- One Availability Group database may legitimately appear once per replica, depending on the Cohesity object tree.
- `Unprotected` means no active protection-group match was found by the collector; it does not by itself prove that no other protection mechanism exists.
- No CSV export and no Dynatrace JavaScript version are included in this trial.

## Validation Checklist

Confirm the following after the first run:

1. `Clusters discovered` matches the Helios inventory.
2. SQL source and instance counts are reasonable for the environment.
3. Known standalone databases appear as `kDatabase`.
4. Known AAG databases appear as `kAAGDatabase` with the correct Availability Group.
5. Primary and secondary replicas are not incorrectly collapsed into one row.
6. Protection groups and policies are associated with the correct instance and database.
7. Known protected databases show `Protected`.
8. Known unprotected databases show `Unprotected`.
9. Missing fields are investigated with `-DumpRawJson` rather than assumed.
10. Network activity contains only GET requests.
