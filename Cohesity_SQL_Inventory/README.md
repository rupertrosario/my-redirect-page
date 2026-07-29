# Cohesity SQL Inventory

## Current Status

The full SQL database inventory is still under development.

Use **Test 01** first. It reads active SQL protection groups and displays three record types in one grid:

1. SQL configuration fields under `mssqlParams`
2. Protection-group alert settings under `alertPolicy`
3. Latest-run protected objects with both object ID and object name

The same grid rows are exported to CSV automatically.

## Files

| File | Status | Purpose |
|---|---|---|
| `Tests/Test-01-SQLProtectionGroups.ps1` | Current test | Display SQL settings, alert policy, and resolved object details in one grid |
| `Get-CohesitySQLInventory.ps1` | Experimental | Earlier full-inventory trial; do not use as the validated baseline |
| `README.md` | Current | Run instructions and output definition |

# Run Test 01

## 1. Open PowerShell

Open **Windows PowerShell 5.1** on the server or workstation where the Cohesity PowerShell folders and `X:` drive are available.

## 2. Go to the SQL inventory folder

```powershell
Set-Location "X:\PowerShell\Cohesity_API_Scripts\Cohesity_SQL_Inventory"
```

Use the actual local repository path when the repository is checked out somewhere else.

## 3. Run the test

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1
```

The default run:

- discovers clusters through Helios;
- checks clusters alphabetically;
- stops after the first cluster containing active SQL protection groups;
- inspects one SQL protection group;
- expands every leaf field under `mssqlParams`;
- expands every leaf field under `alertPolicy`;
- requests the latest protection-group run with object details;
- resolves each returned object ID to the object name returned by Cohesity;
- exports CSV;
- opens `Out-GridView` automatically.

No grid switch is required.

## Run Against a Specific Cluster

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "CHS-PROD-01"
```

Wildcard matching is supported:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "*PROD*"
```

## Inspect More Protection Groups

The default is one protection group.

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -MaxProtectionGroups 5
```

For a particular cluster:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "CHS-PROD-01" -MaxProtectionGroups 5
```

## Scan All Clusters

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ScanAllClusters
```

To inspect up to five SQL protection groups per cluster:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ScanAllClusters -MaxProtectionGroups 5
```

# Grid Output

`Out-GridView` opens automatically with these columns:

| Column | Meaning |
|---|---|
| `Cluster` | Cohesity cluster name |
| `ProtectionGroupName` | SQL protection-group name |
| `ProtectionGroupId` | Protection-group identifier |
| `Environment` | Expected value is `kSQL` |
| `PolicyId` | Assigned policy identifier |
| `RecordType` | `SQLSetting`, `AlertPolicy`, or `ProtectedObject` |
| `ParameterSection` | Configuration section containing the field |
| `Field` | Nested field path |
| `Value` | Actual value returned by the API |
| `ObjectId` | Cohesity object ID for protected-object rows |
| `ObjectName` | Object name returned for that ID |
| `SourceId` | Registered source ID |
| `SourceName` | Registered source name |
| `ObjectEnvironment` | Object environment returned by Cohesity |
| `ObjectType` | Cohesity object type when returned |
| `SnapshotStatus` | Latest object snapshot status when returned |

## Important SQL setting

The following field is included when returned by the API:

```text
RecordType       = SQLSetting
ParameterSection = fileProtectionTypeParams
Field            = advancedSettings.logChainBreakAutoTriggerOobIncrBackup
Value            = True or False
```

The same field can also appear under `nativeProtectionTypeParams` or `volumeProtectionTypeParams`, depending on `mssqlParams.protectionType`.

## Alert-policy rows

Examples:

```text
RecordType       = AlertPolicy
ParameterSection = alertPolicy
Field            = alertTargets[0].emailAddress
Value            = sql-alerts@example.com
```

```text
RecordType       = AlertPolicy
ParameterSection = alertPolicy
Field            = alertTargets[0].recipientType
Value            = kTo
```

```text
RecordType       = AlertPolicy
ParameterSection = alertPolicy
Field            = backupRunStatus[0]
Value            = kFailure
```

All other returned `alertPolicy` fields are also included.

## Protected-object rows

Object details are read from:

```text
GET /v2/data-protect/protection-groups/{id}/runs?numRuns=1&includeObjectDetails=true
```

Each protected-object row includes both:

```text
ObjectId
ObjectName
```

It also includes source name, source ID, object environment, object type, and latest snapshot status when Cohesity returns those fields.

A protected-object row represents the objects present in the latest returned protection-group run. It is not yet the final discovered SQL database inventory.

# CSV Output

The complete grid is exported automatically to:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests\Test-01-SQLProtectionGroupDetails_YYYYMMDD_HHMMSS.csv
```

When issues are detected, a separate issues CSV is written to:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests\Test-01-SQLProtectionGroupIssues_YYYYMMDD_HHMMSS.csv
```

# Console Summary

Before the grid opens, PowerShell displays:

```text
SQL protection-group detail test
Clusters checked: <count>
Protection groups inspected: <count>
Grid rows collected: <count>
Objects resolved to names: <count>
CSV: <path>
```

The script does not return a second PowerShell summary object.

# Authentication Requirements

The shared AES helper must exist at:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
```

The encrypted API-key file must exist at:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

The helper must provide:

```powershell
Get-CohesityApiKeyFromAes -EncryptedFile <path>
```

# API Safety

Test 01 uses only HTTP `GET` requests:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true
GET /v2/data-protect/protection-groups/{id}/runs?numRuns=1&includeObjectDetails=true
```

The target cluster is selected through the `accessClusterId` header.

No `POST`, `PUT`, `PATCH`, or `DELETE` request is used.

# Troubleshooting

## Out-GridView is not available

Run the test in Windows PowerShell on Windows and verify:

```powershell
Get-Command Out-GridView
```

## Object names are blank or `N/A`

The latest protection-group run did not return complete object details. Review the generated issues CSV.

The current test deliberately uses the latest run because it is a small validation step. A later inventory stage will resolve the complete current SQL object hierarchy separately.

## Too much data in the grid

Keep the default of one protection group or specify one cluster:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "<cluster name>" -MaxProtectionGroups 1
```
