# Cohesity SQL Inventory

## Current Status

The full SQL database inventory is still under development.

Use **Test 01** first. It reads active SQL protection groups, expands every field returned under `mssqlParams`, opens the results in `Out-GridView`, and exports the same rows to CSV.

## Files

| File | Status | Purpose |
|---|---|---|
| `Tests/Test-01-SQLProtectionGroups.ps1` | Current test | Collect every field under `mssqlParams`, display the grid, and export CSV |
| `Get-CohesitySQLInventory.ps1` | Experimental | Earlier full-inventory trial; do not use as the validated baseline |
| `README.md` | Current | Run instructions and output definition |

# Run Test 01

## 1. Open PowerShell

Open **Windows PowerShell 5.1** on the server or workstation where the Cohesity PowerShell folders and `X:` drive are available.

## 2. Go to the SQL inventory folder

```powershell
Set-Location "X:\PowerShell\Cohesity_API_Scripts\Cohesity_SQL_Inventory"
```

Use the actual local repository path if the GitHub repository is checked out somewhere else.

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
- creates a CSV;
- opens the result automatically in `Out-GridView`.

No grid switch is required.

## Run Against a Specific Cluster

Use the Cohesity cluster name:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "CHS-PROD-01"
```

Wildcard matching is supported:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "*PROD*"
```

## Inspect More Protection Groups

The default is one protection group.

To inspect five protection groups from the selected cluster:

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

This can generate a large grid and CSV when many clusters and SQL protection groups exist.

# Output

## Grid

`Out-GridView` opens automatically with these columns:

| Column | Meaning |
|---|---|
| `Cluster` | Cohesity cluster name |
| `ProtectionGroupName` | SQL protection group name |
| `ProtectionGroupId` | Protection group identifier |
| `Environment` | Expected value is `kSQL` |
| `PolicyId` | Assigned policy identifier |
| `ParameterSection` | First section below `mssqlParams`, such as `fileProtectionTypeParams` |
| `Field` | Complete nested field path below that section |
| `Value` | Actual value returned by the API |

Examples of field paths:

```text
advancedSettings.missingDbBackupStatus
advancedSettings.newDatabaseAutoTriggerOobIncrBackup
excludeFilters[0].filterString
prePostScript.preScript.isActive
prePostScript.postScript.timeoutSecs
```

Array indexes are preserved so separate values are not collapsed into one field.

## CSV

The same grid rows are exported automatically to:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests\Test-01-SQLProtectionGroupFields_YYYYMMDD_HHMMSS.csv
```

When issues are detected, a separate issues CSV is written to:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests\Test-01-SQLProtectionGroupIssues_YYYYMMDD_HHMMSS.csv
```

## Console Summary

Before the grid opens, PowerShell displays:

```text
SQL protection-group field test
Clusters checked: <count>
Protection groups inspected: <count>
Fields collected: <count>
CSV: <path>
```

The script does not return a second PowerShell summary object, so the duplicate summary output has been removed.

# SQL Fields Collected

The test recursively collects everything returned below:

```text
mssqlParams
```

This includes the active protection type and all returned fields under sections such as:

```text
protectionType
fileProtectionTypeParams
nativeProtectionTypeParams
volumeProtectionTypeParams
```

Typical fields include:

- `aagBackupPreferenceType`
- `backupSystemDbs`
- `fullBackupsCopyOnly`
- `logBackupNumStreams`
- `logBackupWithClause`
- `supportFilestreamDbs`
- `useAagPreferencesFromServer`
- `userDbBackupPreferenceType`
- all returned `advancedSettings` fields
- all returned exclusion filters
- all returned pre-script and post-script settings

The collector does not hard-code only these fields. It recursively exports every leaf value that the API returns under `mssqlParams`.

# Authentication Requirements

The script expects the shared AES helper at:

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
```

The target cluster is selected through the `accessClusterId` header.

No `POST`, `PUT`, `PATCH`, or `DELETE` request is used.

# Troubleshooting

## Out-GridView is not available

Test 01 requires `Out-GridView`. Run it in Windows PowerShell on a Windows system where the cmdlet is installed.

Check availability:

```powershell
Get-Command Out-GridView
```

## No SQL protection-group fields were collected

Run against a known SQL cluster:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "<known SQL cluster>"
```

Then review any issues CSV created in:

```text
X:\PowerShell\Data\Cohesity\SQLInventory\Tests
```

## Too much data in the grid

Keep the default of one protection group, or specify one cluster:

```powershell
.\Tests\Test-01-SQLProtectionGroups.ps1 -ClusterName "<cluster name>" -MaxProtectionGroups 1
```

# Next Stage

After Test 01 confirms the actual SQL protection-group configuration, the next small test will inspect database and object details separately. The full SQL inventory will not be rebuilt until those API response structures are validated.