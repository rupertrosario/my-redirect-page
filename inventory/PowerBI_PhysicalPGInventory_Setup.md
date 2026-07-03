# Power BI setup — Cohesity Physical PG Inventory

This guide explains how to use Power BI Desktop as the UI for `Get-PhysicalPGInventory.ps1`.

## Current design

The PowerShell script is the data collector.

The script exports these fixed CSV files for Power BI:

```text
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Summary_Latest.csv
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Object_Detail_Latest.csv
```

It also keeps timestamped history CSV files:

```text
Physical_PG_Summary_<timestamp>.csv
Physical_PG_Object_Detail_<timestamp>.csv
```

## Power BI report file path

Create and save the Power BI report here:

```text
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Inventory.pbix
```

## Create the Power BI report

Open Power BI Desktop.

Go to:

```text
Home > Get data > Text/CSV
```

Load this file first:

```text
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Summary_Latest.csv
```

Then load this file:

```text
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Object_Detail_Latest.csv
```

## Create the relationship

Both files contain this column:

```text
PGKey
```

The value is:

```text
Cluster|PGName
```

In Power BI Model view, create this relationship:

```text
Physical_PG_Summary_Latest[PGKey]
        1 -> *
Physical_PG_Object_Detail_Latest[PGKey]
```

Use single-direction filtering from summary to detail.

## Suggested report layout

### PG Summary visual

Use a table visual with:

```text
PGIndex
Cluster
PGName
PolicyName
ProtectionType
PGObjectCount
GlobalExcludePaths
JobExcludedVssWriters
IsPaused
LastRunStatus
LastRunStartET
LastRunEndET
LastRunMessage
```

### Object Detail visual

Use a table visual with:

```text
ObjectName
LastSuccessfulBackupStatus
LastSuccessfulBackupEndET
ObjectIncludedPaths
ObjectExcludedPathsAll
IncludedPath
ExcludedPathsUnderIncludedPath
SkipNestedVolumes
GlobalExcludePaths
ObjectExcludedVssWriters
JobExcludedVssWriters
```

When you click one PG in the summary visual, the object detail visual should filter automatically.

## Normal usage

Use this flow:

```text
1. Run Get-PhysicalPGInventory.ps1
2. Script refreshes the Latest CSV files
3. Open Physical_PG_Inventory.pbix
4. Click Refresh in Power BI Desktop
5. Use Power BI as the UI
```

## Launcher script

A helper launcher is included:

```text
inventory\Run-PhysicalPGInventoryPowerBI.ps1
```

It does this:

```text
1. Runs Get-PhysicalPGInventory.ps1
2. Suppresses Out-GridView during that run
3. Opens Physical_PG_Inventory.pbix if the file exists
```

Run it from PowerShell:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\inventory
.\Run-PhysicalPGInventoryPowerBI.ps1
```

If the PBIX does not exist yet, the launcher will only warn and exit after creating the CSV files.

## Can Power BI Refresh run the PowerShell script?

Not directly through a native Power BI CSV refresh.

Power BI Refresh normally reloads data from its connected source files. With the CSV design, Refresh rereads:

```text
Physical_PG_Summary_Latest.csv
Physical_PG_Object_Detail_Latest.csv
```

It does not automatically execute `Get-PhysicalPGInventory.ps1`.

## Workable options for auto-running the collector

### Option 1 — Recommended

Run the PowerShell script first, then refresh Power BI.

This is the cleanest and safest approach.

### Option 2 — Launcher script

Use:

```text
Run-PhysicalPGInventoryPowerBI.ps1
```

This runs the collector and opens the PBIX.

You still click Refresh in Power BI Desktop after it opens.

### Option 3 — Python bridge inside Power BI Desktop

Power BI Desktop can run Python scripts as a data source. A Python script can call PowerShell, wait for the CSVs to be created, and then return a pandas dataframe to Power BI.

This is possible, but it has extra prerequisites:

```text
Python installed locally
pandas installed
Power BI Desktop configured to use that Python install
```

Also, this should be used carefully because every Power BI refresh can trigger the collector again.

For this Cohesity inventory, avoid the Python bridge until the normal CSV + PBIX workflow is stable.

## Recommended first build

Start with:

```text
Get-PhysicalPGInventory.ps1 -> Latest CSV files -> Power BI Desktop PBIX
```

After the PBIX works properly, decide whether the Python bridge is worth adding.
