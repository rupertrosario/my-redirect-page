# Power BI Python bridge setup — Physical PG Inventory

This is the optional advanced setup where Power BI Desktop Refresh runs the PowerShell collector first.

## What this gives you

Power BI Desktop can run a Python script as a data source.

The Python script can call PowerShell, wait for the CSV files to be updated, and then load the CSV files into Power BI.

Flow:

```text
Power BI Refresh
  -> Python data source runs
  -> Python calls Invoke-PhysicalPGInventoryHeadless.ps1
  -> PowerShell collects Cohesity data using GET-only calls
  -> Latest CSVs are overwritten
  -> Python reads the CSVs into pandas DataFrames
  -> Power BI loads the DataFrames
```

## Files involved

```text
inventory\Get-PhysicalPGInventory.ps1
inventory\Invoke-PhysicalPGInventoryHeadless.ps1
inventory\PowerBI_PhysicalPGInventory_Bridge.py
```

The bridge reads/writes these data files:

```text
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Summary_Latest.csv
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Object_Detail_Latest.csv
```

## Prerequisites

On the machine running Power BI Desktop:

```text
Python installed
pandas installed
Power BI Desktop configured to use that Python installation
PowerShell available as powershell.exe
Access to X:\PowerShell\Cohesity_API_Scripts\inventory
Access to the Cohesity API key file
```

Install pandas if needed:

```powershell
python -m pip install pandas
```

In Power BI Desktop:

```text
File > Options and settings > Options > Python scripting
```

Select the Python installation that has pandas.

## Test the headless wrapper first

Before using Power BI, run this from PowerShell:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\inventory
.\Invoke-PhysicalPGInventoryHeadless.ps1 -ClusterSelection 0
```

Expected result:

```text
Physical_PG_Summary_Latest.csv created/updated
Physical_PG_Object_Detail_Latest.csv created/updated
No Out-GridView opens
No manual cluster prompt appears
```

`ClusterSelection 0` means all clusters.

## Test the Python bridge outside Power BI

Run:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\inventory
python .\PowerBI_PhysicalPGInventory_Bridge.py
```

If it exits without error, Power BI should be able to use it.

## Add the Python bridge to Power BI Desktop

In Power BI Desktop:

```text
Home > Get data > More > Other > Python script
```

Paste this small loader:

```python
exec(open(r"X:\PowerShell\Cohesity_API_Scripts\inventory\PowerBI_PhysicalPGInventory_Bridge.py", encoding="utf-8").read())
```

Power BI Navigator should show two DataFrames:

```text
Physical_PG_Summary
Physical_PG_Object_Detail
```

Load both.

## Create the model relationship

Create this relationship:

```text
Physical_PG_Summary[PGKey]
        1 -> *
Physical_PG_Object_Detail[PGKey]
```

Use single-direction filtering from summary to detail.

## Report layout

### PG Summary table

Use:

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

### Object Detail table

Use:

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

Click a PG in the summary table. The object detail table should filter automatically.

## Important behavior

Every Power BI Refresh can run the Cohesity collector again.

That means:

```text
Refresh = API calls to Helios/clusters + CSV overwrite + Power BI reload
```

Avoid repeatedly clicking Refresh unless you intentionally want fresh data.

## Desktop vs Service

This bridge is intended for Power BI Desktop.

Do not assume it will work in Power BI Service scheduled refresh. Running local Python that calls local PowerShell and reads an X: drive is generally a local desktop workflow, not a clean service refresh workflow.

## If refresh hangs

Most likely causes:

```text
PowerShell waiting for input
Python path not configured in Power BI
pandas not installed
X: drive unavailable to Power BI Desktop process
Cohesity API call is slow
Network/API timeout
```

The headless wrapper prevents the cluster prompt by overriding Read-Host for the run.

## Recommended first test

Use a single cluster first if all clusters takes too long.

Example:

```powershell
.\Invoke-PhysicalPGInventoryHeadless.ps1 -ClusterSelection 1
```

Then set this in `PowerBI_PhysicalPGInventory_Bridge.py`:

```python
CLUSTER_SELECTION = "1"
```

After stable testing, switch back to:

```python
CLUSTER_SELECTION = "0"
```
