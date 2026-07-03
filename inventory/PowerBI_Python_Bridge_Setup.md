# Power BI Python bridge setup — Physical PG Inventory

This is the optional advanced setup where Power BI Desktop Refresh runs the PowerShell collector first.

## Recommended design

Use Power BI slicers for cluster filtering.

Do not run the collector per cluster from Power BI unless you are testing.

Recommended flow:

```text
Power BI Refresh
  -> Python bridge runs
  -> Python calls PowerShell headless wrapper
  -> PowerShell collects ALL clusters using GET-only calls
  -> Latest CSVs are overwritten
  -> Python reads the CSVs into pandas DataFrames
  -> Power BI loads the DataFrames
  -> Power BI slicers filter Cluster / PG / Policy / Status
```

Keep this setting in `PowerBI_PhysicalPGInventory_Bridge.py` for normal use:

```python
CLUSTER_SELECTION = "0"
```

`0` means all clusters. Power BI should handle filtering after the data is loaded.

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

This relationship is what allows a selected PG or cluster in the summary table to filter the object detail table.

## Cluster filter / slicer setup

Add a Slicer visual using:

```text
Physical_PG_Summary[Cluster]
```

This is the main cluster filter.

When one cluster is selected:

```text
Cluster slicer
  -> filters Physical_PG_Summary
  -> relationship filters Physical_PG_Object_Detail through PGKey
```

Do not use a separate Python run per cluster for normal usage. It is slower and makes the model harder to manage.

## Recommended slicers

Add these slicers on the report page:

```text
Cluster
PolicyName
ProtectionType
LastRunStatus
IsPaused
```

Optional slicers:

```text
PGName
ObjectName
LastSuccessfulBackupStatus
```

Recommended source fields:

```text
Cluster                       = Physical_PG_Summary[Cluster]
PolicyName                    = Physical_PG_Summary[PolicyName]
ProtectionType                = Physical_PG_Summary[ProtectionType]
LastRunStatus                 = Physical_PG_Summary[LastRunStatus]
IsPaused                      = Physical_PG_Summary[IsPaused]
PGName                        = Physical_PG_Summary[PGName]
ObjectName                    = Physical_PG_Object_Detail[ObjectName]
LastSuccessfulBackupStatus    = Physical_PG_Object_Detail[LastSuccessfulBackupStatus]
```

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

## Testing with one cluster only

Use a single cluster only for testing if all clusters takes too long.

Example:

```powershell
.\Invoke-PhysicalPGInventoryHeadless.ps1 -ClusterSelection 1
```

Then set this in `PowerBI_PhysicalPGInventory_Bridge.py`:

```python
CLUSTER_SELECTION = "1"
```

After stable testing, switch back to all clusters:

```python
CLUSTER_SELECTION = "0"
```

For the final Power BI report, the intended setup is all-cluster collection plus Cluster slicer filtering inside Power BI.
