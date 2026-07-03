# Power BI dashboard baseline - Physical PG Inventory

This document tracks the working Power BI baseline for the Cohesity Physical PG Inventory report.

## Data flow

```text
Get-PhysicalPGInventory.ps1
  -> Physical_PG_Summary_Latest.csv
  -> Physical_PG_Object_Detail_Latest.csv
  -> Power BI Desktop report
```

## Local files

```text
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Summary_Latest.csv
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Object_Detail_Latest.csv
X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Inventory.pbix
```

The `.pbix` file is local unless manually committed or uploaded.

## Power BI tables

Load these two CSV files using `Home -> Get data -> Text/CSV`:

```text
Physical_PG_Summary_Latest
Physical_PG_Object_Detail_Latest
```

## Relationship

Power BI relationship:

```text
Physical_PG_Object_Detail_Latest[PGKey]   many (*)
    ->
Physical_PG_Summary_Latest[PGKey]         one (1)
```

Settings:

```text
Cardinality: Many to one (*:1)
Cross filter direction: Single
Active: Yes
```

This is equivalent to:

```text
One PG summary row -> many object detail rows
```

## Dashboard layout

```text
Title: COHESITY PHYSICAL PG INVENTORY

Cluster slicer

KPI cards:
[Total PGs] [Total Objects] [Paused PGs] [Problem PGs]

Middle charts:
[PGs by LastRunStatus] [PGs by ProtectionType]

Tables:
[PG Summary]
[Object Detail]
```

## Existing slicer

Keep only this slicer for the baseline:

```text
Physical_PG_Summary_Latest[Cluster]
```

## PG Summary table fields

```text
Cluster
PGName
PolicyName
ProtectionType
PGObjectCount
LastRunStatus
LastRunEndET
```

## Object Detail table fields

```text
ObjectName
IncludedPath
ExcludedPathsUnderIncludedPath
GlobalExcludePaths
ObjectExcludedVssWriters
JobExcludedVssWriters
```

## DAX measures

### Total PGs

```DAX
Total PGs = DISTINCTCOUNT('Physical_PG_Summary_Latest'[PGKey])
```

### Total Objects

```DAX
Total Objects = COUNTROWS('Physical_PG_Object_Detail_Latest')
```

### Paused PGs

Working version when `IsPaused` is imported as TRUE/FALSE boolean in Power BI:

```DAX
Paused PGs =
CALCULATE(
    DISTINCTCOUNT('Physical_PG_Summary_Latest'[PGKey]),
    'Physical_PG_Summary_Latest'[IsPaused] = TRUE()
)
```

### Problem PGs

This measure treats only pure success as healthy. Success with warning is counted as a problem.

```DAX
Problem PGs =
CALCULATE(
    DISTINCTCOUNT('Physical_PG_Summary_Latest'[PGKey]),
    FILTER(
        'Physical_PG_Summary_Latest',
        NOT (
            LOWER(TRIM('Physical_PG_Summary_Latest'[LastRunStatus])) IN {
                "ksuccess",
                "success",
                "succeeded"
            }
        )
    )
)
```

Counted as problem examples:

```text
SucceededWithWarning
kSucceededWithWarning
kWarning
kFailed
Failed
Canceled
Error
```

Excluded from problem count:

```text
kSuccess
Success
Succeeded
```

## Refresh workflow

After running all clusters from PowerShell:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\inventory
.\Get-PhysicalPGInventory.ps1
```

Select:

```text
0
```

Then in Power BI Desktop:

```text
Home -> Refresh
```

The Cluster slicer, cards, charts, PG Summary table, and Object Detail table should update from the latest CSV files.
