# Cohesity Protection Dashboard Data Contract

This document defines the stable data contract for the Cohesity Protection Power BI dashboard and the future Claude Code enhancement workflow.

## Current implementation status

Baseline environments in scope now:

```text
Physical
Hyper-V
Nutanix AHV
```

Cohesity API environment mapping:

```text
Physical     -> kPhysical
Hyper-V      -> kHyperV
Nutanix AHV  -> kAcropolis
```

The current Physical-only proof of concept remains available:

```text
Get-PhysicalPGInventory.ps1
Physical_PG_Summary_Latest.csv
Physical_PG_Object_Detail_Latest.csv
```

The generic framework collector is:

```text
Get-CohesityProtectionInventory.ps1
```

## Architecture

```text
PowerShell = data collection
CSV/JSON   = stable data contract
Power BI   = dashboard/reporting layer
Claude Code = future insight/refactor/enhancement engine
```

The dashboard must not be built around one giant raw table. It should use summary tables, exception tables, and controlled drilldown.

## Output files

The generic collector writes these files to:

```text
X:\PowerShell\Cohesity_API_Scripts\inventory
```

Required files:

```text
Cohesity_Protection_PG_Summary_Latest.csv
Cohesity_Protection_Object_Detail_Latest.csv
Cohesity_Protection_Path_Detail_Latest.csv
Cohesity_Protection_Exceptions_Latest.csv
Cohesity_Protection_Run_Metadata.json
```

## PG Summary

Power BI role:

```text
Primary fact table for executive and operations KPIs.
```

Columns:

```text
PGKey
InventoryDateET
Cluster
ClusterId
Environment
ProtectionGroup
ProtectionGroupId
PolicyName
PolicyId
IsActive
IsDeleted
ObjectCount
GlobalExcludePathCount
ObjectExcludePathCount
HasGlobalExclusions
HasObjectExclusions
LastSuccessfulBackupET
LastSuccessfulBackupStatus
LastSuccessfulBackupAgeHours
LastRunStatus
LastRunType
IsPaused
StorageDomain
SourceName
```

Notes:

```text
PolicyName must be resolved to a readable policy name when the policy API exposes it.
PolicyId is retained separately for troubleshooting.
PGKey should prefer ClusterId|ProtectionGroupId.
Environment should use friendly names, not Cohesity API enum values.
```

## Object Detail

Power BI role:

```text
Controlled drilldown table. Not the main executive reporting table.
```

Columns:

```text
ObjectKey
PGKey
InventoryDateET
Cluster
ClusterId
Environment
ProtectionGroup
ProtectionGroupId
PolicyName
HostName
ObjectName
ObjectType
ObjectId
ParentSource
IncludedPathCount
ObjectExcludePathCount
HasGlobalExclusions
HasObjectExclusions
LastSuccessfulBackupET
LastSuccessfulBackupStatus
```

Environment behavior:

```text
Physical     -> host/path-aware object rows
Hyper-V      -> VM object rows
Nutanix AHV  -> VM object rows
```

## Path Detail

Power BI role:

```text
Technical drilldown only. Do not expose this on the executive page.
```

Columns:

```text
PathKey
PGKey
ObjectKey
InventoryDateET
Cluster
Environment
ProtectionGroup
HostName
ObjectName
IncludedPath
ExcludedPath
ExclusionLevel
SkipNestedVolumes
GlobalExcludePaths
```

Environment behavior:

```text
Physical     -> populated for file/volume protection path visibility
Hyper-V      -> generally not populated unless the API exposes path-like data
Nutanix AHV  -> generally not populated unless the API exposes path-like data
```

Do not create fake path rows for VM environments just to fill a table.

## Exceptions

Power BI role:

```text
Main driver for audit, risk, and operations action views.
```

Columns:

```text
InventoryDateET
Cluster
Environment
ProtectionGroup
HostName
ObjectName
ExceptionType
Severity
ExceptionReason
RecommendedAction
```

Baseline exception types:

```text
PG_ZERO_OBJECTS
MISSING_POLICY
MISSING_LAST_SUCCESS
LAST_SUCCESS_GT_24H
LAST_SUCCESS_GT_48H
PG_GLOBAL_EXCLUSIONS
OBJECT_LEVEL_EXCLUSIONS
OBJECT_NO_INCLUDED_PATH
SKIP_NESTED_VOLUMES
VERY_LARGE_PG
HIGH_EXCLUSION_COUNT
```

Severity guidance:

```text
Critical = likely backup coverage or recoverability risk
High     = stale backup, missing object configuration, or immediate operational concern
Medium   = audit/configuration concern or potentially risky design
Low      = informational only
```

## Run Metadata

Power BI role:

```text
Refresh diagnostics and report header metadata.
```

Fields:

```text
InventoryDateET
ScriptName
HeliosBaseUrl
SelectedClusters
SelectedEnvironments
OutputFiles
Counts
Notes
```

## Power BI model guidance

Recommended table names after import:

```text
PG_Summary
Object_Detail
Path_Detail
Exceptions
Run_Metadata
```

Recommended relationships:

```text
PG_Summary[PGKey]          1 -> * Object_Detail[PGKey]
PG_Summary[PGKey]          1 -> * Path_Detail[PGKey]
PG_Summary[ProtectionGroup] 1 -> * Exceptions[ProtectionGroup]     -- temporary if PGKey is not available in Exceptions
```

Future improvement:

```text
Add PGKey to Exceptions for direct relationship.
```

Recommended dimension tables later:

```text
DimCluster
DimEnvironment
DimPolicy
DimProtectionGroup
DimDate
DimSeverity
DimExceptionType
```

## Power BI page design

### Page 1: Executive Overview

Purpose:

```text
Read-only published dashboard feel.
```

Top KPI cards:

```text
Active Clusters
Active Protection Groups
Protected Objects
Objects with Exclusions
Successful Backups in last 24h
Exceptions Count
```

Main visuals:

```text
PGs by Environment
Objects by Environment
PGs by Cluster
PGs by Policy
Included vs Excluded Paths by Cluster
Top 10 PGs by Object Count
Backup Success 24h %
Exception Summary by Severity
```

Only a compact detail table should appear at the bottom.

### Page 2: Environment Breakdown

Purpose:

```text
Show how each environment is protected.
```

Visuals:

```text
Environment slicer
Cluster vs Environment matrix
Object count by environment
Protection Group count by environment
Exclusion count by environment
Backup freshness by environment
Policy distribution by environment
Success/failure status by environment
```

### Page 3: Exceptions / Risk View

Purpose:

```text
Operational and audit risks.
```

Visuals:

```text
Exceptions by severity
Exceptions by environment
Exceptions by cluster
PGs with no successful backup in last 24/48h
PGs with zero objects
Objects with missing included paths
Objects with object-level exclusions
PGs with global exclusions
SkipNestedVolumes enabled
Top risky PGs
```

### Page 4: Detail Drilldown

Purpose:

```text
Controlled technical drilldown, not raw spreadsheet manipulation.
```

Allowed slicers:

```text
Cluster
Environment
Policy
Protection Group
Host/Object
```

## Dashboard style

```text
Enterprise dashboard style
Blue/teal/green theme
White/light gray background
Rounded KPI cards
Limited slicers
Read-only / published view feel
Avoid raw-data-first layout
Avoid too many editable-looking tables
Avoid making the page look like Excel
```

## DAX measure examples

```DAX
Active PGs =
DISTINCTCOUNT(PG_Summary[ProtectionGroupId])
```

```DAX
Protected Objects =
SUM(PG_Summary[ObjectCount])
```

```DAX
PGs With Global Exclusions =
CALCULATE(
    DISTINCTCOUNT(PG_Summary[ProtectionGroupId]),
    PG_Summary[GlobalExcludePathCount] > 0
)
```

```DAX
Objects With Exclusions =
CALCULATE(
    DISTINCTCOUNT(Object_Detail[ObjectId]),
    Object_Detail[HasObjectExclusions] = TRUE()
)
```

```DAX
Backup Success 24h % =
DIVIDE(
    CALCULATE(
        COUNTROWS(PG_Summary),
        PG_Summary[LastSuccessfulBackupAgeHours] <= 24
    ),
    COUNTROWS(PG_Summary)
)
```

```DAX
Exception Count =
COUNTROWS(Exceptions)
```

```DAX
Critical Exceptions =
CALCULATE(
    COUNTROWS(Exceptions),
    Exceptions[Severity] = "Critical"
)
```

## Claude Code guidance

Claude Code should work against stable artifacts, not random one-off scripts.

Preferred structure:

```text
inventory/Get-CohesityProtectionInventory.ps1
inventory/Cohesity_Protection_DataContract.md
inventory/PowerBI_PhysicalPG_Dashboard_Baseline.md
inventory/PowerBI_PhysicalPG_Dashboard_Measures.dax
```

Claude Code future tasks:

```text
Refactor PowerShell functions
Add all-environment support
Improve last-success run scanning
Add environment-specific risk logic
Generate or update DAX measures
Generate Power BI theme JSON
Modify PBIP/TMDL files after template is created
Generate dashboard insight summaries
```

## Known baseline limitation

Current generic collector uses latest run information to populate `LastSuccessfulBackupET` only when the latest run status is success.

Future enhancement:

```text
For each PG, scan recent runs and pick the most recent successful completed backup.
```

This should be added after the Physical + Hyper-V + Nutanix inventory model is validated.
