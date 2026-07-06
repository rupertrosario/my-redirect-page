# Generic Cohesity Protection Inventory Runbook

Current focus: validate VM protection parameter columns.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Script build: 2b53bbd5410fc4f32a7539278adc024a96d99735

## Test order

1. Pull latest branch.
2. Run the generic collector for Hyper-V only.
3. Run the CSV validation script.
4. Confirm VM columns exist in the PG Summary and Object Detail CSVs.
5. Repeat for AHV only.

## Commands

From the inventory folder:

    git checkout Cohesity_Automations
    git pull
    cd X:\PowerShell\Cohesity_API_Scripts\inventory
    Remove-Item .\Cohesity_Protection_*_Latest.csv -ErrorAction SilentlyContinue
    Remove-Item .\Cohesity_Protection_Run_Metadata.json -ErrorAction SilentlyContinue
    .\Get-CohesityProtectionInventory.ps1

For Hyper-V choose environment option 2.
For AHV choose environment option 3.

After each run:

    .\Test-CohesityProtectionInventoryCsv.ps1
    $m = Get-Content .\Cohesity_Protection_Run_Metadata.json -Raw | ConvertFrom-Json
    $m.Counts
    $m.EnvironmentCounts
    $m.CollectionErrors

## Columns to spot-check

PG Summary:

- SourceId
- SourceName
- CloudMigration
- AppConsistentSnapshot
- FallbackToCrashConsistentSnapshot
- ContinueOnQuiesceFailure
- BackupDirectlyAttachedVolumeGroups
- GlobalIncludeDisks
- GlobalExcludeDisks
- ExcludeObjectIds
- VmTagIds
- ExcludeVmTagIds
- IndexingEnabled
- IndexingIncludePaths
- IndexingExcludePaths

Object Detail:

- ObjectIncludeDisks
- ObjectExcludeDisks

Do not move to Power BI until both Hyper-V and AHV validate.
