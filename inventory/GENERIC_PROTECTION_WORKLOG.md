# Generic Cohesity Protection Inventory Worklog

This file tracks actual work and status.

Active script: inventory/Get-CohesityProtectionInventory.ps1

Scope: Physical, Hyper-V, Nutanix AHV.

Frozen script: inventory/Get-PhysicalPGInventory.ps1. Standalone Physical is working and should not be changed unless requested.

Current decision: final Power BI model should use generic Cohesity_Protection output files, not the standalone Physical CSVs.

Current status:

- Standalone Physical works.
- Generic collector now uses AES helper for key loading.
- Generic collector has safer optional field handling.
- Next validation is Hyper-V first, then Nutanix AHV.

Current risk:

Hyper-V or Nutanix may show PG rows but zero object rows. If that happens, fix only generic VM object extraction logic.

Next likely fix area:

Get-ObjectsFromParams in inventory/Get-CohesityProtectionInventory.ps1.
