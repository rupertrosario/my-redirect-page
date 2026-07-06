# Generic Cohesity Protection Inventory Worklog

Active script: inventory/Get-CohesityProtectionInventory.ps1

Scope: Physical, Hyper-V, Nutanix AHV.

Frozen script: inventory/Get-PhysicalPGInventory.ps1. Standalone Physical is working and should not be changed unless requested.

Decision: final Power BI model should use generic Cohesity_Protection output files.

Status:

- Standalone Physical works and is frozen.
- Generic collector uses AES helper for key loading.
- Generic collector has safer optional field handling.
- Hyper-V CSV validation completed.
- Runbook now points to AHV test.

Next action:

Run generic collector for AHV only.

Risk:

If AHV PG rows exist but Object Detail rows are zero, update generic VM object extraction logic.
