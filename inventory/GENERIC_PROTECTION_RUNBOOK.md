# Generic Cohesity Protection Inventory Runbook

Current focus: Power BI slicer-ready output.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Main report table uses shared PG fields only.

Power BI slicers needed:

- Cluster
- Environment
- Policy
- Active or paused state

Main fields needed:

- Protection group
- Object count
- Backup status
- Latest backup time

Platform-specific settings stay in separate output files.

Wait for output structure review before next report work.
