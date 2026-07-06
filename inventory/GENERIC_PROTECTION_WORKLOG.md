# Generic Cohesity Protection Inventory Worklog

This file tracks actual work and status.

Active script: inventory/Get-CohesityProtectionInventory.ps1

Scope: Physical, Hyper-V, Nutanix AHV.

Frozen script: inventory/Get-PhysicalPGInventory.ps1. Standalone Physical is working and should not be changed unless requested.

Current decision: final Power BI model should use generic Cohesity_Protection output files, not the standalone Physical CSVs.

Current status:

- Standalone Physical works and is frozen.
- Generic collector uses AES helper for key loading.
- Generic collector has safer optional field handling.
- Hyper-V test hit a read-only automatic variable conflict.
- Generic collector was patched in commit 29fe01325c6820fca3587d37b8967a866a27d596.
- Runbook was refreshed in commit 2dc4bbe651efab6ab98e0c62c0337330061f7a4b.

Current risk:

Hyper-V may still have object extraction issues after the variable fix. Retest is required.

Next action:

Pull latest and rerun Hyper-V only.

Next likely fix area:

If PG rows exist but Object Detail rows are zero, update generic VM object extraction logic.
