# Generic Cohesity Protection Inventory Worklog

Active script: inventory/Get-CohesityProtectionInventory.ps1

Scope: Physical, Hyper-V, Nutanix AHV.

Frozen script: inventory/Get-PhysicalPGInventory.ps1. Standalone Physical is working and should not be changed unless requested.

Decision: final Power BI model should use generic Cohesity_Protection output files.

Status:

- Standalone Physical works and is frozen.
- Generic collector uses AES helper for key loading.
- Generic collector has safer optional field handling.
- Hyper-V CSV validation completed earlier.
- VM field requirements are documented in inventory/GENERIC_PROTECTION_VM_FIELDS.md.
- Generic collector now captures Hyper-V and AHV PG/object protection parameter fields.

Latest commits:

- VM field requirements: 8339e603a4b39d14b0f93ec88b540acd7e4b8ad3
- Generic script update: 2b53bbd5410fc4f32a7539278adc024a96d99735
- Runbook update: 82c0c11c4d322c8529122a7c4a61a65e3380c848

Next action:

Run generic collector for Hyper-V only, then AHV only, and validate columns before Power BI work.

Risk:

Extra VM configuration columns may be trimmed later after reviewing real output.
