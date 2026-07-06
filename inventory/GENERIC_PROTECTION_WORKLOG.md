# Generic Cohesity Protection Inventory Worklog

Active script: inventory/Get-CohesityProtectionInventory.ps1

Scope: Physical, Hyper-V, Nutanix AHV.

Frozen script: inventory/Get-PhysicalPGInventory.ps1. Standalone Physical is working and should not be changed unless requested.

Decision: Power BI output must use common tables plus specific detail tables.

Common tables:

- PG Summary
- Object Detail

Specific detail tables:

- Physical Path Detail
- VM Disk Detail
- VM PG Config

Common PG/Object fields should stay clean and consistent across Physical, Hyper-V, and AHV.

Environment-specific fields should not be forced into every row of the common tables.

Status:

- Standalone Physical works and is frozen.
- Generic collector uses AES helper for key loading.
- Hyper-V CSV validation completed earlier.
- VM field requirements are documented in inventory/GENERIC_PROTECTION_VM_FIELDS.md.
- Current script captures VM fields, but next redesign should split common and specific outputs.

Next action:

Adjust generic collector output model for Power BI:

- keep common PG fields in PG Summary
- keep common object fields in Object Detail
- move Physical path fields to Physical Path Detail
- move VM disk fields to VM Disk Detail
- move VM PG configuration fields to VM PG Config

Risk:

One wide table will become messy in Power BI. Split output is preferred.
