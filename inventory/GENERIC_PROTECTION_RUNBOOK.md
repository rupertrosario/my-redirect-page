# Generic Cohesity Protection Inventory Runbook

Current focus: single environment run model.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Environment selection must be one environment at a time.

Allowed environment choices:

- Physical
- Hyper-V
- Nutanix AHV

Remove the All environments option from the script.

Power BI slicers:

- Cluster
- Environment

Main table:

- Cluster
- Environment
- ProtectionGroup
- PolicyName
- IsActive
- IsPaused
- ObjectCount
- LastRunStatus
- LastSuccessfulBackupET

Environment-specific details should appear only when that environment is selected.
