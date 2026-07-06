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
- Hyper-V test returned 9 CollectionErrors.
- Need exact CollectionErrors from Cohesity_Protection_Run_Metadata.json before changing script.

Current risk:

The 9 Hyper-V errors could be API collection, Hyper-V params mismatch, or object/run processing. Do not guess until Stage and Error values are reviewed.

Next action:

Capture and review Counts, EnvironmentCounts, and all CollectionErrors from Cohesity_Protection_Run_Metadata.json.

Next likely fix area:

Depends on CollectionErrors Stage:

- Get-ProtectionGroups: API call or cluster access issue.
- EnvironmentParams: Hyper-V parameter field name mismatch.
- ProcessProtectionGroup: object extraction, policy, or run field handling issue.
