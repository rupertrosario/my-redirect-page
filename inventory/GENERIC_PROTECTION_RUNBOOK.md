# Generic Cohesity Protection Inventory Runbook

This file is overwritten whenever the current run instructions change.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Scope: Physical, Hyper-V, Nutanix AHV.

Standalone Physical script is frozen for now.

Current test order:

1. Pull latest from branch Cohesity_Automations.
2. Go to X:\PowerShell\Cohesity_API_Scripts\inventory.
3. Remove only generic Cohesity_Protection output files from the inventory folder.
4. Run Get-CohesityProtectionInventory.ps1.
5. Test one known Hyper-V cluster with environment option 2.
6. Run Test-CohesityProtectionInventoryCsv.ps1.
7. Review Counts, EnvironmentCounts, and CollectionErrors in Cohesity_Protection_Run_Metadata.json.
8. Repeat with one known Nutanix AHV cluster using environment option 3.
9. Only after Hyper-V and Nutanix show object rows, run all clusters and all baseline environments using cluster option 0 and environment option 0.

Send back only: PG Summary rows, Object Detail rows, Path Detail rows, Exception rows, Collection errors, EnvironmentCounts, and CollectionErrors.

Expected: Hyper-V and Nutanix path rows can be zero. Object rows should not be zero if VM extraction is working.
