# Generic Cohesity Protection Inventory Runbook

Current focus: Hyper-V retest.

Current script: inventory/Get-CohesityProtectionInventory.ps1

The generic collector was updated in commit 29fe01325c6820fca3587d37b8967a866a27d596.

Next steps:

1. Pull the latest Cohesity_Automations branch.
2. Run the generic collector again.
3. Select the same Hyper-V cluster.
4. Select environment option 2.
5. Review the metadata JSON for Counts, EnvironmentCounts, and CollectionErrors.

Stop after Hyper-V. Do not test Nutanix yet.
