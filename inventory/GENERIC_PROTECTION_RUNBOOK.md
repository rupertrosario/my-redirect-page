# Generic Cohesity Protection Inventory Runbook

Current focus: Nutanix AHV test.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Hyper-V CSV validation completed after the generic collector fix.

Next steps:

1. Pull latest branch.
2. Run the generic collector again.
3. Select one known AHV cluster.
4. Select environment option 3.
5. Run the CSV validation script.
6. Review Counts, EnvironmentCounts, and CollectionErrors.

Stop after AHV. Do not move to Power BI yet.
