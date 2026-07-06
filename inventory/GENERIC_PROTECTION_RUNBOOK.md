# Generic Cohesity Protection Inventory Runbook

Current focus: verify VM details from the generated CSV files.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Script build: 2b53bbd5410fc4f32a7539278adc024a96d99735

The UI image path is only an icon reference. The CSV output is the source to validate.

Next test:

1. Pull latest branch.
2. Run Hyper-V only.
3. Run CSV validation.
4. Review Object Detail row count.
5. Review the first 20 rows from the Object Detail CSV.
6. Repeat the same for AHV only.

Key fields to check:

- ObjectName
- ObjectId
- ObjectType
- ObjectIncludeDisks
- ObjectExcludeDisks

Expected result:

- If Object Detail rows are greater than zero, the API is exposing protected object entries.
- If PG Summary rows are greater than zero but Object Detail rows are zero, the script needs another object source path or endpoint.

Do not move to Power BI yet.
