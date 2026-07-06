# Generic Cohesity Protection Inventory Runbook

This file is overwritten whenever the current run instructions change.

Current script: inventory/Get-CohesityProtectionInventory.ps1

Current focus: Hyper-V collection errors.

Standalone Physical script is frozen for now.

Current issue: Hyper-V run returned 9 CollectionErrors.

Next action:

1. Stay in X:\PowerShell\Cohesity_API_Scripts\inventory.
2. Do not rerun the script yet.
3. Extract the actual CollectionErrors from the metadata JSON.

Run this:

    $m = Get-Content .\Cohesity_Protection_Run_Metadata.json -Raw | ConvertFrom-Json
    $m.Counts
    $m.EnvironmentCounts
    $m.CollectionErrors | Format-Table Cluster, Environment, Stage, Error -AutoSize

If the error text is truncated in the table, run this also:

    $m.CollectionErrors | ConvertTo-Json -Depth 6

Send back:

- Counts
- EnvironmentCounts
- all 9 CollectionErrors

Do not test Nutanix yet. Do not move to Power BI yet.

Likely next fix depends on Stage:

- Get-ProtectionGroups means API call, parameter, or accessClusterId problem.
- EnvironmentParams means Hyper-V parameter field name mismatch.
- ProcessProtectionGroup means object extraction or run/policy field handling issue.
