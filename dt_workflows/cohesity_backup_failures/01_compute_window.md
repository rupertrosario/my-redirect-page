# Step 01 - compute_window

Purpose: create the daily 18:00 ET window for the Cohesity Backup Failures workflow.

Current source action: `dyna_faul_cohesity_inc`.

Required action name in Dynatrace: `compute_window`.

Outputs to verify:

- `correlationId`
- `windowKey`
- `windowLabel`
- `snStartUtc`
- `snEndUtc`

Validation:

Run this action alone first. Confirm `windowKey` is based on the current 18:00 ET boundary before wiring the failure collector or ServiceNow actions.
