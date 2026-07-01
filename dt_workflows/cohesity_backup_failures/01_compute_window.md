# Step 01 - compute_window

Use the existing `dyna_faul_cohesity_inc` logic as the source for this Dynatrace action.

Dynatrace action name: `compute_window`

Purpose: calculate the current incident window from 18:00 ET to the next 18:00 ET.

Required outputs:

- `correlationId`
- `windowKey`
- `windowLabel`
- `snStartUtc`
- `snEndUtc`

Validation:

Run only this action first. Confirm the window changes only after 18:00 ET.
