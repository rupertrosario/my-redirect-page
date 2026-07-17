# Jira Update - Cohesity Interface Down Rework

The Cohesity Interface Down workflow is being reworked to support proper idempotency, so repeated workflow runs do not create duplicate ServiceNow incidents for the same active issue.

Based on feedback from the Dynatrace team, the workflow is being restructured so ServiceNow search results, CI details, and incident create/update actions are handled in the correct order.

- Keeping existing alert collection and interface validation logic unchanged.
- Separating ServiceNow CI search from validation.
- Matching each Team incident candidate to the correct Cohesity cluster CI.
- Keeping Team incident search aligned with the same validated source data.
- Focusing first on the Team incident create/update path.
- Leaving DC handling unchanged for now.
