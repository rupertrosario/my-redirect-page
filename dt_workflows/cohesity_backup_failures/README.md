# Cohesity Backup Failures Dynatrace Workflow

Build this workflow step by step on the `dyna_alerts` branch only.

Order:

1. `compute_window` - create the 18:00 ET incident window.
2. `cohesity_backup_failures` - collect backup failures with no later success.
3. `snow_search` - search for an active incident for the current window.
4. `snow_update` - update existing incident.
5. `snow_create` - create incident if none exists.
