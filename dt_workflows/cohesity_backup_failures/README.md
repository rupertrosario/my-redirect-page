# Cohesity Backup Failures Dynatrace Workflow

Workflow build order:

1. `01_compute_window.js` - creates the daily 18:00 ET incident window.
2. `02_collect_failures.js` - collects Cohesity failures with no later successful run.
3. `03_snow_search.md` - ServiceNow search wiring.
4. `04_snow_update.md` - update existing active incident.
5. `05_snow_create.md` - create incident when none exists.
