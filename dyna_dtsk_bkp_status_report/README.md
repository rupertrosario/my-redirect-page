# Dynatrace DTSK Backup Status Report

Purpose: Fetch active ServiceNow decommission DTSKs, validate Cohesity backup status, and send an email report.

Current scope:

- ServiceNow DTSK search only
- Cohesity GET-only validation
- Email report only
- No ServiceNow update
- No `work_notes`
- No `sys_journal_field`
- No idempotency marker

Workflow:

```text
dtsk_snow_search
→ dtsk_prepare_work_items
→ dtsk_get_cluster_map
→ dtsk_validate_one_ci [loop task]
→ dtsk_aggregate_report
→ email
```

Files in this folder:

| File | Purpose |
|---|---|
| `01_dtsk_snow_search_config.md` | ServiceNow DTSK search configuration |
| `02_dtsk_prepare_work_items.js` | Converts DTSK records into clean loop work items |
| `03_dtsk_get_cluster_map.js` | Gets Cohesity cluster list and cluster map |

Pending files to add after code is finalized:

| File | Purpose |
|---|---|
| `04_dtsk_validate_one_ci.js` | Loop worker with full PowerShell-style Cohesity validation logic |
| `05_dtsk_aggregate_report.js` | Aggregates loop outputs and builds email markdown |
