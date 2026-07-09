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

Email output:

- Shows `SLA`, `Assigned To`, and `Assignment Action` from ServiceNow DTSK data.
- `SLA` is derived from the first populated DTSK field in this order: `u_sla`, `u_sla_due`, `sla`, `sla_due`, `due_date`, `made_sla`.
- `Assignment Action` is `Assigned` when `assigned_to` is populated; otherwise it is `Please assign`.
- The `Cluster` column shows only the Cohesity cluster where backup evidence was found.
- Search diagnostics such as `1 cluster(s) checked` are not shown in the email `Cluster` column. They remain only in task JSON/debug summary.

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
| `04_dtsk_validate_one_ci.js` | Loop worker with Cohesity validation logic and ownership fields |
| `05_dtsk_aggregate_report.js` | Aggregates loop outputs and builds email markdown |
| `06_dtsk_send_email_report_config.md` | Email task configuration |
| `07_future_servicenow_writeback_use_cases.md` | Future ServiceNow writeback use cases |
| `08_management_jira_writeup.md` | Management/Jira status writeup |
| `09_jira_closure_and_snow_idea.md` | Jira closure and ServiceNow idea notes |
| `10_ppt_draft_data.md` | PPT draft content/data |
| `11_confluence_table_draft.md` | Confluence table draft |
| `99_test_prepare_work_items_mock.js` | Mock test data for prepare work items |
| `cr_bkup_status.ps1` | PowerShell reference script |
