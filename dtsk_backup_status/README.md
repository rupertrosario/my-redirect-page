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

- Shows `SLA Due`, `SLA Status`, `Assigned To`, and `Assignment Action` from ServiceNow DTSK data.
- Correct SLA for this report is the 2-day SLA from when the DTSK came to the Backup group.
- Current implementation calculates SLA in Dynatrace using `sys_created_on` as the Backup-group start timestamp.
- SLA calculation:
  - `SLA Start = sys_created_on`
  - `SLA Due = sys_created_on + 2 days`
  - `SLA Status = Within SLA | Breached SLA | SLA Missing`
- `Assignment Action` is `Assigned` when `assigned_to` is populated; otherwise it is `Please assign`.
- The `Cluster` column shows only the Cohesity cluster where backup evidence was found.
- Search diagnostics such as `1 cluster(s) checked` are not shown in the email `Cluster` column. They remain only in task JSON/debug summary.

## Running implementation status

Completed so far:

- Created DTSK backup-status report workflow structure under `dtsk_backup_status/`.
- Confirmed workflow is report-only: no ServiceNow updates, no work notes, no journal-table reads, and no idempotency marker.
- Built ServiceNow DTSK search configuration for active decommission DTSKs in the Backup assignment group.
- Built `02_dtsk_prepare_work_items.js` to normalize DTSK rows into loop work items.
- Added assignment ownership fields:
  - `Assigned To`
  - `Assignment Group`
  - `Assignment Action`
- Added calculated SLA fields based on `sys_created_on`:
  - `SLA Start`
  - `SLA Due`
  - `SLA Status`
- Built Cohesity cluster map task.
- Built Cohesity validation loop task with GET-only protected-object validation.
- Kept backup validation at CI/object level rather than only task/request level.
- Added DB/CN fallback logic for SQL/Oracle validation across clusters.
- Removed misleading `1 cluster(s) checked` text from the email `Cluster` column.
- Kept cluster-search diagnostics only in JSON/debug output.
- Added email aggregation task with executive summary, backup type summary, and details table.
- Added summary counts for:
  - DTSKs reviewed
  - Within SLA
  - Breached SLA
  - SLA missing
  - Assigned DTSKs
  - Unassigned DTSKs
  - Server-level protected CIs
  - DB-protected CIs
  - No backup found
  - DB backup found but no server-level backup
- Updated details table to include:
  - `DTSK`
  - `Decom Request`
  - `SLA Due`
  - `SLA Status`
  - `Assigned To`
  - `Assignment Action`
  - `Server`
  - `Backup Type`
  - `Object`
  - `Source`
  - `Cluster`
  - `Protection Group`
  - `Latest Backup`

Current Dynatrace update requirement:

- Keep the existing ServiceNow fields, including `sys_created_on`.
- Replace only these Dynatrace JS tasks with the GitHub versions:
  - `dtsk_backup_status/02_dtsk_prepare_work_items.js`
  - `dtsk_backup_status/05_dtsk_aggregate_report.js`
- No change required for:
  - `dtsk_get_cluster_map`
  - `dtsk_validate_one_ci`
  - email body expression

Email body expression remains:

```text
{{ result("dtsk_aggregate_report").markdown }}
```

Validation checklist:

- Run one DTSK created within the last 2 days; expected `SLA Status = Within SLA`.
- Run one DTSK older than 2 days; expected `SLA Status = Breached SLA`.
- Run one assigned DTSK; expected `Assignment Action = Assigned`.
- Run one unassigned DTSK; expected `Assignment Action = Please assign`.
- Run one no-backup CI; expected `Backup Type = No Backup Found` and `Cluster = N/A`.
- Confirm the email does not show `1 cluster(s) checked` in the `Cluster` column.

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
| `02_dtsk_prepare_work_items.js` | Converts DTSK records into clean loop work items and calculates SLA due/status |
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
