# DTSK Backup Validation - JIRA Closure and ServiceNow Idea

## JIRA Closure Update

Completed the initial automation for Cohesity backup validation for decommission DTSKs.

### Completed

- Built the initial report-only validation workflow for decommission DTSKs.
- Workflow fetches DTSKs from ServiceNow table `x_alfi_decom_decom_task`.
- Added read-only Cohesity Helios validation for FS, VM, Hyper-V, Nutanix/AHV, SQL, and Oracle backups.
- Added email reporting with backup evidence and summary output.
- Added handling for backup found, no backup found, DB only / no server backup, and no active DTSKs found.
- Generated workflow emails are attached to the JIRA as evidence.

### Current phase

- Current implementation is report-only.
- ServiceNow fetch/read is enabled.
- Cohesity validation is read-only.
- Email reporting is enabled.
- DTSK updates are not implemented in this phase.
- `work_notes` update is allowed, but not implemented in this phase.
- `CR Required = No`, state update, and assignment update are not implemented in this phase.

### Holdback / future enhancement

The update portion is being held for a separate ServiceNow review because update behavior needs ServiceNow confirmation and approval before implementation.

Future ServiceNow update items to review:

- Add validation result to DTSK `work_notes`.
- Read existing notes from `sys_journal_field` to prevent duplicate updates.
- Use marker `[COHESITY_BACKUP_VALIDATION_WORKFLOW]` for duplicate prevention.
- Set `CR Required = No` only when backup is confirmed.
- Use DTSK state values `2 = Work in Progress` and `3 = Closed Complete` only after update rules are approved.
- Consider assignment logic later by fetching active ServiceNow users from the backup team. This is only a proposed option.

### Next action

A ServiceNow idea/request will be opened to confirm feasibility and approval for the DTSK update phase. The ServiceNow idea/request will reference this JIRA and will cover `work_notes`, duplicate prevention, `CR Required = No`, state update, and possible assignment logic.

This JIRA is being closed for the initial Cohesity backup validation and email reporting automation.

---

## ServiceNow Idea / Request

### Title

Feasibility review for automated DTSK update from Cohesity backup validation workflow

### Request

Requesting ServiceNow review for a future enhancement to update decommission DTSKs after automated Cohesity backup validation.

Reference JIRA: `<JIRA_KEY>`

### Current status

The current workflow is completed as report-only.

Current workflow:

- dtsk_snow_search
- dtsk_prepare_work_items
- dtsk_get_cluster_map
- dtsk_validate_one_ci
- dtsk_aggregate_report
- dtsk_send_email_report

Current DTSK table: `x_alfi_decom_decom_task`

The workflow currently reads DTSKs, validates backup status from Cohesity Helios, and sends an email report. No DTSK update is performed today.

### Feasibility items requested from ServiceNow

Please confirm whether the following updates are feasible and allowed.

- Update DTSK `work_notes` using `x_alfi_decom_decom_task.work_notes`.
- Read existing work notes from `sys_journal_field` for duplicate prevention.
- Use `element_id = DTSK sys_id` and `element = work_notes` when checking existing notes.
- Use marker `[COHESITY_BACKUP_VALIDATION_WORKFLOW]` to avoid duplicate notes on reruns.
- Set `CR Required = No` when backup is confirmed.
- Update DTSK state when eligible using `2 = Work in Progress` and `3 = Closed Complete`.
- Review possible assignment logic by fetching active ServiceNow users from the backup team. Possible source tables are `sys_user`, `sys_user_grmember`, and `sys_user_group`.

### Expected future behavior

- Backup confirmed: write validation note, set `CR Required = No`, update state only if approved, assign only if assignment logic is approved.
- No backup found: write validation note only; do not close automatically.
- SQL/Oracle only with no server-level backup: write validation note only; do not close automatically unless approved later.
- Validation error: write validation note only; do not close automatically.

### Requested outcome

Please confirm whether these ServiceNow update actions are feasible and what permissions, field behavior, ACLs, or process approvals are required.

After ServiceNow approval, a separate implementation JIRA will be created for the DTSK update phase.
