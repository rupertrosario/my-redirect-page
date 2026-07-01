# DTSK Backup Validation - JIRA Closure and ServiceNow Idea

## JIRA Closure Update

Completed the initial report-only Dynatrace Workflow for Cohesity backup validation for decommission DTSKs.

### Completed

- Workflow fetches active decommission DTSKs from ServiceNow table `x_alfi_decom_decom_task`.
- Workflow validates backup status from Cohesity Helios using read-only API checks.
- Workflow sends an email report with backup evidence.
- Generated workflow emails are attached as evidence.

### Current scope

This phase is report-only.

No ServiceNow DTSK update is performed yet. No automatic close, assignment, `CR Required = No`, or CI removal from backup action is implemented in this phase.

### Future enhancement

The next phase will require ServiceNow review and approval before implementation.

The decommission backup stage includes:

- validating backup status
- updating the DTSK with validation evidence
- supporting CI removal from backup where applicable

Future update behavior will need to be controlled by workload type and eligibility. Some DTSKs may be eligible for automation, while others may require manual review or additional backup cleanup before closure.

A ServiceNow idea/request will be opened to request:

- approved test path for prototype testing
- approval to update DTSK `work_notes`
- approval to read existing notes for duplicate prevention
- approval for eligible `CR Required = No` and state updates
- review of assignment logic
- review of future CI removal from backup handling

This JIRA is being closed for the initial report-only validation and email reporting workflow. A separate implementation enhancement will be created after ServiceNow confirms the approved approach.

---

## ServiceNow Idea Form

### Idea Title

Allow Dynatrace Workflow to update decommission DTSKs for backup validation and CI removal from backup

### Targeted Release Date

31-Jul-2026

### Configuration Item

ServiceNow (PRODUCTION)

### Priority

4 - Low

### Idea Description

Requesting ServiceNow review and approval to allow Dynatrace Workflow to perform controlled updates on eligible decommission DTSKs.

The requested capability is to support the backup decommission stage, which includes:

- validating backup status
- updating the DTSK with backup validation evidence
- supporting CI removal from backup where applicable
- avoiding duplicate DTSK updates
- updating eligible DTSKs only when approved rules are met

Requested ServiceNow access/update review:

- read decommission DTSKs from `x_alfi_decom_decom_task`
- update DTSK `work_notes`
- read existing DTSK work notes to prevent duplicate notes
- use a unique identifier in workflow-created notes to avoid duplicate updates
- update `CR Required = No` only for eligible DTSKs
- update state only for approved scenarios
- review active backup-team user assignment logic
- review the approved path for future production implementation

Reference JIRA: `<JIRA_KEY>`

### Acceptance Criteria

- Approved test path is available for Dynatrace Workflow prototype testing.
- Dynatrace Workflow can read required DTSK details from `x_alfi_decom_decom_task`, including:
  - `sys_id`
  - DTSK number
  - CI/server name
  - decommission request details
  - created date
  - assignment group
  - state/status
  - `CR Required`
- Dynatrace Workflow can update DTSK `work_notes` with backup validation evidence.
- Existing DTSK work notes can be checked before adding new notes.
- `sys_journal_field` can be used to check existing work notes using the DTSK `sys_id`.
- Workflow-created work notes include a unique identifier so the workflow can detect previous updates and avoid adding duplicate notes on reruns.
- `CR Required = No` can be updated only for eligible DTSKs.
- DTSK state updates can be tested only for approved scenarios.
- State values to review:
  - `2 = Work in Progress`
  - `3 = Closed Complete`
- DTSKs requiring manual review or CI removal from backup should receive work notes only and should not be auto-closed.
- Assignment logic can be reviewed using:
  - `sys_user_group` for the backup group
  - `sys_user_grmember` for group members
  - `sys_user` for active user details and user `sys_id`
- Required roles, ACLs, integration-user permissions, business rules, and release process are identified before production implementation.
- Production implementation proceeds only after prototype testing and ServiceNow approval.

### Business Value

This enhancement will reduce manual effort for decommission DTSK handling by allowing validated backup results to be recorded directly on the related DTSK.

Expected benefits:

- Reduces manual backup validation follow-up.
- Adds consistent backup evidence into DTSK work notes.
- Avoids duplicate notes by checking existing entries before writing.
- Supports controlled update of `CR Required = No` when eligible.
- Improves audit trail and operational consistency.
- Creates a controlled path for future CI removal from backup where applicable.

### Will this update help reduce costs?

Yes

### Will this update help save time?

Yes

### How much time can be saved?

In the last 12 months, approximately **2,390 decommission DTSKs** were processed.

Based on an estimated **3-5 minutes of manual validation/update effort per DTSK**, the expected time saving is approximately **120-200 hours annually**.

### Is this for an external/internal audit or risk mitigation?

Yes

This supports internal operational control, audit evidence, and risk reduction for server decommission backup validation and CI removal from backup readiness.
