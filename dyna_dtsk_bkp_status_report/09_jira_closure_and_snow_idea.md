# DTSK Backup Validation - JIRA Closure and ServiceNow Idea

## JIRA Closure Update

Completed the initial report-only Dynatrace Workflow for Cohesity backup validation for decommission DTSKs.

### Completed

- Workflow fetches active decommission DTSKs from ServiceNow table `x_alfi_decom_decom_task`.
- Workflow validates backup status from Cohesity Helios using read-only API checks.
- Workflow sends an email report with backup evidence.
- Initial testing was completed using ServiceNow Development.
- Generated workflow emails are attached as evidence.

### Current scope

This phase is report-only.

No ServiceNow DTSK update is performed yet. No automatic close, assignment, `CR Required = No`, or CI removal from backup action is implemented in this phase.

### Future enhancement

The next phase will require ServiceNow review and approval before implementation.

The decommission backup stage includes:

- validating backup status
- updating the DTSK with validation evidence
- supporting removal or unregistration of the CI from backup where applicable

Future update behavior will need to be controlled by workload type and eligibility. Some DTSKs may be eligible for automation, while others may require manual review or additional backup cleanup before closure.

A ServiceNow idea/request will be opened to request:

- ServiceNow DEV access for prototype testing
- approval to update DTSK `work_notes`
- approval to read existing notes for duplicate prevention
- approval for eligible `CR Required = No` and state updates
- review of assignment logic
- review of future CI removal from backup handling

This JIRA is being closed for the initial report-only validation and email reporting workflow. A separate implementation enhancement will be created after ServiceNow confirms the approved approach.

---

## ServiceNow Idea Form

### Idea Title

Automate DTSK updates for Cohesity backup validation and CI removal from backup through Dynatrace Workflow

### Targeted Release Date

31-Jul-2026

### Configuration Item

ServiceNow (PRODUCTION)

### Priority

4 - Low

### Idea Description

The initial report-only Dynatrace Workflow for decommission DTSK backup validation has been completed and tested with ServiceNow Development.

The current workflow uses a ServiceNow search task to read active decommission DTSKs from `x_alfi_decom_decom_task`. It then validates backup status from Cohesity Helios using read-only API checks and sends an email report with backup evidence.

The next phase is to build a working prototype in ServiceNow DEV for controlled DTSK updates before any PROD implementation is considered.

Requesting ServiceNow review and DEV access to validate the approved approach for:

- updating DTSK `work_notes`
- checking existing notes to prevent duplicate updates
- updating eligible DTSKs with `CR Required = No`
- updating DTSK state only for approved scenarios
- reviewing possible assignment logic using active backup-team users
- supporting future CI removal or unregistration from backup where applicable

This is not only a backup validation use case. The decommission backup stage includes both backup validation and CI removal from backup where applicable.

Reference JIRA: `<JIRA_KEY>`

### Acceptance Criteria

- ServiceNow confirms whether Dynatrace Workflow can be tested in ServiceNow DEV for a working prototype.
- ServiceNow confirms whether the approved DEV prototype can later be replicated to PROD through the standard change/release process.
- ServiceNow confirms whether Dynatrace Workflow can update DTSK `work_notes`.
- ServiceNow confirms whether existing DTSK notes can be read to prevent duplicate workflow updates.
- ServiceNow confirms whether marker-based duplicate prevention is acceptable.
- ServiceNow confirms whether `CR Required = No` can be updated for eligible DTSKs.
- ServiceNow confirms whether DTSK state updates can be performed only for approved scenarios.
- ServiceNow confirms whether active backup-team users can be fetched for possible assignment logic.
- ServiceNow confirms any access, role, integration-user, business rule, or approval requirement before implementation.

### Update behavior to review

- Automatic updates should apply only when backup validation gives a clear and eligible result.
- Some workloads may require additional backup cleanup or CI removal from backup before closure.
- Where additional cleanup or manual review is required, the workflow should update `work_notes` with validation evidence but should not automatically close the DTSK.
- Auto-close behavior should be limited to scenarios approved by ServiceNow and the process owner.

### Business Value

This enhancement will reduce manual effort for decommission DTSK handling by allowing validated backup results to be recorded directly on the related DTSK.

Expected benefits:

- Reduces manual backup validation follow-up.
- Adds consistent backup evidence into DTSK work notes.
- Avoids duplicate notes by checking existing entries before writing.
- Supports controlled update of `CR Required = No` when eligible.
- Improves audit trail and operational consistency.
- Creates a controlled path for future CI removal from backup or backup cleanup where applicable.

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
