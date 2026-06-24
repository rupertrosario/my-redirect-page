# Cohesity Backup Validation for Decommission DTSKs

## Management Write-up

### Summary

A Dynatrace-based automation workflow is being built to validate Cohesity backup status for server decommission DTSKs. The objective is to reduce manual backup checks, provide consistent validation evidence, and generate a clear management-ready report before decommission activities proceed.

The workflow is currently focused only on **DTSK-level validation and reporting**. It does not update the parent CR, does not update unrelated ServiceNow records, and does not perform any Cohesity write actions.

### Current Scope

The workflow performs the following actions:

1. Searches ServiceNow for active decommission DTSKs assigned to the backup team.
2. Extracts the CI/server name from each DTSK.
3. Validates backup protection in Cohesity Helios using GET-only API calls.
4. Checks the following backup types:
   - FS
   - VM
   - Hyper-V
   - Nutanix/AHV
   - SQL
   - Oracle
5. Excludes NAS backups from server decommission validation.
6. Produces a consolidated email report showing:
   - DTSKs reviewed
   - Backup type summary
   - Server-level protected CIs
   - DB-protected CIs
   - No backup found
   - DB-only/no server-level backup cases
   - Detailed backup evidence per DTSK/CI

### ServiceNow Scope

ServiceNow activity is limited to DTSK records only.

Current phase:

- Read DTSK details only.
- No ServiceNow writeback.
- No DTSK state change.
- No assignment update.
- No CR Required update.
- No work_notes update.

Future phase, after validation:

- Update only the related DTSK with backup validation status and/or work_notes.
- Do not update the parent CR.
- Do not update unrelated CTASK/DTSK records.
- Include an idempotency marker to prevent duplicate work_notes on reruns.

### Report Behavior

The email report is management-friendly and contains:

- Executive Summary
- Backup Type Summary
- Details table
- Short note explaining validation scope

If there are no active DTSKs assigned to the backup team, the report shows only a clean run-status table stating that no validation was required.

### Important Report Notes

- NAS backups are excluded from this server decommission validation.
- No Backup Found means no in-scope Cohesity backup object was found for the CI.
- DB Only / No Server Backup means a SQL/Oracle backup was found, but no FS, VM, Hyper-V, or Nutanix/AHV backup was found for the server.
- Servers with names containing db or cn may require DB-level backup review if only FS/VM backup is found.

### Evidence / Attachments

The received workflow emails should be attached to the JIRA as validation evidence. Suggested attachments:

1. Sample email for active DTSK backup validation.
2. Sample email for no active DTSKs.
3. Any email showing backup evidence for FS/VM/SQL/Oracle validation.
4. Any email showing No Backup Found or DB Only / No Server Backup cases, if available.

### Current Status

The workflow is in validation/testing phase. The report-only flow is being refined first. ServiceNow writeback should be added only after the email output and validation logic are approved.

---

## JIRA Comment

Updated the scope back to the DTSK backup-validation workflow only.

Current implementation is report-only. The workflow reads active decommission DTSKs assigned to the backup team, validates backup protection in Cohesity Helios using GET-only API calls, and sends a consolidated management-ready email report.

ServiceNow writeback is not active yet. When writeback is added, it should update only the related DTSK record. It should not update the parent CR or unrelated ServiceNow records.

Validation covers FS, VM, Hyper-V, Nutanix/AHV, SQL, and Oracle. NAS backups are excluded from this server decommission validation.

The email report includes executive summary, backup type summary, detailed backup evidence, and clear handling for No Backup Found and DB Only / No Server Backup cases.

Next step: attach the received workflow emails to this JIRA as test evidence and continue validating the report output before enabling any DTSK writeback.

---

## Attachment Note

Attached emails are provided as validation evidence for the current report-only workflow. These emails confirm the generated DTSK backup-validation output and will be used to review formatting, backup-status interpretation, and readiness for future DTSK-only ServiceNow writeback.
