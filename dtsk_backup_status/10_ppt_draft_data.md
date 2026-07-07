# DTSK Backup Status - PPT Draft Data

> Purpose: Initial slide-ready draft content for the DTSK Backup Status automation presentation.  
> Format: Markdown PPT data only. This is not an actual `.pptx` file.

---

## Slide 1 - Problem Statement & Solution Overview

### Problem Statement

- Decommission DTSKs require backup validation before the backup team can provide status.
- Current validation is manual and requires checking the CI/server across multiple Cohesity clusters.
- With 23 clusters, manual lookup is time-consuming and repetitive for each DTSK/CI.
- Manual checks can lead to inconsistent updates, missed backup types, or delayed DTSK handling.
- The team spends effort validating routine cases instead of focusing on exceptions.
- Current phase has no ServiceNow writeback, no work_notes, no state change, and no automatic closure.
- Physical server unregister/removal handling is still pending and should remain a future enhancement.

### Solution Overview

- Build a Dynatrace workflow to fetch active ServiceNow decommission DTSKs.
- Extract the DTSK number, CI/server name, assignment details, and decommission request details.
- Validate backup status in Cohesity Helios using GET-only/read-only API checks.
- Check in-scope backup types: FS, VM, Hyper-V, Nutanix/AHV, SQL, and Oracle.
- Exclude NAS backups from this server decommission validation.
- Generate a clean email report with executive summary, backup type summary, details, No Backup Found, and DB Only / No Server Backup cases.
- Keep the current version report-only until ServiceNow update permissions and field rules are approved.

---

## Slide 2 - Automation Benefits, Time Savings & Future Enhancements

### Automation Benefits

- Reduces manual backup validation effort for decommission DTSKs.
- Removes repeated cluster-by-cluster lookup across 23 Cohesity clusters.
- Gives a consistent monthly backup-status view for DTSK handling.
- Clearly separates confirmed backup, missing backup, DB-only backup, and validation-error cases.
- Helps the team focus on exception handling instead of routine status checks.
- Keeps the first phase safe by using read-only validation and email reporting only.
- Creates a controlled path for future ServiceNow updates once permissions are approved.
- Supports future eligible auto-closure, especially for VM-related decommission DTSKs.

### Manual vs Automated Time Saving

| Criteria | Manual Process | Automated Process | Estimated Saving |
|---|---:|---:|---:|
| Item scope | 1 DTSK / CI | 1 DTSK / CI | Same scope |
| Cluster coverage | 23 clusters checked manually | 23 clusters checked by workflow | Removes manual cluster lookup |
| Time per cluster/check | 3-5 minutes | API-driven | Manual effort avoided |
| Frequency | Monthly | Monthly | Same cadence |
| Time per monthly run | 69-115 minutes | ~5 minutes review | ~64-110 minutes/month |
| Annual saving for monthly run | ~13-22 hours/year | Review only | ~13-22 hours/year |
| Broader DTSK volume estimate | 2,390 DTSKs/year x 3-5 minutes | Automated validation/update path | ~120-200 hours/year potential |

### Future Enhancements

- A ServiceNow idea/request has been raised to allow controlled DTSK update access.
- Requested future capabilities include updating DTSK work_notes, reading existing notes to avoid duplicates, setting eligible CR Required = No, and changing state only for approved scenarios.
- Future writeback should use a separate task after report validation, not inside the Cohesity validation loop.
- Auto-close should be limited to confirmed and eligible cases only.
- Uncertain cases such as NoObject, validation errors, or DB-only/no-server-backup should not be auto-closed.
- Physical server unregister/removal from backup should remain a later controlled enhancement after ownership, permissions, and process rules are confirmed.

---

## Speaker Notes / Positioning

- This is the initial draft for management discussion.
- The current automation is report-only and safe because it does not update ServiceNow or Cohesity.
- The next step depends on ServiceNow approval for update permissions, work_notes, state change, CR Required updates, and idempotency checks.
- VM decommission DTSKs are the best first candidates for controlled auto-closure because backup validation is more direct compared with physical and DB-only scenarios.
