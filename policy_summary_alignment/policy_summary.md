# DTSK Backup Status - Policy Summary

## Summary
The DTSK backup-status automation is currently positioned as a report-only validation workflow. It fetches active decommission DTSKs, validates backup protection in Cohesity using GET-only API calls, and sends an email/report output for review.

## Current policy alignment
| Area | Current position | Alignment status |
|---|---|---|
| ServiceNow read | Reads active decommission DTSKs only | Aligned for report-only validation |
| Cohesity API | Uses GET-only validation | Aligned; no Cohesity write action |
| Email/report | Sends consolidated backup evidence | Aligned for review/evidence |
| ServiceNow work notes | Not enabled in current phase | Pending approval/enhancement |
| DTSK state change | Not enabled in current phase | Pending approval/enhancement |
| Automatic closure | Not enabled in current phase | Pending approval/enhancement |
| CI removal from backup | Not implemented in current phase | Future enhancement/manual review required |

## Current scope
- ServiceNow DTSK search.
- CI/server extraction.
- Cohesity backup-status validation.
- FS, VM, Hyper-V, Nutanix/AHV, SQL, and Oracle validation.
- NAS excluded from server decommission validation.
- Email/report output only.

## Explicit exclusions in current phase
- No parent CR update.
- No unrelated CTASK/DTSK update.
- No ServiceNow `work_notes` update.
- No automatic DTSK state update.
- No automatic closure.
- No Cohesity delete/unprotect/remove action.

## Future enhancement
A ServiceNow idea/enhancement is tracked to allow controlled future updates for eligible DTSKs only, including:
- approved test path for prototype validation
- DTSK `work_notes` update with validation evidence
- duplicate-note prevention
- eligible `CR Required = No` update
- eligible state update after approval
- future CI removal from backup handling where applicable

## Validation note
Current output should be treated as operational validation evidence, not final audit evidence, until the workflow output and writeback controls are approved.