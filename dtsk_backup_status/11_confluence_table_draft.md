# DTSK Backup Status - Confluence Table Draft

> Purpose: Brief Confluence-ready table for the DTSK Backup Status automation.  
> Format: Markdown table draft for copy/paste into Confluence.

| Field | Details |
|---|---|
| Automation Theme | DTSK backup status validation for server decommission requests |
| Problem Statement | Backup team currently validates decommission DTSKs manually by checking CI/server backup status across Cohesity clusters. |
| Current Impact | Manual validation is repetitive, slow across 23 clusters, and can delay DTSK handling or miss newly created DTSKs. |
| Purpose | Automate recurring backup-status validation so the team gets a clean report and can focus on exceptions. |
| Automation Solution | Dynatrace workflow fetches active decommission DTSKs, extracts CI/server details, validates backup status in Cohesity Helios using GET-only APIs, and sends an email report every 5 hours daily. |
| Tools Used | Dynatrace Workflow, ServiceNow DTSK table, Cohesity Helios API, email notification. |
| Estimated Effort to Complete Automation | Initial report-only workflow completed; future ServiceNow writeback/closure depends on approved access, field confirmation, and testing. |
| Work Performed By | Backup automation / backup operations team. |
| Expected Savings and Benefits | Effort per DTSK: 3-5 minutes manual validation/update. Annual DTSK volume: ~2,390. Monthly task volume: ~199 DTSKs/month. Monthly effort saved: ~597-995 minutes/month, equal to ~10-17 hours/month. Annual effort saved: ~7,170-11,950 minutes/year, equal to ~120-200 hours/year. Additional benefit: every-5-hour schedule reduces missed DTSKs and gives consistent status reporting. |
| Current Status | Report-only automation is in place. No ServiceNow writeback, no work_notes update, no state change, and no auto-closure in current phase. |
| Comments / Future Enhancement | ServiceNow idea/request raised for controlled update access: work_notes update, duplicate-note check, eligible CR Required = No, state change, and auto-close for approved cases, especially VM DTSKs. Physical server unregister/removal remains pending for later enhancement. |

---

## Savings Calculation

| Metric | Value |
|---|---:|
| Manual effort per DTSK | 3-5 minutes |
| Annual DTSK volume | ~2,390 DTSKs/year |
| Monthly DTSK volume | ~199 DTSKs/month |
| Monthly effort saved | ~597-995 minutes/month |
| Monthly effort saved | ~10-17 hours/month |
| Annual effort saved | ~7,170-11,950 minutes/year |
| Annual effort saved | ~120-200 hours/year |
| Automation run frequency | Every 5 hours daily |

---

## Short Version for Confluence Page

The current automation validates decommission DTSK backup status using a read-only Dynatrace workflow. It checks active DTSKs, validates CI/server protection from Cohesity Helios, and sends a clean email report every 5 hours daily. Based on ~2,390 DTSKs/year and 3-5 minutes of manual effort per DTSK, estimated effort avoided is ~120-200 hours/year. The current phase does not update ServiceNow. Future enhancement will depend on approved ServiceNow access for work_notes, state changes, CR Required updates, and eligible auto-closure.
