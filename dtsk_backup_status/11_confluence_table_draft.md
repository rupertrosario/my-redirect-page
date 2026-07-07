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
| Expected Savings and Benefits | Removes repeated cluster-by-cluster lookup across 23 Cohesity clusters; runs every 5 hours daily to reduce missed DTSKs; provides consistent recurring backup-status reporting; separates confirmed backup, missing backup, DB-only backup, and validation-error cases; keeps the first phase safe with read-only reporting; creates a path for future ServiceNow writeback and eligible auto-closure. Estimated potential avoidance at same 5-hour cadence: ~154-264 hours/month. Broader DTSK volume estimate: ~120-200 hours/year. |
| Current Status | Report-only automation is in place. No ServiceNow writeback, no work_notes update, no state change, and no auto-closure in current phase. |
| Comments / Future Enhancement | ServiceNow idea/request raised for controlled update access: work_notes update, duplicate-note check, eligible CR Required = No, state change, and auto-close for approved cases, especially VM DTSKs. Physical server unregister/removal remains pending for later enhancement. |

---

## Manual vs Automation Time Saving

| Criteria | Manual Process | Automated Process | Estimated Saving |
|---|---:|---:|---:|
| Item scope | 1 DTSK / CI | 1 DTSK / CI | Same scope |
| Cluster coverage | 23 clusters checked manually | 23 clusters checked by workflow | Removes manual cluster lookup |
| Time per cluster | 3-5 minutes per cluster | API-driven | Manual per-cluster effort avoided |
| Frequency | Manual/on-demand or monthly review | Scheduled every 5 hours daily | Reduces missed DTSKs |
| Time per run | 23 clusters x 3-5 minutes = 69-115 minutes | ~5 minutes review | ~64-110 minutes/run |
| Estimated runs per month | Not practical manually at 5-hour cadence | ~144 scheduled runs/month | Continuous coverage without manual effort |
| Monthly effort if done manually at same cadence | ~166-276 hours/month | ~12 hours/month review | ~154-264 hours/month potential avoidance |
| Broader DTSK volume estimate | 2,390 DTSKs/year x 3-5 minutes | Automated validation/update path | ~120-200 hours/year potential |

---

## Benefits Summary

- Reduces manual backup validation effort for decommission DTSKs.
- Removes repeated cluster-by-cluster lookup across 23 Cohesity clusters.
- Runs every 5 hours daily to reduce the risk of missing newly created DTSKs.
- Gives a consistent recurring backup-status view for DTSK handling.
- Clearly separates confirmed backup, missing backup, DB-only backup, and validation-error cases.
- Helps the team focus on exception handling instead of routine status checks.
- Keeps the first phase safe by using read-only validation and email reporting only.
- Creates a controlled path for future ServiceNow updates once permissions are approved.
- Supports future eligible auto-closure, especially for VM-related decommission DTSKs.

---

## Short Version for Confluence Page

The current automation validates decommission DTSK backup status using a read-only Dynatrace workflow. It checks active DTSKs, validates CI/server protection from Cohesity Helios, and sends a clean email report every 5 hours daily. Manual checking across 23 clusters would take ~69-115 minutes per run, while automated review is estimated at ~5 minutes per run. At the same 5-hour cadence, estimated potential manual effort avoided is ~154-264 hours/month. Broader annual DTSK volume estimate remains ~120-200 hours/year based on 2,390 DTSKs/year and 3-5 minutes per DTSK.
