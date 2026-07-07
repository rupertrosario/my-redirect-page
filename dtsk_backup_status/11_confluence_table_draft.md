# DTSK Backup Status - Confluence Table Draft

> Purpose: Brief Confluence-ready table for the DTSK Backup Status automation.  
> Format: Single field/value table for copy/paste into Confluence.

| Field | Details |
|---|---|
| Automation Theme | DTSK backup status validation for server decommission requests. |
| Problem Statement | Backup team currently validates decommission DTSKs manually by checking CI/server backup status across Cohesity clusters. |
| Current Impact | Manual validation is repetitive and slow because each CI may need to be checked across 23 clusters. This can delay DTSK handling and increases the chance of missing newly created DTSKs. |
| Purpose | Automate recurring DTSK backup-status validation so the team gets a clean report and can focus on exceptions. |
| Automation Solution | Dynatrace workflow fetches active decommission DTSKs, extracts CI/server details, validates backup status in Cohesity Helios using GET-only APIs, and sends an email report every 5 hours daily. |
| Tools Used | Dynatrace Workflow, ServiceNow DTSK table, Cohesity Helios API, email notification. |
| Estimated Effort to Complete Automation | Initial report-only workflow completed. Future ServiceNow writeback/closure requires approved access, field confirmation, idempotency testing, and state-change validation. |
| Work Performed By | Backup automation / backup operations team. |
| Expected Savings and Benefits | Manual effort per run: 23 clusters x 3-5 minutes = ~69-115 minutes. Automated effort per run: ~5 minutes review. Saving per run: ~64-110 minutes. Automation frequency: every 5 hours daily, approximately 144 runs/month. If the same cadence was handled manually, monthly manual effort would be ~166-276 hours/month; automated review effort is ~12 hours/month; estimated potential avoidance is ~154-264 hours/month. Broader DTSK volume estimate: 2,390 DTSKs/year x 3-5 minutes = ~120-200 hours/year potential. Benefits: removes repeated 23-cluster lookup, reduces missed DTSKs, gives consistent recurring status reporting, separates confirmed backup/missing backup/DB-only/validation-error cases, and keeps current phase safe with read-only reporting. |
| Current Status | Report-only automation is in place. No ServiceNow writeback, no work_notes update, no state change, and no auto-closure in current phase. |
| Comments / Future Enhancement | ServiceNow idea/request raised for controlled update access: work_notes update, duplicate-note check, eligible CR Required = No, state change, and auto-close for approved cases, especially VM DTSKs. Physical server unregister/removal remains pending for later enhancement. |
