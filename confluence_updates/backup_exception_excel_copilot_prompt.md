# Copilot Prompt — Backup Exception Request Form v1.1

Use this prompt in Excel Copilot to generate the interim Backup Exception Request Form workbook.

```text
Create an Excel workbook named Backup_Exception_Request_Form_v1.1.xlsx.

Purpose:
Create a clean Excel-based Backup Exception Request Form workbook to be completed and attached to an incident until the formal STEM process is finalized.

Workbook structure:
1. Sheet 1 name: Backup_Exclusion_v1.1
2. Sheet 2 name: Retention_Extension_v1.1
3. Sheet 3 name: Legal_Hold_v1.1
4. Sheet 4 name: Summary_Tracker
5. Sheet 5 name: Dropdown_Values
6. Sheet 6 name: Instructions

General formatting for all request tabs:
- Build each request tab as a form, not a long tracker.
- Use section headers, good spacing, wide input cells, wrap text, and enough row height for data entry.
- Use light shading for editable input cells and darker shading for section headers.
- Lock field-name cells and keep only input/value cells editable.
- Mark mandatory fields with *.
- Add borders around all form sections.
- Use merged section headers.
- Freeze the top title row.
- Add conditional formatting to highlight mandatory blank fields.
- Protect each sheet after setup, but allow editing of input cells only.

Sheet 1: Backup_Exclusion_v1.1
Title: Backup Exclusion Request Form v1.1
Subtitle: Complete this form and attach it to the incident for backup exclusion requests.

Section 1: Request Information
- Ticket Number *
- Request Date *
- Requested By Name *
- Requested By Email *
- Environment *
- CI Name *
- Application Name
- Owner Name *
- Owner Email *
- Owner Approval *
- Approval Email / Evidence *
- Status *

Section 2: Exclusion Details
Create larger text-entry areas for:
- Scope Type *
- Scope Details *
- Requested Exclusion *
- Business Reason *
- Backup Team Review
- Impact / Risk Notes
- Expiry / Review Date
- Comments

Sheet 2: Retention_Extension_v1.1
Title: Retention Extension Request Form v1.1
Subtitle: Complete this form and attach it to the incident for retention extension requests.

Section 1: Request Information
- Ticket Number *
- Request Date *
- Requested By Name *
- Requested By Email *
- Environment *
- CI Name *
- Application Name
- Owner Name *
- Owner Email *
- Owner Approval *
- Approval Email / Evidence *
- Status *

Section 2: Retention Details
Create larger text-entry areas for:
- Scope Type *
- Scope Details *
- Current Retention / Expiry Date
- Requested Retention Extension Until *
- Business Reason *
- Backup Team Review
- Impact / Risk Notes
- Review Date *
- Comments

Sheet 3: Legal_Hold_v1.1
Title: Legal Hold Request Form v1.1
Subtitle: Complete this form and attach it to the incident for legal hold or preservation requests.

Section 1: Request Information
- Ticket Number *
- Request Date *
- Requested By Name *
- Requested By Email *
- Environment *
- CI Name *
- Application Name
- Owner Name *
- Owner Email *
- Backup Team Manager Approval *
- Approval Email / Evidence *
- Status *

Section 2: Hold Details
Create larger text-entry areas for:
- Scope Type *
- Scope Details *
- Preservation Reason *
- Hold Start Date *
- Hold Review Date *
- Release Owner
- Backup Team Review
- Impact / Risk Notes
- Comments

Scope Type dropdown values for all request tabs:
- Server
- VM
- Database
- File
- Folder
- Disk
- NAS Path

Environment dropdown values:
- PROD
- Non-PROD
- DR
- Test

Owner Approval dropdown values:
- Pending
- Approved
- Rejected

Backup Team Manager Approval dropdown values:
- Pending
- Approved
- Rejected

Status dropdown values:
- New
- Under Review
- Approved
- Implemented
- Closed

Summary_Tracker sheet:
Create a simple table with filters enabled. This sheet is used only for summary tracking across requests.
Columns:
- Ticket Number
- Request Type
- CI Name
- Environment
- Scope Type
- Owner / Manager Approval
- Status
- Review / Expiry Date
- Last Updated
- Comments

Add dropdown filters to the Summary_Tracker header row.

Dropdown_Values sheet:
Create the dropdown source lists for Environment, Scope Type, Owner Approval, Backup Team Manager Approval, and Status. Hide or protect this sheet after setup.

Instructions sheet:
Add brief instructions:
- Use the correct tab based on request type: Backup Exclusion, Retention Extension, or Legal Hold.
- Complete all mandatory fields marked with *.
- Owner approval is mandatory for Backup Exclusion and Retention Extension.
- Backup Team Manager approval is mandatory for Legal Hold.
- Approval Email / Evidence should contain pasted approval email text, email attachment reference, or incident attachment reference.
- Scope Details must clearly identify the CI or protected object so the Backup team can map it to the correct backup job, policy, or copy.
- Attach the completed workbook or the completed request tab to the incident.
```
