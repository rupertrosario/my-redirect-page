# Copilot Prompt — Backup Exception Request Form v1.0

Use this prompt in Excel Copilot to generate the interim Backup Exception Request Form.

```text
Create an Excel workbook named Backup_Exception_Request_Form_v1.0.xlsx.

Purpose:
Create a clean Excel-based Backup Exception Request Form to be completed and attached to an incident until the formal STEM process is finalized.

Workbook structure:
1. Sheet 1 name: Request_Form_v1.0
2. Sheet 2 name: Dropdown_Values
3. Sheet 3 name: Instructions

Sheet 1 layout:
Create a professional form layout, not a tracker.
Use section headers, good spacing, wide input cells, wrap text, and enough row height for users to enter details.
Use light shading for editable input cells and darker shading for section headers.
Lock the field-name cells and keep only input/value cells editable.

Title:
Backup Exception Request Form v1.0

Subtitle:
Complete this form and attach it to the incident for backup exclusion or retention extension requests.

Section 1: Request Information
Create the following fields with clear input cells:
- Ticket Number *
- Request Type *
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

Section 2: Exception Details
Create the following fields with larger text-entry areas:
- Scope Type *
- Scope Details *
- Requested Exception *
- Business Reason *
- Backup Team Review
- Impact / Risk Notes
- Expiry / Review Date
- Comments

Dropdown values:
Request Type:
- Backup Exclusion
- Retention Extension

Environment:
- PROD
- Non-PROD
- DR
- Test

Scope Type:
- Server
- VM
- Database
- File
- Folder
- Disk
- NAS Path

Owner Approval:
- Pending
- Approved
- Rejected

Status:
- New
- Under Review
- Approved
- Implemented
- Closed

Instructions sheet:
Add brief instructions:
- Complete all mandatory fields marked with *.
- Owner approval is mandatory.
- Approval Email / Evidence should contain pasted approval email text, email attachment reference, or incident attachment reference.
- Scope Details must clearly identify the CI or protected object so the Backup team can map it to the correct backup job, policy, or copy.
- Attach the completed form to the incident.

Formatting requirements:
- Add borders around all form sections.
- Use merged section headers.
- Use wrap text for long-entry fields.
- Increase row height for Scope Details, Requested Exception, Business Reason, Approval Email / Evidence, Backup Team Review, Impact / Risk Notes, and Comments.
- Freeze the top title row.
- Add data validation dropdowns for Request Type, Environment, Scope Type, Owner Approval, and Status.
- Add conditional formatting to highlight mandatory blank fields.
- Protect the sheet after setup, but allow editing of input cells only.
``` 
