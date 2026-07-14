# Cohesity GFlag Reports — Power Automate

## Objective

Copy the HTML body of either Cohesity GFlag report email from an Outlook mailbox folder into one SharePoint folder.

```text
Outlook mailbox folder
        ↓
Power Automate
        ↓
SharePoint: Documents/Cohesity GFlag Reports
```

This flow copies the email body. It does not move or delete the original Outlook email.

## Observed email subjects

The actual report subjects follow these patterns:

```text
105142 - Cohesity Custer Common GFlag Report - Jul 14, 2026
105142 - Cohesity Custer Specific GFlag Report - Jul 14, 2026
```

The leading number and trailing date can change. The stable subject fragments are:

```text
Custer Common GFlag Report
Custer Specific GFlag Report
```

## Current design

- One Outlook trigger
- No Condition action
- One SharePoint folder
- One SharePoint **Create file** action
- File Name expression identifies Common or Specific
- Attachments are not copied

## Build the flow

### 1. Create the flow

1. Open **Power Automate**.
2. Select **Create → Automated cloud flow**.
3. Flow name:

   ```text
   Copy Cohesity GFlag Emails to SharePoint
   ```

4. Select the correct Outlook trigger:
   - Personal mailbox: **When a new email arrives (V3)**
   - Shared mailbox: **When a new email arrives in a shared mailbox (V2)**

### 2. Configure the Outlook trigger

| Field | Value |
|---|---|
| Mailbox Address | Enter the shared mailbox address only when using the shared-mailbox trigger |
| Folder | Select the Inbox subfolder where the GFlag emails arrive |
| Subject Filter | `GFlag Report` |
| Only with Attachments | `No` |
| Include Attachments | `No` |

Important:

- Select the exact Outlook subfolder containing the reports.
- The folder must be visible in Outlook on the web.
- Local PST folders cannot be used.

### 3. Remove the Condition

Do not add a **Condition** action.

If a Condition already exists:

1. Select the Condition card.
2. Select **... → Delete**.
3. Add the SharePoint action directly below the Outlook trigger.

The trigger already limits the flow to subjects containing `GFlag Report`. The File Name expression below identifies whether the report is Common or Specific.

### 4. Add SharePoint Create file

1. Select **+ → Add an action** directly below the Outlook trigger.
2. Search for **SharePoint**.
3. Select **Create file**.

Do not select **Create item**. Create item writes to a SharePoint List and does not create an HTML file.

### 5. Select the SharePoint folder

Configure **Create file**:

1. **Site Address** → select the required SharePoint site.
2. **Folder Path** → click the folder icon.
3. Open **Documents**.
4. Open **Cohesity GFlag Reports**.
5. Select that folder.

Both report types are stored in this same folder.

### 6. Configure File Name

Click **File Name → fx / Expression** and paste:

```text
concat(if(contains(toLower(coalesce(triggerOutputs()?['body/subject'],'')),'custer common gflag report'),'Cohesity_Custer_Common_GFlags_',if(contains(toLower(coalesce(triggerOutputs()?['body/subject'],'')),'custer specific gflag report'),'Cohesity_Custer_Specific_GFlags_','Cohesity_Unknown_GFlag_')),formatDateTime(utcNow(),'yyyy-MM-dd_HHmmssfff'),'.html')
```

Example filenames:

```text
Cohesity_Custer_Common_GFlags_2026-07-14_103015245.html
Cohesity_Custer_Specific_GFlags_2026-07-14_103020671.html
```

If the subject contains `GFlag Report` but matches neither observed type, the file uses the prefix:

```text
Cohesity_Unknown_GFlag_
```

### 7. Configure File Content

1. Click **File Content**.
2. Open **Dynamic content**.
3. Select **Body** from **When a new email arrives**.

Use:

```text
Body
```

Do not use:

```text
Body Preview
```

Body Preview can be truncated and may omit tables or other HTML content.

## Final flow structure

```text
When a new email arrives in the selected Outlook folder
                    ↓
SharePoint — Create file
                    ↓
Documents/Cohesity GFlag Reports
```

## Save and test

1. Select **Save**.
2. Select **Test → Manually**.
3. Send, receive, or forward a new Custer Common GFlag report into the monitored Outlook folder.
4. Open **Run history**.
5. Confirm **Create file** succeeded.
6. Confirm the filename starts with `Cohesity_Custer_Common_GFlags_`.
7. Repeat with a Custer Specific GFlag report.
8. Confirm the second filename starts with `Cohesity_Custer_Specific_GFlags_`.
9. Open both HTML files and verify the complete email body and tables are present.

## Operational notes

- The original email remains in Outlook.
- The flow copies only the HTML email body.
- Attachments are not saved.
- Inline images using mail-specific `cid:` references may not render from SharePoint.
- If the Outlook folder is renamed or moved, update the trigger's Folder selection.
- This file is the canonical instruction set. Future updates must overwrite this same file.
