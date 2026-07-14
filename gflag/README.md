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

## Current design

- One Outlook email trigger
- One condition with two subject checks joined by **OR**
- One SharePoint folder
- One SharePoint **Create file** action
- Report type identified in the filename
- Attachments are not copied

## Observed email subjects

The actual report subjects follow these patterns:

```text
105142 - Cohesity Custer Common GFlag Report - Jul 14, 2026
105142 - Cohesity Custer Specific GFlag Report - Jul 14, 2026
```

The leading number and trailing date can change. The condition therefore matches only the stable middle text:

```text
Custer Common GFlag Report
Custer Specific GFlag Report
```

## Prerequisites

1. The SharePoint folder already exists:

   ```text
   Documents/Cohesity GFlag Reports
   ```

2. The Power Automate connection account can create files in this folder.
3. The GFlag emails arrive in an Outlook mailbox folder visible in Outlook on the web.
4. For a shared mailbox, the Power Automate connection account has permission to access it.

## Build the flow

### 1. Create the flow

1. Open **Power Automate**.
2. Select **Create**.
3. Select **Automated cloud flow**.
4. Enter the flow name:

   ```text
   Copy Cohesity GFlag Emails to SharePoint
   ```

5. Select the correct Outlook trigger:
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

- Select the exact email subfolder under **Inbox**.
- If an Outlook rule moves the reports into that subfolder, monitor the subfolder rather than Inbox.
- The folder must be visible in Outlook on the web. Local PST folders cannot be used.

### 3. Add the Condition

1. Select **+ New step**.
2. Add **Control → Condition**.
3. In **Condition parameters**, change **AND** to **OR**.
4. Configure the first row:

   | Field | Value |
   |---|---|
   | Left **Choose a value** | Select **Subject** from Dynamic content |
   | Operator | Select **contains** |
   | Right **Choose a value** | `Custer Common GFlag Report` |

5. Select **Add row**.
6. Configure the second row:

   | Field | Value |
   |---|---|
   | Left **Choose a value** | Select **Subject** from Dynamic content |
   | Operator | Select **contains** |
   | Right **Choose a value** | `Custer Specific GFlag Report` |

The completed condition must be:

```text
Subject contains Custer Common GFlag Report
OR
Subject contains Custer Specific GFlag Report
```

Do not use **AND**. Each report email contains only one report type.

Do not use `Cluster`, `Cluster-Specific`, or a hyphen. The observed subjects use the exact word `Custer` and a space between `Custer` and `Specific`.

### 4. Add SharePoint Create file

Inside the condition's **True** branch:

1. Select **Add an action**.
2. Search for **SharePoint**.
3. Select **Create file**.

Do not select **Create item**. Create item writes a row to a SharePoint List; it does not create an HTML file.

### 5. Select the SharePoint folder

Configure the **Create file** action:

1. **Site Address** → select the SharePoint site containing the document library.
2. **Folder Path** → click the folder icon.
3. Open **Documents**.
4. Open **Cohesity GFlag Reports**.
5. Select that folder.

Both report types are stored in this same folder.

### 6. Configure File Name

Click inside **File Name**, open **fx / Expression**, and paste this exact expression:

```text
concat(if(contains(toLower(triggerOutputs()?['body/subject']),'custer common gflag report'),'Cohesity_Common_GFlags_','Cohesity_Custer_Specific_GFlags_'),formatDateTime(utcNow(),'yyyy-MM-dd_HHmmssfff'),'.html')
```

Example filenames:

```text
Cohesity_Common_GFlags_2026-07-14_103015245.html
Cohesity_Custer_Specific_GFlags_2026-07-14_103020671.html
```

The expression is case-insensitive because it converts the subject to lowercase before matching. The timestamp is UTC and includes milliseconds.

### 7. Configure File Content

1. Click inside **File Content**.
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

### 8. Leave the False branch empty

The condition's **False** branch requires no action. Emails that do not match either report subject are ignored.

## Final flow structure

```text
When a new email arrives in the selected Outlook folder
                    ↓
Subject contains Custer Common OR Custer Specific GFlag Report?
          ┌─────────┴─────────┐
         True                False
          ↓                    ↓
SharePoint — Create file    No action
          ↓
Documents/Cohesity GFlag Reports
```

## Save and test

1. Select **Save**.
2. Select **Test → Manually**.
3. Send, receive, or forward a new Custer Common GFlag report into the monitored Outlook folder.
4. Open **Run history** and confirm the condition followed the **True** branch.
5. Confirm the Common HTML file was created in `Documents/Cohesity GFlag Reports`.
6. Open the file and verify that the complete email body and tables are present.
7. Repeat with a new Custer Specific GFlag report email.
8. Confirm the second file uses the `Cohesity_Custer_Specific_GFlags_` prefix.

Existing emails already present before the flow is enabled should not be used as the primary trigger test. Use a newly delivered or forwarded email.

## Operational notes

- The flow copies the HTML email body only; it does not archive the complete Outlook message.
- The original email remains in Outlook.
- Attachments are not saved.
- Protected or encrypted messages may not expose their body to the connector.
- Inline images using mail-specific `cid:` references may not display after the HTML file is opened from SharePoint.
- Keep the report subject text consistent.
- If the Outlook folder is renamed or moved, update the trigger's Folder selection.
- This file is the canonical instruction set. Future updates must replace this same file rather than create duplicate or dated instruction files.
