# Cohesity GFlag Reports — Power Automate

## Purpose

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

- One Outlook trigger
- One condition containing two subject checks joined with **OR**
- One SharePoint folder
- One SharePoint **Create file** action
- Report type retained in the generated filename
- Attachments are not copied

## Prerequisites

1. The SharePoint folder already exists:

   ```text
   Documents/Cohesity GFlag Reports
   ```

2. The Power Automate SharePoint connection account can create files in that folder.
3. The report emails arrive in an Outlook mailbox folder visible in Outlook on the web.
4. For a shared mailbox, the Power Automate connection account has access to that mailbox.

## Build the flow

### 1. Create the flow

1. Open **Power Automate**.
2. Select **Create**.
3. Select **Automated cloud flow**.
4. Flow name:

   ```text
   Copy Cohesity GFlag Emails to SharePoint
   ```

5. Select the appropriate Outlook trigger:
   - Personal mailbox: **When a new email arrives (V3)**
   - Shared mailbox: **When a new email arrives in a shared mailbox (V2)**

### 2. Configure the Outlook trigger

Configure the trigger as follows:

| Field | Value |
|---|---|
| Mailbox Address | Enter the shared mailbox address only when using the shared-mailbox trigger |
| Folder | Use the folder picker and select the actual Inbox subfolder containing the GFlag emails |
| Subject Filter | `GFlag Report` |
| Only with Attachments | `No` |
| Include Attachments | `No` |

Important:

- Select the exact Outlook folder where new reports arrive.
- If an Outlook rule moves messages into a subfolder, the trigger must monitor that subfolder, not Inbox.
- Do not use **Body Preview** later in the flow.

### 3. Add the report-subject condition

1. Select **+ New step**.
2. Add **Control → Condition**.
3. At the top of the condition parameters, change **AND** to **OR**.
4. Configure the first row:

   | Field | Selection |
   |---|---|
   | Left **Choose a value** | Select **Subject** from Dynamic content |
   | Operator | Select **contains** |
   | Right **Choose a value** | Enter `Cohesity Common GFlag Report` |

5. Select **Add row**.
6. Configure the second row:

   | Field | Selection |
   |---|---|
   | Left **Choose a value** | Select **Subject** from Dynamic content |
   | Operator | Select **contains** |
   | Right **Choose a value** | Enter `Cohesity Cluster-Specific GFlag Report` |

The finished condition must read:

```text
Subject contains Cohesity Common GFlag Report
OR
Subject contains Cohesity Cluster-Specific GFlag Report
```

Do not use **AND**. A single email subject cannot normally contain both complete report names, so an AND condition would send valid report emails to the False branch.

### 4. Add SharePoint Create file

Inside the condition's **True** branch:

1. Select **Add an action**.
2. Add **SharePoint → Create file**.
3. Configure:

| Field | Value |
|---|---|
| Site Address | Select the required SharePoint site |
| Folder Path | Use the picker to select `Documents/Cohesity GFlag Reports` |
| File Name | Use the expression below |
| File Content | Select Outlook **Body** from Dynamic content |

#### File Name expression

Select the **File Name** field, open **Expression**, and paste:

```text
concat(
  if(
    contains(triggerOutputs()?['body/subject'],'Cohesity Common GFlag Report'),
    'Cohesity_Common_GFlags_',
    'Cohesity_Cluster_Specific_GFlags_'
  ),
  formatDateTime(utcNow(),'yyyy-MM-dd_HHmmssfff'),
  '.html'
)
```

Example output files:

```text
Cohesity_Common_GFlags_2026-07-14_071530125.html
Cohesity_Cluster_Specific_GFlags_2026-07-14_071545472.html
```

The timestamp is UTC. Milliseconds are included to reduce filename collisions.

#### File Content

Choose:

```text
Body
```

Do not choose:

```text
Body Preview
```

`Body Preview` can be truncated and may omit report tables or other HTML content.

### 5. Leave the False branch empty

The condition's **False** branch requires no action. Non-matching messages are ignored.

## Final flow structure

```text
When a new email arrives in the selected Outlook folder
                    ↓
Subject contains Common OR Cluster-Specific GFlag Report?
          ┌─────────┴─────────┐
         True                False
          ↓                    ↓
SharePoint — Create file    No action
          ↓
Documents/Cohesity GFlag Reports
```

## Test procedure

1. Save and enable the flow.
2. Send or receive a new **Cohesity Common GFlag Report** email in the monitored Outlook folder.
3. Open **Run history** and confirm the condition selected **True**.
4. Confirm the HTML file was created in:

   ```text
   Documents/Cohesity GFlag Reports
   ```

5. Open the file and verify the complete body and tables are present.
6. Repeat with a new **Cohesity Cluster-Specific GFlag Report** email.
7. Confirm the second file uses the Cluster-Specific filename prefix.

Existing emails already present before the flow is enabled should not be used as the primary trigger test. Use a newly delivered report email.

## Operational notes

- The flow saves the email body only; it does not archive the complete Outlook message.
- Attachments are not saved.
- Protected or encrypted emails may not expose their body to the Outlook connector.
- Inline images referenced through mail-specific `cid:` links may not render after the HTML file is opened from SharePoint.
- Keep the expected report subject text consistent.
- If the Outlook folder is renamed or moved, update the trigger's Folder selection.
- Use this file as the canonical instructions. Future changes should replace this file instead of creating dated copies.

## Microsoft references

- Office 365 Outlook connector: https://learn.microsoft.com/en-us/connectors/office365/
- SharePoint connector: https://learn.microsoft.com/en-us/connectors/sharepointonline/
- Workflow expression functions: https://learn.microsoft.com/en-us/azure/logic-apps/expression-functions-reference
