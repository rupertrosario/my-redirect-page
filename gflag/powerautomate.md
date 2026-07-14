# Power Automate — Cohesity GFlag Email Copy

## Purpose

Copy Cohesity Cluster Common and Cluster Specific GFlag report emails from Outlook into SharePoint as HTML files.

The original Outlook email is not moved or deleted.

## Observed subject patterns

```text
105142 - Cohesity Cluster Common GFlag Report - Jul 14, 2026
105142 - Cohesity Cluster Specific GFlag Report - Jul 14, 2026
```

The number and date can change. The stable subject fragments are:

```text
Cluster Common GFlag Report
Cluster Specific GFlag Report
```

## Flow structure

```text
Office 365 Outlook trigger
        ↓
SharePoint — Create file
        ↓
Documents/Cohesity GFlag Reports
```

A separate Condition action is not required because the trigger filters emails containing `GFlag Report`. The File Name expression identifies whether the report is Common or Specific.

## 1. Outlook trigger

Use the **Office 365 Outlook** connector.

- Personal mailbox: **When a new email arrives (V3)**
- Shared mailbox: **When a new email arrives in a shared mailbox (V2)**

Configure:

| Field | Value |
|---|---|
| Folder | Select the Outlook Inbox subfolder containing the reports |
| Subject Filter | `GFlag Report` |
| Only with Attachments | `No` |
| Include Attachments | `No` |

For the shared-mailbox trigger, also enter the shared mailbox address.

## 2. SharePoint Create file

Add **SharePoint → Create file** directly below the Outlook trigger.

Do not select **Create item**.

Configure:

| Field | Value |
|---|---|
| Site Address | Select the required SharePoint site |
| Folder Path | `Documents/Cohesity GFlag Reports` |

## 3. File Name

Open **File Name → Expression** and paste:

```text
concat(if(contains(toLower(coalesce(triggerOutputs()?['body/subject'],'')),'cluster common gflag report'),'Cohesity_Cluster_Common_GFlags_',if(contains(toLower(coalesce(triggerOutputs()?['body/subject'],'')),'cluster specific gflag report'),'Cohesity_Cluster_Specific_GFlags_','Cohesity_Unknown_GFlag_')),formatDateTime(utcNow(),'yyyy-MM-dd_HHmmssfff'),'.html')
```

Example output:

```text
Cohesity_Cluster_Common_GFlags_2026-07-14_103015245.html
Cohesity_Cluster_Specific_GFlags_2026-07-14_103020671.html
```

## 4. File Content

In **File Content**, select **Body** from the Outlook trigger.

Do not select **Body Preview** because it can be truncated.

## 5. Save and test

1. Save the flow.
2. Select **Test → Manually**.
3. Send or forward a new Cluster Common report into the monitored folder.
4. Confirm **Create file** succeeds.
5. Repeat with a Cluster Specific report.
6. Open both HTML files in SharePoint and verify the full tables are present.

Existing emails already in the folder do not normally trigger the flow. Use a newly delivered or forwarded email.

## Optional weekly Monday schedule

For a separate scheduled flow, use **Schedule → Recurrence**:

| Field | Value |
|---|---|
| Frequency | `Week` |
| Interval | `1` |
| On these days | `Monday` |
| Time zone | `India Standard Time` |

Equivalent five-field cron for Monday at midnight:

```cron
0 0 * * 1
```
