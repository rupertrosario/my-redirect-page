# Cohesity GFlag Settings Collection, Reporting, and SharePoint Storage

## Overview

GFlags are configurable Cohesity settings used to control system behaviour, enable features, tune performance, and apply support-recommended workarounds.

This read-only process collects the current GFlag settings from the managed Cohesity clusters, compares the settings across clusters, generates two email reports, and stores the received reports in SharePoint through Power Automate.

## GFlag Settings Collected

| Setting | Description |
|---|---|
| Cluster | Cohesity cluster from which the setting was retrieved |
| Service | Cohesity service associated with the GFlag |
| GFlag | Name of the configuration setting |
| Value | Current configured value |
| Reason | Available reason or source for the setting |
| Applied Time | Available timestamp displayed in Eastern Time |

The process does not create, modify, or remove any GFlag setting.

## Reports Generated

| Report | Description |
|---|---|
| Common GFlag Report | Settings where the Service, GFlag, and Value are identical across all successfully queried clusters |
| Cluster-Specific GFlag Report | Settings that are specific to individual clusters or have different values across the successfully queried clusters |

## Schedule

| Frequency | Day | Time | Time Zone |
|---|---|---|---|
| Every two weeks | Monday | 9:00 AM | Eastern Time (ET) |

## End-to-End Process

| Step | Process | Result |
|---|---|---|
| 1 | The scheduled workflow starts every two weeks on Monday at 9:00 AM ET | GFlag collection begins |
| 2 | Current GFlag settings are retrieved from the managed Cohesity clusters | Cluster, Service, GFlag, Value, Reason, and Applied Time are collected |
| 3 | Service, GFlag, and Value are compared across successfully queried clusters | Settings are classified as Common or Cluster-Specific |
| 4 | The Common and Cluster-Specific reports are generated | Two report outputs are produced |
| 5 | Both reports are sent to the designated Outlook mailbox | The reports are available by email |
| 6 | Power Automate detects each email with `GFlag Report` in the subject | The SharePoint file process starts automatically |
| 7 | Power Automate copies the complete email Body into an HTML file | The report content and tables are preserved |
| 8 | The HTML file is created in the configured SharePoint folder | The report is available for future reference |

## Power Automate and SharePoint Storage

Power Automate starts only after a GFlag report email reaches the configured Outlook mailbox folder.

| Power Automate Setting | Configuration |
|---|---|
| Trigger | When a new email arrives in the selected Outlook folder |
| Subject Filter | `GFlag Report` |
| Attachments | Not required and not copied |
| SharePoint Action | Create file |
| File Content | Full Outlook email `Body` |
| Destination | `Documents/Cohesity GFlag Reports` |
| Source Email | Remains unchanged in Outlook |

Both report types are stored in the same SharePoint folder as HTML files. The filename identifies whether the report is Common or Cluster-Specific and includes a timestamp to prevent duplicate filenames.

Example filenames:

```text
Cohesity_Custer_Common_GFlags_YYYY-MM-DD_HHmmssfff.html
Cohesity_Custer_Specific_GFlags_YYYY-MM-DD_HHmmssfff.html
```

## Links

| Resource | Link |
|---|---|
| SharePoint GFlag Report Location | [Add SharePoint link here](ADD_SHAREPOINT_URL_HERE) |
| Outlook GFlag Report Folder | [Add Outlook folder link here](ADD_OUTLOOK_FOLDER_URL_HERE) |

## Images

### Example GFlag Report Email

Replace the placeholder below with a screenshot of the Common or Cluster-Specific report email.

![Example GFlag report email](ADD_EMAIL_REPORT_IMAGE_HERE)

### SharePoint Report Location

Replace the placeholder below with a screenshot showing the SharePoint folder and saved HTML reports.

![SharePoint GFlag report location](ADD_SHAREPOINT_LOCATION_IMAGE_HERE)

## Operational Notes

- The GFlag collection is read-only.
- Power Automate runs whenever a matching report email arrives.
- The full email `Body` must be used; `Body Preview` can be truncated.
- The original report emails remain in Outlook.
- Changes to the Outlook folder, email subject, or SharePoint location must also be updated in Power Automate.
