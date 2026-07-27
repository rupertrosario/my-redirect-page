# Cohesity Workflow Names and Descriptions

| Existing Workflow | Recommended Workflow Name | Description |
|---|---|---|
| Cohesity_Capacity_Garbage_Report-DailyTwice | Cohesity Cluster Health Summary | Validates Cohesity cluster availability, critical services, hardware status, capacity, protection health, and active alerts. Consolidates results across all clusters into a summarized health report. |
| Cohesity_Backup_Failures-4Hourly | Cohesity Backup Failure Summary | Consolidates backup failure details across Cohesity clusters, groups failures by severity and category, removes duplicate alerts, and provides a summarized operational view for investigation. |
| Cohesity_Health_Check_TwiceDaily_8AM_8PM | Cohesity Cluster Health Summary | Validates Cohesity cluster availability, critical services, hardware status, capacity, protection health, and active alerts. Consolidates results across all clusters into a summarized health report. |
| Cohesity_Cohesity User, Groups and Roles Report - Weekly | Cohesity Access and Role Review | Generates a consolidated report of Cohesity users, groups, assigned roles, and permissions across all clusters for access review and audit compliance. |
| Cohesity_AD_Summary_WeeklyMon | Cohesity Active Directory Integration Summary | Reports configured Active Directory domains, connectivity status, authentication configuration, and integration-related issues across Cohesity clusters. |
| Cohesity_Agent_Summary_WeeklyMon | Cohesity Agent Health and Version Summary | Reports registered Cohesity agents, including version, connectivity, host status, upgrade requirements, and communication issues. |
| Cohesity_Long_Running_Backup_Report-Daily | Cohesity Long-Running Backup Report | Identifies backup runs exceeding the defined duration threshold and reports the cluster, protection group, source, start time, elapsed duration, and current status. |
| Cohesity_Helios_Scheduled_Reports_Info_WeeklyOnce | Cohesity Helios Scheduled Reports Inventory | Generates an inventory of configured Helios reports, including report name, recipients, status, and associated clusters. |
| Cohesity_Nutanix_Inventory_Daily-Test | Cohesity Nutanix Protection Inventory | Consolidates Nutanix inventory protected by Cohesity, including clusters, virtual machines, protection assignments, and protection status. |
| Cohesity_Paused_PGs_Report-Daily | Cohesity Paused Protection Groups Report | Identifies paused protection groups across all Cohesity clusters and reports the cluster, protection group, pause status, and last protection activity. |
| Cohesity_Policy_Details | Cohesity Protection Policy Inventory | Generates a consolidated inventory of protection policies, including backup frequency, retention, replication, archival, and associated protection groups. |
| Cohesity_PWD_Compliance_Report-Monthly | Cohesity Password Compliance Report | Reviews Cohesity user password compliance and configured security settings to support governance and audit requirements. |
| Cohesity_View_Summary-WeeklyMon | Cohesity View Capacity and Protection Summary | Reports Cohesity Views, including capacity consumption, protocol configuration, protection status, and cluster association. |
| DTSK_Backup_Status_Check_Report-4Hourly-Test | DTSK Backup Status Exception Report | Checks DTSK backup status across all clusters, identifies failed, missed, or incomplete protection activity, and produces a consolidated exception report. |
