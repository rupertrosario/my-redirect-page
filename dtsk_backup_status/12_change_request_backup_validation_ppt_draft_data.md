# Change Request Backup Validation - PPT Draft Data

> Purpose: Slide-ready draft content for enhanced Change Request backup validation.  
> Format: Markdown PPT data only. This is not an actual `.pptx` file.

---

## Slide 1 - Problem Statement & Solution Overview

### Problem Statement

- Backup validation is required before a Change Request is approved or processed.
- Earlier validation provided only basic backup-present or backup-status information.
- The team still had to manually check backup type, object, source, cluster, protection group, and last backup time.
- Manual validation across Cohesity clusters increased effort and delayed Change Request approval.
- Backup gaps were harder to identify early without a detailed validation report.

### Solution Overview

- Enhanced Backup Status Report provides detailed read-only validation.
- Runs through PowerShell and Cohesity Helios GET-only APIs.
- Covers FS, SQL, Oracle, Hyper-V, Nutanix/AHV, and VM backups.
- Captures ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup, and LastBackupTime.
- Generates TXT and CSV outputs that can be attached to the Change Request.
- Helps confirm backup coverage and identify gaps before Change Request approval.

### How It Works

- User provides the Change Request number and CI/server input file.
- PowerShell reads the input and connects to Cohesity Helios using an API key.
- GET-only API calls collect cluster information and search backup objects/protected objects.
- Script applies workload-specific logic for FS, SQL, Oracle, Hyper-V, Nutanix/AHV, and VM results.
- Final TXT and CSV reports are generated for Change Request review and attachment.

---

## Slide 2 - Benefits, Time Savings & Future Enhancements

### Automation Benefits

- Gives richer Change Request validation than basic backup-status output.
- Reduces manual cluster-by-cluster checks.
- Helps identify backup gaps before Change Request approval.
- Supports faster validation decisions before the Change Request is processed.
- Provides consistent TXT and CSV support files for Change Request attachment.
- Keeps validation safe with GET-only/read-only execution.

### Manual vs Enhanced Validation Time Saving

| Criteria | Manual Process | Enhanced Process | Saving |
|---|---:|---:|---:|
| Scope | 1 Change Request / CI | 1 Change Request / CI | Same |
| Cluster coverage | Up to 23 manual checks | PowerShell + API search | Manual lookup removed |
| Time per cluster | 3-5 min | API-driven | Per-cluster effort avoided |
| Time per request | 69-115 min | ~5-10 min review | ~59-110 min saved |
| Output | Basic status | TXT + CSV details | Cleaner attachment support |
| Gap review | Manual interpretation | Backup details shown | Faster action |

### Future Enhancements

- Add standard Change Request summary wording.
- Add clear categories: Backup Found, No Backup Found, DB Only, Server Backup Missing, Validation Error.
- Use output for backup-gap action tracking before Change Request approval.
- Consider workflow integration for recurring or bulk validation.
- Keep automatic Change Request updates out of scope until ServiceNow approval and field mapping are confirmed.

---

## Speaker Notes / Positioning

- This is for Change Request backup validation, not backup-failure incident handling.
- The validation is performed before Change Request approval or processing.
- The implementation uses PowerShell with Cohesity Helios GET-only APIs.
- The key improvement is detailed backup visibility in one report.
- The report shows server, backup type, object, source, cluster, protection group, and last backup time.
- TXT and CSV outputs can be attached to the Change Request as validation support.
- The script remains GET-only and does not modify Cohesity, ServiceNow, or the Change Request.
