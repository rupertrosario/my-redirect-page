# Change Request Backup Validation - PPT Draft Data

> Purpose: Slide-ready draft content for enhanced Change Request backup validation.  
> Format: Markdown PPT data only. This is not an actual `.pptx` file.

---

## Slide 1 - Problem Statement & Solution Overview

### Problem Statement

- Backup validation is required before a Change Request is approved or processed.
- Earlier validation provided only basic backup-present or backup-status information.
- Backup type, source mapping, cluster, protection group, and last backup time were not always clear in one view.
- Limited detail created ambiguity and follow-up review effort for unclear or gap-related validation cases.
- Backup gaps were harder to identify early without a detailed validation report.

### Solution Overview

- Enhanced Backup Status Report provides detailed read-only validation through PowerShell.
- Uses Cohesity Helios GET-only APIs to collect cluster information and search backup objects/protected objects.
- Covers FS, SQL, Oracle, Hyper-V, Nutanix/AHV, and VM backups.
- Captures ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup, and LastBackupTime.
- Generates TXT and CSV outputs that can be attached to the Change Request.
- Helps confirm backup coverage and identify gaps before Change Request approval.

---

## Slide 2 - Benefits, Time Savings & Future Enhancements

### Automation Benefits

- Improves CR validation from basic backup-status output to detailed backup coverage reporting.
- Uses PowerShell with Cohesity Helios GET-only APIs for safe read-only validation.
- Helps identify backup gaps before Change Request approval.
- Reduces ambiguity where backup exists but backup type, source mapping, cluster, protection group, or latest backup time is unclear.
- Provides clearer details for validation decisions before the Change Request is processed.
- Supports faster follow-up only for unclear or gap-related backup validation cases.

### Earlier vs Enhanced Validation Efficiency

| Criteria | Earlier Script-Based Process | Enhanced CR Backup Validation | Estimated Benefit |
|---|---|---|---|
| Validation method | Basic backup-status script output | Enhanced PowerShell + Cohesity Helios GET-only API validation | Improved validation clarity |
| Successful backup cases | Shows backup is present/successful | Shows backup is present with object, source, cluster, protection group, and last backup time | No major time saving; ambiguity reduced |
| Ambiguous backup cases | Backup may exist, but the relationship or details may not be clear | Backup type, source mapping, cluster, protection group, and last backup time are clearly shown | ~5-15 minutes saved per ambiguous CI |
| Backup-gap review | Missing or partial coverage may require additional checking | Backup gaps are easier to identify before Change Request approval | Faster validation decision |
| Output detail | Limited backup-status detail | ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup, LastBackupTime | Less interpretation required |

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
- Clear successful backup cases do not have major direct time saving; the main benefit is better detail and reduced ambiguity.
- Time saving applies mainly to ambiguous or gap-related CIs where follow-up review would otherwise be required.
- Estimated efficiency gain: number of ambiguous CIs x 5-15 minutes.
- The script remains GET-only and does not modify Cohesity, ServiceNow, or the Change Request.
