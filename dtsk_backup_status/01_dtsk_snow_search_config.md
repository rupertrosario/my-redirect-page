# 01 - dtsk_snow_search

Task type: ServiceNow search / get records

## Table

```text
x_alfi_decom_decom_task
```

## Query

Use this to include assigned and unassigned DTSKs:

```text
assignment_group.name=<BACKUP_GROUP_NAME>^stateNOT IN6,7
```

Use this only if the report should include assigned DTSKs only:

```text
assignment_group.name=<BACKUP_GROUP_NAME>^assigned_toISNOTEMPTY^stateNOT IN6,7
```

## Fields

```text
sys_id,number,short_description,state,sys_created_on,assignment_group.name,assigned_to.name,decom_request.number,decom_request.ci_name.name
```

## SLA rule

The report does not depend on ServiceNow SLA fields.

Correct SLA for this report is calculated in Dynatrace:

```text
SLA Start  = time DTSK came to the Backup group
SLA Due    = SLA Start + 2 days
SLA Status = Within SLA | Breached SLA | SLA Missing
```

For the current report-only implementation, `sys_created_on` is used as the DTSK Backup-group start timestamp because these DTSKs are searched from the Backup assignment group queue. If the task can be reassigned to Backup later than creation, the workflow must be extended to fetch the group-assignment timestamp from ServiceNow audit/history or a dedicated field.

## Notes

- This step is GET/search only.
- Do not request `work_notes`.
- Do not use `sys_journal_field` for the current report-only version.
- This output feeds `dtsk_prepare_work_items`.
