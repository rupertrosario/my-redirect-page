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
sys_id,number,short_description,state,sys_created_on,assignment_group.name,assigned_to.name,sla_due,due_date,made_sla,u_backup_assignment_time,u_assigned_to_backup_on,u_assignment_group_assigned_on,assignment_group_assigned_on,decom_request.number,decom_request.ci_name.name
```

## SLA rule

Correct SLA for this report is the 2-day SLA from when the DTSK was assigned to the Backup group.

Preferred source:

```text
sla_due
```

Use `sla_due` if ServiceNow already calculates it from Backup-group assignment time.

Fallback source if `sla_due` is not available:

```text
u_backup_assignment_time,u_assigned_to_backup_on,u_assignment_group_assigned_on,assignment_group_assigned_on
```

The prepare task adds 2 days to the first populated assignment timestamp and calculates:

```text
SLA Due
SLA Status = Within SLA | Breached SLA | SLA Missing
```

If the actual DTSK assignment timestamp has a different ServiceNow field name, add that field to the Fields list and to `02_dtsk_prepare_work_items.js`.

## Notes

- This step is GET/search only.
- Do not request `work_notes`.
- Do not use `sys_journal_field` for the current report-only version.
- This output feeds `dtsk_prepare_work_items`.
