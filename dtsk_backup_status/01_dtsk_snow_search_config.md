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
sys_id,number,short_description,state,sys_created_on,assignment_group.name,assigned_to.name,due_date,sla_due,made_sla,u_sla,u_sla_due,sla,decom_request.number,decom_request.ci_name.name
```

## SLA field note

The prepare task checks these ServiceNow fields in order and uses the first populated value:

```text
u_sla,u_sla_due,sla,sla_due,due_date,made_sla
```

If the actual DTSK SLA column has a different ServiceNow field name, add that field to the Fields list and to `02_dtsk_prepare_work_items.js`.

## Notes

- This step is GET/search only.
- Do not request `work_notes`.
- Do not use `sys_journal_field` for the current report-only version.
- This output feeds `dtsk_prepare_work_items`.
