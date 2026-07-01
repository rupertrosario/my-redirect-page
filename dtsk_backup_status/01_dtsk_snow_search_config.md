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

## Notes

- This step is GET/search only.
- Do not request `work_notes`.
- Do not use `sys_journal_field` for the current report-only version.
- This output feeds `dtsk_prepare_work_items`.
