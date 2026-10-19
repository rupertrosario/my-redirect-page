# ServiceNow DEV DTSK Automation Defect

## Defect title

ServiceNow DEV DTSK automation – unable to update CR Required/state or read work notes

## Description

During testing of the Dynatrace workflow integration with ServiceNow DEV for automated decommission DTSK processing, the integration is able to update the DTSK `work_notes` field successfully.

However, the following operations are currently not working:

- Unable to update CR Required with the required Yes/No value
- Unable to update the DTSK state
- Unable to retrieve existing work notes from `sys_journal_field`
- `assigned_to` / assignment update has not yet been tested

The integration is using the DTSK record `sys_id` and the decommission task table:

```text
x_alfi_decom_decom_task
```

The `work_notes` update works successfully against the DTSK record, confirming that the workflow is able to update the record.

## Current behaviour

```text
work_notes        → Update successful
CR Required       → Update unsuccessful
state             → Update unsuccessful
assigned_to       → Not yet tested
sys_journal_field → Unable to retrieve existing work_notes
```

For reading existing work notes, the query is being made against:

```text
sys_journal_field
```

using:

```text
element_id=<DTSK_SYS_ID>^element=work_notes
```

## Expected behaviour

The Dynatrace workflow should be able to:

1. Update `work_notes`
2. Update CR Required with the appropriate Yes/No value
3. Update the DTSK state as applicable
4. Update `assigned_to` when required
5. Read existing `work_notes` from `sys_journal_field`

These operations are part of the planned automated DTSK backup-validation workflow.

## Impact

At present, the workflow can successfully add the backup validation result to `work_notes`, but it cannot complete the remaining DTSK updates such as CR Required and state.

The workflow is also unable to read the existing work notes from `sys_journal_field`, which is needed to identify previous workflow updates and avoid adding duplicate work notes during reruns.

## Request

Please review the ServiceNow DEV configuration, integration-user permissions, ACLs, field-level access and any applicable business rules for the following:

### Table: `x_alfi_decom_decom_task`

Fields:

- `work_notes`
- CR Required
- `state`
- `assigned_to`

### Table: `sys_journal_field`

Read fields:

- `element_id`
- `element`
- `value`

`work_notes` write/update is currently confirmed as working.

CR Required and state updates are currently not working.

Reading existing `work_notes` from `sys_journal_field` is currently not working.

`assigned_to` has not yet been tested.
