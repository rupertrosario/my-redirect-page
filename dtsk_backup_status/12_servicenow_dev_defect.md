# ServiceNow DEV DTSK Automation Defect

## Short Description

DTSK automation - CR Required/state update and work-note retrieval not working in DEV

## Description

During testing of the Dynatrace workflow integration with ServiceNow DEV for decommission DTSK processing, updating `work_notes` on the DTSK is working successfully.

The following operations are not working as expected:

- Unable to update **CR Required** with the required Yes/No value.
- Unable to update the DTSK **state**.
- Unable to retrieve existing DTSK work notes from `sys_journal_field`.
- `assigned_to` has not yet been tested.

These functions are being tested as part of the DTSK backup-validation workflow so that the validation result can be recorded on the task and the DTSK can be updated appropriately based on the validation outcome.

## Work Notes

DEV testing completed so far:

- `work_notes` update on `x_alfi_decom_decom_task` - **Successful**
- CR Required Yes/No update - **Not working**
- DTSK state update - **Not working**
- `assigned_to` update - **Not yet tested**
- Read existing work notes from `sys_journal_field` - **Not working**

The DTSK record is being addressed using its `sys_id` on:

`x_alfi_decom_decom_task`

For retrieving existing work notes, the query being tested against `sys_journal_field` is:

`element_id=<DTSK_SYS_ID>^element=work_notes`

The purpose of reading the existing work notes is to identify previous workflow updates and avoid adding duplicate notes when the workflow is rerun.

Please review why CR Required and state are not being updated and why existing work notes cannot be retrieved from `sys_journal_field`.

`work_notes` update is confirmed working. `assigned_to` will be tested separately.
