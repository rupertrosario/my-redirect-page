import { execution, result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  // This script runs inside the per-CR sub-workflow.
  // The parent workflow passes exactly one CR number into the sub-workflow.
  const ex = await execution();
  const crNumber = ex.input?.crNumber;

  // get_cis runs once for this CR and returns only this CR's CI records.
  let cis = await result("get_cis");

  // Normally get_cis should already be an array.
  // Keep these fallbacks in case ServiceNow/Dynatrace wraps the response.
  if (!Array.isArray(cis)) {
    cis = cis?.result ?? cis?.body?.result ?? [];
  }

  // Add the current CR number to every CI sys_id so downstream tasks
  // always retain the CR -> CI relationship.
  const output = cis
    .map(ci => ({
      crNumber,
      value: ci?.ci_item?.value
    }))
    .filter(item => item.crNumber && item.value);

  // Example output:
  // [
  //   { crNumber: "CR0566689", value: "<ci_sys_id_1>" },
  //   { crNumber: "CR0566689", value: "<ci_sys_id_2>" }
  // ]
  return output;
}
