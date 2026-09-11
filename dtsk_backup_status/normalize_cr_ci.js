import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  // get_cis runs once for a single CR inside the sub-workflow.
  // Configure get_cis to return these fields from task_ci:
  //   sysparm_fields=task.number,ci_item
  // The dot-walked task.number preserves the CR number in every returned row,
  // while ci_item.value is the CI sys_id required by the next workflow step.
  let cis = await result("get_cis");

  // Normally get_cis should already be an array.
  // Keep these fallbacks in case ServiceNow/Dynatrace wraps the response.
  if (!Array.isArray(cis)) {
    cis = cis?.result ?? cis?.body?.result ?? [];
  }

  // Return only the two values needed downstream:
  //   crNumber = ServiceNow task.number
  //   value    = ServiceNow ci_item sys_id
  const output = cis
    .map(ci => ({
      crNumber: ci?.["task.number"],
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
