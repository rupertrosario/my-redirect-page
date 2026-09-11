import { execution, result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  // This script now runs inside the per-CR sub-workflow.
  // The parent workflow passes exactly one CR into each sub-workflow execution.
  const ex = await execution();
  const workflowInput = await ex.input();
  const crNumber = workflowInput?.crNumber;

  // get_cis is NOT looped in the sub-workflow.
  // It runs once for the current CR and returns only that CR's CI records.
  let cis = await result("get_cis");

  // Normally get_cis should already be an array.
  // Keep these fallbacks in case ServiceNow/Dynatrace wraps the response.
  if (!Array.isArray(cis)) {
    cis = cis?.result ?? cis?.body?.result ?? [];
  }

  const output = [];

  // Preserve the current CR number with every CI sys_id so the next task
  // always knows exactly which CR each CI belongs to.
  for (const ci of cis) {
    const value = ci?.ci_item?.value;

    if (crNumber && value) {
      output.push({
        crNumber,
        value
      });
    }
  }

  // Example output:
  // [
  //   { crNumber: "CR0566689", value: "<ci_sys_id_1>" },
  //   { crNumber: "CR0566689", value: "<ci_sys_id_2>" }
  // ]
  return output;
}
