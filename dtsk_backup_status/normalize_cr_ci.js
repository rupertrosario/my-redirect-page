import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  // This script runs inside the per-CR sub-workflow.
  // The parent workflow passes exactly one CR number into this execution.
  const crNumber = input().crNumber;

  // get_cis runs once for this CR and returns only this CR's CI records.
  const getCis = await result("get_cis");

  let cis = getCis;

  // Handle ServiceNow/Dynatrace wrappers if the response is not already an array.
  if (!Array.isArray(cis)) {
    cis = cis?.result ?? cis?.body?.result ?? [];
  }

  const output = [];

  // Preserve the current CR number with every CI sys_id so the next task
  // always knows exactly which CR each CI belongs to.
  for (const ci of cis) {
    const value = ci?.ci_item?.value;

    if (value) {
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
