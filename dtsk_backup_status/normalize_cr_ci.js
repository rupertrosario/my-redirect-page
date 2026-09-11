import { result } from "@dynatrace-sdk/automation-utils";

export default async function ({ crNumber }) {
  // This script runs inside the per-CR sub-workflow.
  // Configure the script task input as:
  // crNumber = {{ input().crNumber }}
  // This avoids trying to call workflow input APIs from inside JavaScript.

  // get_cis already ran once for this CR and returns only this CR's CI records.
  let cis = await result("get_cis");

  // Handle ServiceNow/Dynatrace wrappers if the response is not already an array.
  if (!Array.isArray(cis)) {
    cis = cis?.result ?? cis?.body?.result ?? [];
  }

  // Add the CR number to every CI sys_id so downstream tasks keep the mapping.
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
