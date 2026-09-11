import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  // CRs returned by validate_groups.
  // Example: [{ crNumber: "CR0566689" }, { crNumber: "CR0566670" }]
  const crs = await result("validate_groups");

  // get_cis runs once per CR and returns a nested array:
  // get_cis[0] = all CI records for crs[0]
  // get_cis[1] = all CI records for crs[1]
  // and so on.
  const ciGroups = await result("get_cis");

  const output = [];

  // Preserve CR -> CI mapping by matching both arrays by index.
  for (let i = 0; i < ciGroups.length; i++) {
    const crNumber = crs?.[i]?.crNumber;

    let cis = ciGroups[i];

    // Normally each get_cis iteration is already an array.
    // Keep these fallbacks in case Dynatrace/ServiceNow wraps it in result/body.result.
    if (!Array.isArray(cis)) {
      cis = cis?.result ?? cis?.body?.result ?? [];
    }

    // Extract only the ServiceNow CI sys_id value needed by the next workflow step.
    for (const ci of cis) {
      const value = ci?.ci_item?.value;

      if (crNumber && value) {
        output.push({
          crNumber,
          value
        });
      }
    }
  }

  // Final output is a flat list ready for the next workflow:
  // [{ crNumber: "CR0566689", value: "<ci sys_id>" }, ...]
  return output;
}
