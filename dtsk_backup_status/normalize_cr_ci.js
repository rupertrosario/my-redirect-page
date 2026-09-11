import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const crs = await result("validate_groups");
  const ciGroups = await result("get_cis");

  const output = [];

  for (let i = 0; i < ciGroups.length; i++) {
    const crNumber = crs?.[i]?.crNumber;

    let cis = ciGroups[i];

    if (!Array.isArray(cis)) {
      cis = cis?.result ?? cis?.body?.result ?? [];
    }

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

  return output;
}
