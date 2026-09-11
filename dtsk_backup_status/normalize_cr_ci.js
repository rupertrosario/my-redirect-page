import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const validatedCrs = await result("validate_groups");
  const getCis = await result("get_cis");
  const output = [];

  // get_cis loops over validate_groups with concurrency 1, so each
  // get_cis result maps to the CR at the same position in validate_groups.
  for (let i = 0; i < getCis.length; i++) {
    const crNumber = validatedCrs?.[i]?.crNumber;
    let body = getCis[i]?.body ?? getCis[i];

    if (typeof body === "string") {
      body = JSON.parse(body);
    }

    const rows = body?.result ?? [];

    for (const row of rows) {
      const ciSysId = row?.ci_item?.value ?? row?.ci_item;

      if (crNumber && ciSysId) {
        output.push({
          crNumber,
          ciSysId
        });
      }
    }
  }

  return {
    items: output,
    count: output.length
  };
}
