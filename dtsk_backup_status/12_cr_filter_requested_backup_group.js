import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const crs = await result("get_crs");
  const groups = await result("get_requested_groups");
  const BACKUP_GROUP_SYS_ID = "PUT_BACKUP_GROUP_SYS_ID_HERE";

  const matches = [];

  for (let i = 0; i < (groups || []).length; i++) {
    const groupList = groups[i] || [];
    const cr = crs?.[i] || {};

    for (const row of groupList) {
      if (row?.assignment_group?.value === BACKUP_GROUP_SYS_ID) {
        matches.push({
          crSysId: row?.parent?.value || "",
          crNumber: cr?.number?.value || cr?.number || "",
          groupSysId: row?.assignment_group?.value || ""
        });
      }
    }
  }

  return matches;
}
