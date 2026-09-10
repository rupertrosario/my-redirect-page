import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const groups = await result("get_requested_groups");
  const BACKUP_GROUP_SYS_ID = "PUT_BACKUP_GROUP_SYS_ID_HERE";

  const matches = [];

  for (const groupList of groups || []) {
    for (const row of groupList || []) {
      if (row?.assignment_group?.value === BACKUP_GROUP_SYS_ID) {
        matches.push({
          crSysId: row?.parent?.value || "",
          groupSysId: row?.assignment_group?.value || ""
        });
      }
    }
  }

  return matches;
}
