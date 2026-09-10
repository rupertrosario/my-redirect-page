import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const crs = await result("get_crs");
  const groups = await result("get_requested_groups");
  const BACKUP_GROUP_SYS_ID = "PUT_BACKUP_GROUP_SYS_ID_HERE";

  const matches = [];

  for (let i = 0; i < (groups || []).length; i++) {
    const groupList = groups[i] || [];

    const isBackupRequested = groupList.some(
      row => row?.assignment_group?.value === BACKUP_GROUP_SYS_ID
    );

    if (!isBackupRequested) continue;

    const cr = crs?.[i] || {};
    const crNumber =
      cr?.number?.display_value ||
      cr?.number?.value ||
      cr?.number ||
      "";

    if (crNumber) {
      matches.push({ crNumber });
    }
  }

  return matches;
}
