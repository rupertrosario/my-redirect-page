// ==========================================================
// Dynatrace JS Task
// Task name: dtsk_validate_one_ci
// Phase: Loop wiring test only
//
// Purpose:
// - Verify the Dynatrace loop task passes one DTSK work item per execution
// - Verify dtsk_get_cluster_map is visible to this task
// - Does NOT call Cohesity protected-object search yet
// - Replace this file with full validation logic after loop wiring is confirmed
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";

export default async function (input = {}) {

  // ========================================================
  // Expected input when configuring the loop task:
  // {
  //   "workItem": {{ _.item }}
  // }
  // ========================================================

  const clusterData = await result("dtsk_get_cluster_map");

  const workItem =
    input?.workItem ||
    input?.loopItemValue ||
    input?.item ||
    null;

  const output = {
    phase: "LOOP_WIRING_TEST",
    loopItemReceived: Boolean(workItem),
    workItem: workItem || null,
    dtsk: workItem?.dtsk || "N/A",
    ciName: workItem?.ciName || "N/A",
    aliases: Array.isArray(workItem?.aliases) ? workItem.aliases : [],
    clustersLoaded: clusterData?.summary?.clustersLoaded || 0,
    clusterMapEntries: clusterData?.summary?.clusterMapEntries || 0,
    message: "If loopItemReceived=true and ciName is populated, loop wiring is working."
  };

  console.log("==== DTSK VALIDATE ONE CI - LOOP WIRING TEST ====");
  console.log(JSON.stringify(output, null, 2));

  return output;
}
