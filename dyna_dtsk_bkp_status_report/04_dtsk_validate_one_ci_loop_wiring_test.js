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
//
// Dynatrace loop settings for this task:
// - Item variable name: workItem
// - List: {{ result("dtsk_prepare_work_items")["workItems"] }}
// - Concurrency: 1 initially
//
// Important:
// - This JS task has no separate input parameter box.
// - The loop item is injected directly into the JS using {{ _.workItem }}.
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {

  const clusterData = await result("dtsk_get_cluster_map");

  // This comes from the loop item variable name configured on the task.
  // If your item variable name is changed from workItem to item, replace this with:
  // const workItem = {{ _.item }};
  const workItem = {{ _.workItem }};

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
