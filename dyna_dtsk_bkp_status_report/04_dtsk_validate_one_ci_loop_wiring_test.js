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
// - Do NOT paste workflow expressions like {{ _.workItem }} inside the JS code.
// - Dynatrace validates the JS before expression substitution, so {{ }} causes a parse error.
// - The loop item should be available to the action input/runtime based on the configured item variable name.
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";

export default async function (input = {}) {

  const clusterData = await result("dtsk_get_cluster_map");

  // With loop item variable name = workItem, Dynatrace should pass the loop item
  // through the action input/runtime. This fallback also supports cases where
  // the runtime passes the item itself as input.
  const workItem =
    input?.workItem ||
    input?.item ||
    input?.loopItem ||
    input?.loopItemValue ||
    (input?.dtsk && input?.ciName ? input : null) ||
    null;

  const output = {
    phase: "LOOP_WIRING_TEST",
    inputType: typeof input,
    inputKeys: input && typeof input === "object" ? Object.keys(input) : [],
    loopItemReceived: Boolean(workItem),
    workItem: workItem || null,
    dtsk: workItem?.dtsk || "N/A",
    ciName: workItem?.ciName || "N/A",
    aliases: Array.isArray(workItem?.aliases) ? workItem.aliases : [],
    clustersLoaded: clusterData?.summary?.clustersLoaded || 0,
    clusterMapEntries: clusterData?.summary?.clusterMapEntries || 0,
    message: "If loopItemReceived=true and ciName is populated, loop wiring is working. If false, check the task Input tab for the exact loop item key."
  };

  console.log("==== DTSK VALIDATE ONE CI - LOOP WIRING TEST ====");
  console.log(JSON.stringify(output, null, 2));

  return output;
}
