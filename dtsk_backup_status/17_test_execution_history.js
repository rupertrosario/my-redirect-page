// ==========================================================
// Dynatrace JS Task
// Purpose: Read execution history for ONE specific workflow only.
// Read-only proof of concept.
// Required permission: automation:workflows:read
// ==========================================================

import { executionsClient } from "@dynatrace-sdk/client-automation";

// Replace with the exact UUID of the child workflow you want to inspect.
const TARGET_WORKFLOW_ID = "PUT_CHILD_WORKFLOW_UUID_HERE";

export default async function () {
  if (!TARGET_WORKFLOW_ID || TARGET_WORKFLOW_ID === "PUT_CHILD_WORKFLOW_UUID_HERE") {
    throw new Error("Replace TARGET_WORKFLOW_ID with the child workflow UUID before running.");
  }

  const history = await executionsClient.getExecutions({
    workflow: [TARGET_WORKFLOW_ID],
    state: ["SUCCESS"],
    limit: 5
  });

  return {
    targetWorkflowId: TARGET_WORKFLOW_ID,
    history
  };
}
