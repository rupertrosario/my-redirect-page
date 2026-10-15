// ==========================================================
// Dynatrace JS Task
// Purpose: Proof-of-concept to verify whether a workflow can
// read previous workflow execution history using the Automation SDK.
//
// Run this in a NEW test workflow. Run the workflow at least twice.
// The second run should be able to see the first completed execution
// if execution history is available to the workflow.
//
// Required permission/scope: automation:workflows:read
// ==========================================================

import { executionsClient } from "@dynatrace-sdk/client-automation";

export default async function ({ executionId }) {
  const currentExecution = await executionsClient.getExecution({
    id: executionId
  });

  const history = await executionsClient.getExecutions({
    limit: 20
  });

  // Keep the response mostly untouched for this first test so we can
  // confirm the exact schema returned by YOUR Dynatrace tenant before
  // writing the final CR-filtering logic.
  return {
    currentExecutionId: executionId,
    currentExecution,
    history
  };
}
