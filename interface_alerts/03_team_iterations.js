// Dynatrace JS action: Team iterations for ServiceNow search
// Purpose: Take validate_interfaces.teamincident[] and add a ServiceNow query per item.
// ServiceNow search is NOT a JS task; it loops over this output and uses loopItemValue.query.

import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const validateExec = await execution("validate_interfaces");
  const validateResult = validateExec?.result || validateExec || {};

  const source = Array.isArray(validateResult.teamincident)
    ? validateResult.teamincident
    : [];

  const teamincident = source.map((item) => {
    const correlation_id = String(
      item.correlation_id ||
      item.CorrelationId ||
      ""
    ).trim();

    return {
      ...item,

      correlation_id: correlation_id,

      query:
        "correlation_id=" +
        correlation_id +
        "^stateNOT IN6,7^ORDERBYDESCsys_updated_on"
    };
  });

  return {
    teamCount: teamincident.length,
    teamincident: teamincident
  };
}
