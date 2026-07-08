// ------------------------------------------------------------
// Dynatrace JS action: Team iterations for ServiceNow search
// ------------------------------------------------------------
// Purpose:
// - Read validate_interfaces output
// - Build one Team search item per cluster-level Team candidate
// - Keep Team correlation at cluster level only
//
// ServiceNow search should loop over this output and use:
//   sysparm_query = {{ _.item.query }}
// ------------------------------------------------------------

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const validateResult = await result("validate_interfaces");

  const source =
    Array.isArray(validateResult?.teamIncidents)
      ? validateResult.teamIncidents
      : Array.isArray(validateResult?.teamincident)
        ? validateResult.teamincident
        : Array.isArray(validateResult?.teamIncidentCandidates)
          ? validateResult.teamIncidentCandidates
          : [];

  const teamincident = source.map(function (item) {
    const correlation_id = String(
      item?.correlation_id ||
      item?.CorrelationId ||
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
    teamincident: teamincident,
    teamIncidents: teamincident
  };
}
