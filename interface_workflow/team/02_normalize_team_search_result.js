// Dynatrace JS action: Normalize ServiceNow Team incident search output.
import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  var iterationExec = await execution("for_each_team_iteration");
  var searchExec = await execution("search_team_incident");
  var iterationResult = iterationExec && iterationExec.result ? iterationExec.result : (iterationExec || {});
  var rawSearchResult = searchExec && searchExec.result ? searchExec.result : (searchExec || {});

  function unwrapCandidate(value) {
    if (!value) return {};
    return value.candidate || value.item || value.value || value.currentItem || value;
  }

  function getRows(searchResult) {
    if (!searchResult) return [];
    if (Array.isArray(searchResult.records)) return searchResult.records;
    if (Array.isArray(searchResult.result)) return searchResult.result;
    if (searchResult.body) {
      if (Array.isArray(searchResult.body.result)) return searchResult.body.result;
      if (Array.isArray(searchResult.body.records)) return searchResult.body.records;
      if (typeof searchResult.body === "string") {
        try {
          var parsed = JSON.parse(searchResult.body);
          if (Array.isArray(parsed.result)) return parsed.result;
          if (Array.isArray(parsed.records)) return parsed.records;
        } catch (error) {
          console.log("ServiceNow search body was not valid JSON.");
        }
      }
    }
    return [];
  }

  var candidate = unwrapCandidate(iterationResult);
  var rows = getRows(rawSearchResult);
  var first = rows[0] || {};
  var sysId = first.sys_id || "";
  if (rows.length > 0 && !sysId) {
    throw new Error("Team search matched a record without sys_id; refusing to create a duplicate.");
  }

  return {
    Type: "TEAM",
    ClusterName: candidate.ClusterName || "",
    ClusterId: candidate.ClusterId || "",
    CorrelationId: candidate.CorrelationId || "",
    ShortDescription: candidate.ShortDescription || "",
    matchedCount: rows.length,
    action: rows.length > 0 ? "update" : "create",
    sys_id: sysId,
    existingNumber: first.number || "",
    candidate: candidate,
    searchRows: rows
  };
}
