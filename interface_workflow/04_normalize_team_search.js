// Dynatrace JS action: Normalize Team ServiceNow search results
// Current workflow:
//   get_alerts -> validate_interfaces -> snow_search_team -> normalize_team_search
//
// Purpose:
// - Read Team incident candidates from validate_interfaces.teamIncidents[]
// - Read looped ServiceNow results from snow_search_team
// - Split Team items into create/update/no-write arrays
//
// Expected cases per Team item:
// - 0 active incidents found => createTeamIncidents[]
// - 1 active incident found  => updateTeamIncidents[]
// - 2+ active incidents      => noWriteTeamIncidents[] safety guard

import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function getExecResult(execObj) {
    if (!execObj) return {};
    return execObj.result || execObj;
  }

  function extractLoopResults(searchResult) {
    if (!searchResult) return [];

    // Some Dynatrace looped actions return an array directly.
    if (Array.isArray(searchResult)) return searchResult;

    // Common wrapper names for looped task results.
    if (Array.isArray(searchResult.results)) return searchResult.results;
    if (Array.isArray(searchResult.loopResults)) return searchResult.loopResults;
    if (Array.isArray(searchResult.executions)) return searchResult.executions;
    if (Array.isArray(searchResult.items)) return searchResult.items;

    // If not looped, treat the single result as one search result.
    return [searchResult];
  }

  function extractRecords(searchItem) {
    if (!searchItem) return [];

    // ServiceNow table API common shapes.
    if (Array.isArray(searchItem.result)) return searchItem.result;
    if (Array.isArray(searchItem.records)) return searchItem.records;

    // Dynatrace / HTTP / connector wrappers.
    if (searchItem.body) {
      if (Array.isArray(searchItem.body.result)) return searchItem.body.result;
      if (Array.isArray(searchItem.body.records)) return searchItem.body.records;
    }

    if (searchItem.response) {
      if (Array.isArray(searchItem.response.result)) return searchItem.response.result;
      if (Array.isArray(searchItem.response.records)) return searchItem.response.records;
      if (searchItem.response.body) {
        if (Array.isArray(searchItem.response.body.result)) return searchItem.response.body.result;
        if (Array.isArray(searchItem.response.body.records)) return searchItem.response.body.records;
      }
    }

    if (searchItem.output) {
      if (Array.isArray(searchItem.output.result)) return searchItem.output.result;
      if (Array.isArray(searchItem.output.records)) return searchItem.output.records;
      if (searchItem.output.body) {
        if (Array.isArray(searchItem.output.body.result)) return searchItem.output.body.result;
        if (Array.isArray(searchItem.output.body.records)) return searchItem.output.body.records;
      }
    }

    return [];
  }

  try {
    const validateExec = await execution("validate_interfaces");
    const validateResult = getExecResult(validateExec);

    const searchExec = await execution("snow_search_team");
    const searchResult = getExecResult(searchExec);

    const teamCandidates = Array.isArray(validateResult.teamIncidents)
      ? validateResult.teamIncidents
      : [];

    const searchLoopResults = extractLoopResults(searchResult);

    const createTeamIncidents = [];
    const updateTeamIncidents = [];
    const noWriteTeamIncidents = [];

    for (let i = 0; i < teamCandidates.length; i++) {
      const candidate = teamCandidates[i] || {};
      const searchItem = searchLoopResults[i] || {};
      const records = extractRecords(searchItem);

      const correlation_id = norm(candidate.correlation_id || candidate.CorrelationId);

      if (!correlation_id) {
        noWriteTeamIncidents.push({
          ...candidate,
          action: "no_write",
          reason: "missing_correlation_id",
          matchedCount: records.length
        });
        continue;
      }

      if (records.length === 0) {
        createTeamIncidents.push({
          ...candidate,
          correlation_id: correlation_id,
          action: "create",
          reason: "no_active_incident_found",
          matchedCount: 0
        });
        continue;
      }

      if (records.length === 1) {
        const inc = records[0] || {};

        updateTeamIncidents.push({
          ...candidate,
          correlation_id: correlation_id,
          action: "update",
          reason: "one_active_incident_found",
          matchedCount: 1,
          sys_id: inc.sys_id || "",
          number: inc.number || "",
          existing_state: inc.state || "",
          existing_short_description: inc.short_description || "",
          existing_correlation_id: inc.correlation_id || "",
          existing_updated_on: inc.sys_updated_on || ""
        });
        continue;
      }

      noWriteTeamIncidents.push({
        ...candidate,
        correlation_id: correlation_id,
        action: "no_write",
        reason: "duplicate_active_incidents_found",
        matchedCount: records.length,
        matchedIncidents: records.map(function (r) {
          r = r || {};
          return {
            sys_id: r.sys_id || "",
            number: r.number || "",
            state: r.state || "",
            correlation_id: r.correlation_id || ""
          };
        })
      });
    }

    return {
      ok: true,
      inputTeamCount: teamCandidates.length,
      searchLoopCount: searchLoopResults.length,
      createCount: createTeamIncidents.length,
      updateCount: updateTeamIncidents.length,
      noWriteCount: noWriteTeamIncidents.length,
      createTeamIncidents: createTeamIncidents,
      updateTeamIncidents: updateTeamIncidents,
      noWriteTeamIncidents: noWriteTeamIncidents
    };

  } catch (e) {
    return {
      ok: false,
      error: String(e && e.message ? e.message : e),
      inputTeamCount: 0,
      searchLoopCount: 0,
      createCount: 0,
      updateCount: 0,
      noWriteCount: 0,
      createTeamIncidents: [],
      updateTeamIncidents: [],
      noWriteTeamIncidents: []
    };
  }
}
