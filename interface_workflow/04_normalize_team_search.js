// Dynatrace JS action: Normalize Team ServiceNow search results
// Current workflow:
//   get_alerts -> validate_interfaces -> snow_search_team -> normalize_team_search
//
// IMPORTANT:
// - snow_search_team is a looped ServiceNow Search task.
// - normalize_team_search is NOT looped.
// - This task pairs validate_interfaces.teamIncidents[index]
//   with snow_search_team loop result[index].
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

  function isPlainObject(v) {
    return v !== null && typeof v === "object" && !Array.isArray(v);
  }

  function keysOf(v) {
    return isPlainObject(v) ? Object.keys(v).sort() : [];
  }

  function unwrapExecution(v) {
    if (!v) return {};

    // Dynatrace execution() usually exposes task output under .result.
    if (v.result !== undefined) return v.result;

    // Keep the original object if there is no .result wrapper.
    return v;
  }

  function findArrayByName(obj, names, maxDepth) {
    if (!obj || maxDepth < 0) return null;

    if (Array.isArray(obj)) return null;
    if (!isPlainObject(obj)) return null;

    for (const n of names) {
      if (Array.isArray(obj[n])) return obj[n];
    }

    for (const k of Object.keys(obj)) {
      const child = obj[k];
      if (isPlainObject(child)) {
        const found = findArrayByName(child, names, maxDepth - 1);
        if (found) return found;
      }
    }

    return null;
  }

  function getTeamCandidates(validateExecRaw, validateResult) {
    const names = [
      "teamIncidents",
      "teamIncident",
      "teamincident",
      "team_incidents"
    ];

    const fromResult = findArrayByName(validateResult, names, 3);
    if (fromResult) return fromResult;

    const fromRaw = findArrayByName(validateExecRaw, names, 4);
    if (fromRaw) return fromRaw;

    return [];
  }

  function looksLikeSnowRecordArray(arr) {
    if (!Array.isArray(arr)) return false;
    if (arr.length === 0) return true;

    const first = arr[0];
    if (!isPlainObject(first)) return false;

    return (
      first.sys_id !== undefined ||
      first.number !== undefined ||
      first.correlation_id !== undefined ||
      first.short_description !== undefined ||
      first.state !== undefined
    );
  }

  function extractRecords(searchItem) {
    if (!searchItem) return [];

    // Sometimes the item itself is already the ServiceNow result array.
    if (looksLikeSnowRecordArray(searchItem)) return searchItem;

    if (!isPlainObject(searchItem)) return [];

    // ServiceNow Table API common shape.
    if (looksLikeSnowRecordArray(searchItem.result)) return searchItem.result;
    if (looksLikeSnowRecordArray(searchItem.records)) return searchItem.records;

    // Dynatrace / HTTP / connector wrappers.
    const wrappers = ["body", "response", "output", "data", "value"];

    for (const w of wrappers) {
      const child = searchItem[w];
      if (!child) continue;

      if (looksLikeSnowRecordArray(child)) return child;

      if (isPlainObject(child)) {
        if (looksLikeSnowRecordArray(child.result)) return child.result;
        if (looksLikeSnowRecordArray(child.records)) return child.records;

        if (child.body) {
          if (looksLikeSnowRecordArray(child.body)) return child.body;
          if (isPlainObject(child.body)) {
            if (looksLikeSnowRecordArray(child.body.result)) return child.body.result;
            if (looksLikeSnowRecordArray(child.body.records)) return child.body.records;
          }
        }
      }
    }

    return [];
  }

  function looksLikeLoopItem(v) {
    if (!isPlainObject(v)) return false;

    if (v.loopItemValue !== undefined) return true;
    if (v.iteration !== undefined) return true;
    if (v.item !== undefined && (v.result !== undefined || v.output !== undefined || v.body !== undefined)) return true;
    if (v.executionId !== undefined && (v.result !== undefined || v.output !== undefined)) return true;
    if (v.taskExecutionId !== undefined && (v.result !== undefined || v.output !== undefined)) return true;

    // A ServiceNow search loop item can also just be an object that contains result/body/output.
    if (v.result !== undefined || v.records !== undefined || v.body !== undefined || v.response !== undefined || v.output !== undefined) return true;

    return false;
  }

  function pickBestLoopArray(candidates, expectedCount) {
    if (!candidates.length) return [];

    // Prefer an array matching teamIncidents count.
    for (const c of candidates) {
      if (expectedCount > 0 && c.arr.length === expectedCount) return c.arr;
    }

    // Then prefer arrays whose members look like loop/search results.
    for (const c of candidates) {
      if (c.arr.some(looksLikeLoopItem)) return c.arr;
    }

    return candidates[0].arr;
  }

  function collectArrays(obj, path, out, maxDepth) {
    if (!obj || maxDepth < 0) return;

    if (Array.isArray(obj)) {
      out.push({ path: path, arr: obj });
      return;
    }

    if (!isPlainObject(obj)) return;

    for (const k of Object.keys(obj)) {
      collectArrays(obj[k], path ? path + "." + k : k, out, maxDepth - 1);
    }
  }

  function getSearchLoopResults(searchExecRaw, searchResult, expectedCount) {
    // If execution().result itself is an array, this is the normal loop wrapper.
    if (Array.isArray(searchResult)) return searchResult;

    const commonNames = [
      "iterations",
      "loopResults",
      "loopExecutions",
      "taskExecutions",
      "executionResults",
      "results",
      "items"
    ];

    const direct = findArrayByName(searchResult, commonNames, 2);
    if (direct && !looksLikeSnowRecordArray(direct)) return direct;

    const directRaw = findArrayByName(searchExecRaw, commonNames, 3);
    if (directRaw && !looksLikeSnowRecordArray(directRaw)) return directRaw;

    const all = [];
    collectArrays(searchResult, "result", all, 4);
    collectArrays(searchExecRaw, "raw", all, 5);

    // Exclude ServiceNow record arrays from being treated as loop arrays unless this is a single expected item.
    const loopCandidates = all.filter(function (c) {
      if (!Array.isArray(c.arr)) return false;
      if (expectedCount === 1 && looksLikeSnowRecordArray(c.arr)) return true;
      return !looksLikeSnowRecordArray(c.arr);
    });

    const picked = pickBestLoopArray(loopCandidates, expectedCount);
    if (picked.length) return picked;

    // Non-loop/single-result fallback.
    if (isPlainObject(searchResult) && (searchResult.result !== undefined || searchResult.records !== undefined || searchResult.body !== undefined || searchResult.output !== undefined)) {
      return [searchResult];
    }

    return [];
  }

  function searchItemDebug(searchItem) {
    const records = extractRecords(searchItem);
    return {
      keys: keysOf(searchItem),
      recordCount: records.length,
      firstRecordKeys: records.length ? keysOf(records[0]) : []
    };
  }

  try {
    const validateExecRaw = await execution("validate_interfaces");
    const validateResult = unwrapExecution(validateExecRaw);

    const searchExecRaw = await execution("snow_search_team");
    const searchResult = unwrapExecution(searchExecRaw);

    const teamCandidates = getTeamCandidates(validateExecRaw, validateResult);
    const searchLoopResults = getSearchLoopResults(searchExecRaw, searchResult, teamCandidates.length);

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
      noWriteTeamIncidents: noWriteTeamIncidents,

      debug: {
        validateRawKeys: keysOf(validateExecRaw),
        validateResultKeys: keysOf(validateResult),
        searchRawKeys: keysOf(searchExecRaw),
        searchResultKeys: keysOf(searchResult),
        firstTeamCandidateKeys: teamCandidates.length ? keysOf(teamCandidates[0]) : [],
        firstSearchItem: searchLoopResults.length ? searchItemDebug(searchLoopResults[0]) : null,
        secondSearchItem: searchLoopResults.length > 1 ? searchItemDebug(searchLoopResults[1]) : null
      }
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
