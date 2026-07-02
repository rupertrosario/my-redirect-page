// Dynatrace JS action: Normalize Team ServiceNow search results
// Current workflow:
//   get_alerts -> validate_interfaces -> snow_search_team -> normalize_team_search
//
// IMPORTANT:
// - snow_search_team / snow_search is a LOOPED ServiceNow Search task.
// - normalize_team_search is NOT looped.
// - This task pairs validate teamIncidents[index] with SNOW search loop result[index].
//
// This version tries multiple possible Dynatrace task IDs because execution("task_id")
// must match the exact task ID on the canvas.

import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const validateTaskIds = [
    "validate_interfaces",
    "validate"
  ];

  const snowSearchTaskIds = [
    "snow_search_team",
    "snow_search",
    "snow_search_team_incident",
    "snow_search_incident",
    "servicenow_search_team"
  ];

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function isObj(v) {
    return v !== null && typeof v === "object" && !Array.isArray(v);
  }

  function keysOf(v) {
    return isObj(v) ? Object.keys(v).sort() : [];
  }

  function unwrap(v) {
    if (!v) return {};
    if (v.result !== undefined) return v.result;
    return v;
  }

  async function getFirstExecution(taskIds) {
    const errors = [];

    for (const taskId of taskIds) {
      try {
        const raw = await execution(taskId);
        return {
          found: true,
          taskId: taskId,
          raw: raw,
          result: unwrap(raw),
          errors: errors
        };
      } catch (e) {
        errors.push({
          taskId: taskId,
          error: String(e && e.message ? e.message : e)
        });
      }
    }

    return {
      found: false,
      taskId: "",
      raw: {},
      result: {},
      errors: errors
    };
  }

  function pathGet(obj, path) {
    let cur = obj;
    for (const p of path.split(".")) {
      if (!p) continue;
      if (cur === null || cur === undefined) return undefined;
      cur = cur[p];
    }
    return cur;
  }

  function collectArrayValues(obj, path, out, maxDepth) {
    if (!obj || maxDepth < 0) return;

    if (Array.isArray(obj)) {
      out.push({ path: path || "$", value: obj });
      return;
    }

    if (!isObj(obj)) return;

    for (const k of Object.keys(obj)) {
      collectArrayValues(obj[k], path ? path + "." + k : k, out, maxDepth - 1);
    }
  }

  function summarizeArrays(raw, result) {
    const found = [];
    const values = [];
    collectArrayValues(result, "result", values, 6);
    collectArrayValues(raw, "raw", values, 7);

    for (const item of values.slice(0, 30)) {
      const a = item.value;
      found.push({
        path: item.path,
        length: Array.isArray(a) ? a.length : 0,
        firstKeys: Array.isArray(a) && a.length && isObj(a[0]) ? keysOf(a[0]) : []
      });
    }

    return found;
  }

  function scoreTeamArray(a) {
    if (!Array.isArray(a) || a.length === 0) return 0;
    const first = a[0];
    if (!isObj(first)) return 0;

    let score = 0;
    if (first.correlation_id !== undefined || first.CorrelationId !== undefined) score += 10;
    if (first.cluster_id !== undefined || first.ClusterId !== undefined) score += 5;
    if (first.short_description !== undefined) score += 3;
    if (first.description !== undefined || first.work_notes !== undefined) score += 2;
    if (first.type === "TEAM") score += 2;
    return score;
  }

  function getTeamCandidates(validateRaw, validateResult) {
    const directPaths = [
      "teamIncidents",
      "result.teamIncidents",
      "output.teamIncidents",
      "body.teamIncidents",
      "teamIncident",
      "teamincident",
      "team_incidents"
    ];

    for (const p of directPaths) {
      const v1 = pathGet(validateResult, p);
      if (Array.isArray(v1)) return { path: "validateResult." + p, value: v1 };

      const v2 = pathGet(validateRaw, p);
      if (Array.isArray(v2)) return { path: "validateRaw." + p, value: v2 };
    }

    const arrays = [];
    collectArrayValues(validateResult, "validateResult", arrays, 7);
    collectArrayValues(validateRaw, "validateRaw", arrays, 8);

    let best = { path: "not_found", value: [] };
    let bestScore = 0;

    for (const item of arrays) {
      const s = scoreTeamArray(item.value);
      if (s > bestScore) {
        bestScore = s;
        best = item;
      }
    }

    return best;
  }

  function isSnowRecordArray(a) {
    if (!Array.isArray(a)) return false;
    if (a.length === 0) return true;
    const first = a[0];
    if (!isObj(first)) return false;

    return (
      first.sys_id !== undefined ||
      first.number !== undefined ||
      first.state !== undefined ||
      first.short_description !== undefined ||
      first.correlation_id !== undefined
    );
  }

  function extractRecords(item) {
    if (!item) return [];
    if (isSnowRecordArray(item)) return item;
    if (!isObj(item)) return [];

    const paths = [
      "result",
      "records",
      "body.result",
      "body.records",
      "response.result",
      "response.records",
      "response.body.result",
      "response.body.records",
      "output.result",
      "output.records",
      "output.body.result",
      "output.body.records",
      "data.result",
      "data.records"
    ];

    for (const p of paths) {
      const v = pathGet(item, p);
      if (isSnowRecordArray(v)) return v;
    }

    return [];
  }

  function scoreLoopArray(a, expectedCount) {
    if (!Array.isArray(a) || a.length === 0) return 0;

    let score = 0;
    const first = a[0];

    if (expectedCount > 0 && a.length === expectedCount) score += 20;
    if (isObj(first) && first.loopItemValue !== undefined) score += 10;
    if (isObj(first) && first.result !== undefined) score += 5;
    if (isObj(first) && first.output !== undefined) score += 5;
    if (isObj(first) && first.body !== undefined) score += 5;
    if (isObj(first) && first.response !== undefined) score += 5;

    if (expectedCount > 1 && isSnowRecordArray(a)) score -= 30;

    return score;
  }

  function getSearchLoopResults(searchRaw, searchResult, expectedCount) {
    if (Array.isArray(searchResult) && !isSnowRecordArray(searchResult)) {
      return { path: "searchResult", value: searchResult };
    }

    const directPaths = [
      "iterations",
      "loopResults",
      "loopExecutions",
      "taskExecutions",
      "executionResults",
      "results",
      "items",
      "result.iterations",
      "result.loopResults",
      "result.results",
      "output.iterations",
      "output.results"
    ];

    for (const p of directPaths) {
      const v1 = pathGet(searchResult, p);
      if (Array.isArray(v1) && !isSnowRecordArray(v1)) return { path: "searchResult." + p, value: v1 };

      const v2 = pathGet(searchRaw, p);
      if (Array.isArray(v2) && !isSnowRecordArray(v2)) return { path: "searchRaw." + p, value: v2 };
    }

    const arrays = [];
    collectArrayValues(searchResult, "searchResult", arrays, 7);
    collectArrayValues(searchRaw, "searchRaw", arrays, 8);

    let best = { path: "not_found", value: [] };
    let bestScore = 0;

    for (const item of arrays) {
      const s = scoreLoopArray(item.value, expectedCount);
      if (s > bestScore) {
        bestScore = s;
        best = item;
      }
    }

    if (bestScore > 0) return best;

    if (isObj(searchResult)) {
      return { path: "searchResult_single", value: [searchResult] };
    }

    return best;
  }

  function searchItemDebug(item) {
    const records = extractRecords(item);
    return {
      keys: keysOf(item),
      recordCount: records.length,
      firstRecordKeys: records.length ? keysOf(records[0]) : []
    };
  }

  try {
    const validateExec = await getFirstExecution(validateTaskIds);
    const searchExec = await getFirstExecution(snowSearchTaskIds);

    if (!validateExec.found || !searchExec.found) {
      return {
        ok: false,
        error: "Required previous task execution not found",
        inputTeamCount: 0,
        searchLoopCount: 0,
        createCount: 0,
        updateCount: 0,
        noWriteCount: 0,
        createTeamIncidents: [],
        updateTeamIncidents: [],
        noWriteTeamIncidents: [],
        debug: {
          selectedValidateTask: validateExec.taskId,
          selectedSearchTask: searchExec.taskId,
          validateTried: validateTaskIds,
          searchTried: snowSearchTaskIds,
          validateErrors: validateExec.errors,
          searchErrors: searchExec.errors
        }
      };
    }

    const teamPick = getTeamCandidates(validateExec.raw, validateExec.result);
    const teamCandidates = teamPick.value;

    const searchPick = getSearchLoopResults(searchExec.raw, searchExec.result, teamCandidates.length);
    const searchLoopResults = searchPick.value;

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
      selectedValidateTask: validateExec.taskId,
      selectedSearchTask: searchExec.taskId,
      inputTeamCount: teamCandidates.length,
      searchLoopCount: searchLoopResults.length,
      createCount: createTeamIncidents.length,
      updateCount: updateTeamIncidents.length,
      noWriteCount: noWriteTeamIncidents.length,
      createTeamIncidents: createTeamIncidents,
      updateTeamIncidents: updateTeamIncidents,
      noWriteTeamIncidents: noWriteTeamIncidents,
      debug: {
        selectedValidateTask: validateExec.taskId,
        selectedSearchTask: searchExec.taskId,
        selectedTeamPath: teamPick.path,
        selectedSearchLoopPath: searchPick.path,
        validateRawKeys: keysOf(validateExec.raw),
        validateResultKeys: keysOf(validateExec.result),
        searchRawKeys: keysOf(searchExec.raw),
        searchResultKeys: keysOf(searchExec.result),
        validateArrayPaths: summarizeArrays(validateExec.raw, validateExec.result),
        searchArrayPaths: summarizeArrays(searchExec.raw, searchExec.result),
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
