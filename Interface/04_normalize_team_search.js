// Dynatrace JS action: Normalize Team ServiceNow search results
//
// Purpose:
// - Read validate_interfaces.teamIncidents[]
// - Read looped ServiceNow search results from snow_search_team
// - Return flat, loop-safe arrays for ServiceNow Create/Update tasks
//
// Output arrays are intentionally simple. Do not spread the full candidate object
// into create/update items, because Dynatrace loop actions may reject nested values.

import { execution } from "@dynatrace-sdk/automation-utils";

export default async function ({ execution_id }) {
  const validateTaskIds = ["validate_interfaces", "validate"];
  const searchTaskIds = [
    "snow_search_team",
    "snow_search",
    "snow_search_team_incident",
    "snow_search_incident",
    "servicenow_search_team"
  ];

  function text(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function isObj(v) {
    return v !== null && typeof v === "object" && !Array.isArray(v);
  }

  function getPath(obj, path) {
    let cur = obj;
    for (const part of path.split(".")) {
      if (!part) continue;
      if (cur === null || cur === undefined) return undefined;
      cur = cur[part];
    }
    return cur;
  }

  async function readFirstTaskResult(workflowExecution, taskIds) {
    const errors = [];

    for (const taskId of taskIds) {
      try {
        const result = await workflowExecution.result(taskId);
        return { found: true, taskId, result, errors };
      } catch (e) {
        errors.push({ taskId, error: text(e && e.message ? e.message : e) });
      }
    }

    return { found: false, taskId: "", result: {}, errors };
  }

  function collectArrays(obj, out, depth) {
    if (!obj || depth < 0) return;

    if (Array.isArray(obj)) {
      out.push(obj);
      return;
    }

    if (!isObj(obj)) return;

    for (const key of Object.keys(obj)) {
      collectArrays(obj[key], out, depth - 1);
    }
  }

  function candidateScore(arr) {
    if (!Array.isArray(arr) || arr.length === 0 || !isObj(arr[0])) return 0;
    const x = arr[0];
    let score = 0;
    if (x.correlation_id !== undefined || x.CorrelationId !== undefined) score += 10;
    if (x.short_description !== undefined) score += 4;
    if (x.description !== undefined || x.work_notes !== undefined || x.comment !== undefined) score += 3;
    if (x.clusterId !== undefined || x.cluster_id !== undefined || x.ClusterId !== undefined) score += 2;
    return score;
  }

  function getTeamIncidents(validateResult) {
    const directPaths = [
      "teamIncidents",
      "result.teamIncidents",
      "output.teamIncidents",
      "body.teamIncidents",
      "team_incidents"
    ];

    for (const p of directPaths) {
      const value = getPath(validateResult, p);
      if (Array.isArray(value)) return { path: p, items: value };
    }

    const arrays = [];
    collectArrays(validateResult, arrays, 6);

    let best = [];
    let bestScore = 0;

    for (const arr of arrays) {
      const score = candidateScore(arr);
      if (score > bestScore) {
        best = arr;
        bestScore = score;
      }
    }

    return { path: bestScore > 0 ? "auto_detected" : "not_found", items: best };
  }

  function isSnowRecordArray(v) {
    if (!Array.isArray(v)) return false;
    if (v.length === 0) return true;
    if (!isObj(v[0])) return false;

    const r = v[0];
    return (
      r.sys_id !== undefined ||
      r.number !== undefined ||
      r.state !== undefined ||
      r.short_description !== undefined ||
      r.correlation_id !== undefined
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
      const value = getPath(item, p);
      if (isSnowRecordArray(value)) return value;
    }

    return [];
  }

  function getSearchLoopItems(searchResult, expectedCount) {
    if (Array.isArray(searchResult)) return { path: "root", items: searchResult };

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
      const value = getPath(searchResult, p);
      if (Array.isArray(value) && !isSnowRecordArray(value)) {
        return { path: p, items: value };
      }
    }

    const arrays = [];
    collectArrays(searchResult, arrays, 7);

    for (const arr of arrays) {
      if (expectedCount > 0 && arr.length === expectedCount && !isSnowRecordArray(arr)) {
        return { path: "auto_detected", items: arr };
      }
    }

    if (isObj(searchResult)) return { path: "single_result", items: [searchResult] };

    return { path: "not_found", items: [] };
  }

  function getCorrelation(candidate) {
    return text(candidate.correlation_id || candidate.CorrelationId || candidate.correlationId);
  }

  function getShortDescription(candidate) {
    return text(candidate.short_description || candidate.shortDescription || "Cohesity Interface DOWN - Team");
  }

  function getDescription(candidate) {
    return text(candidate.description || candidate.work_notes || candidate.comment || getShortDescription(candidate));
  }

  function getComment(candidate) {
    const existing = text(candidate.comment || candidate.work_notes || candidate.description);
    if (existing) return existing;

    const cluster = text(candidate.clusterName || candidate.ClusterName || candidate.cluster_name || candidate.Cluster);
    const correlation = getCorrelation(candidate);

    return [
      "Cohesity Interface DOWN workflow update.",
      cluster ? "Cluster: " + cluster : "",
      correlation ? "Correlation ID: " + correlation : ""
    ].filter(Boolean).join("\n");
  }

  try {
    if (!execution_id) {
      return {
        ok: false,
        error: "Missing execution_id",
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

    const workflowExecution = await execution(execution_id);
    const validate = await readFirstTaskResult(workflowExecution, validateTaskIds);
    const search = await readFirstTaskResult(workflowExecution, searchTaskIds);

    if (!validate.found || !search.found) {
      return {
        ok: false,
        error: "Required previous task result not found",
        inputTeamCount: 0,
        searchLoopCount: 0,
        createCount: 0,
        updateCount: 0,
        noWriteCount: 0,
        createTeamIncidents: [],
        updateTeamIncidents: [],
        noWriteTeamIncidents: [],
        debug: {
          validateTask: validate.taskId,
          searchTask: search.taskId,
          validateErrors: validate.errors,
          searchErrors: search.errors
        }
      };
    }

    const teamPick = getTeamIncidents(validate.result);
    const teamItems = teamPick.items;

    const searchPick = getSearchLoopItems(search.result, teamItems.length);
    const searchItems = searchPick.items;

    const createTeamIncidents = [];
    const updateTeamIncidents = [];
    const noWriteTeamIncidents = [];

    for (let i = 0; i < teamItems.length; i++) {
      const candidate = teamItems[i] || {};
      const records = extractRecords(searchItems[i]);
      const correlationId = getCorrelation(candidate);
      const comment = getComment(candidate);

      if (!correlationId) {
        noWriteTeamIncidents.push({
          correlation_id: "",
          reason: "missing_correlation_id",
          matchedCount: records.length,
          incident_numbers: ""
        });
        continue;
      }

      if (records.length === 0) {
        createTeamIncidents.push({
          short_description: getShortDescription(candidate),
          description: getDescription(candidate),
          correlation_id: correlationId,
          comment: comment
        });
        continue;
      }

      if (records.length === 1) {
        const inc = records[0] || {};
        const number = text(inc.number);

        if (!number) {
          noWriteTeamIncidents.push({
            correlation_id: correlationId,
            reason: "matched_incident_missing_number",
            matchedCount: 1,
            incident_numbers: ""
          });
          continue;
        }

        updateTeamIncidents.push({
          number: number,
          comment: comment,
          correlation_id: correlationId
        });
        continue;
      }

      noWriteTeamIncidents.push({
        correlation_id: correlationId,
        reason: "duplicate_active_incidents_found",
        matchedCount: records.length,
        incident_numbers: records.map(function (r) { return text(r && r.number); }).filter(Boolean).join(", ")
      });
    }

    return {
      ok: true,
      selectedValidateTask: validate.taskId,
      selectedSearchTask: search.taskId,
      selectedTeamPath: teamPick.path,
      selectedSearchLoopPath: searchPick.path,
      inputTeamCount: teamItems.length,
      searchLoopCount: searchItems.length,
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
      error: text(e && e.message ? e.message : e),
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
