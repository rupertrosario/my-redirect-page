// Dynatrace JS action: Build DC incident iterations
// Purpose: One DC incident candidate per affected ClusterId + Location.
// This is the main place to debug when Team incidents work but a DC incident is not created.

import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  var validateStepName = "validate_interfaces";
  var dcCorrelationPrefix = "cohesity_ifdown_dc";

  var allowedLocations = [
    "San Antonio",
    "Carrollton",
    "Detroit",
    "Ashburn"
  ];

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function toArrayMaybe(v) {
    if (!v) return [];
    return Array.isArray(v) ? v : [v];
  }

  function normalizeLocation(v) {
    var s = norm(v);
    if (!s) return "";
    var low = s.toLowerCase();
    if (low.indexOf("san antonio") >= 0 || low === "sat") return "San Antonio";
    if (low.indexOf("carrollton") >= 0 || low === "carr") return "Carrollton";
    if (low.indexOf("detroit") >= 0 || low === "dtw") return "Detroit";
    if (low.indexOf("ashburn") >= 0 || low === "iad") return "Ashburn";
    return s;
  }

  function isAllowedLocation(loc) {
    var n = normalizeLocation(loc);
    for (var i = 0; i < allowedLocations.length; i++) {
      if (allowedLocations[i] === n) return true;
    }
    return false;
  }

  function safeText(v) {
    return norm(v).replace(/\|/g, " ");
  }

  function buildMarkdown(rows) {
    var cols = ["Location", "ClusterName", "ClusterId", "NodeIP", "NodeID", "BondName", "Slave", "LinkState", "Speed", "MAC", "SlotType"];
    var header = "| " + cols.join(" | ") + " |";
    var sep = "| " + cols.map(function () { return "---"; }).join(" | ") + " |";
    var body = rows.map(function (r) {
      return "| " + cols.map(function (c) { return safeText(c === "Location" ? normalizeLocation(r.Location || r.DcLocation) : r[c]); }).join(" | ") + " |";
    });
    return [header, sep].concat(body).join("\n");
  }

  var validateExec = await execution(validateStepName);
  var validateResult = (validateExec && validateExec.result) ? validateExec.result : (validateExec || {});
  var downRows = toArrayMaybe(validateResult.downRows || []);

  var skippedNoLocation = [];
  var skippedNotAllowlisted = [];
  var routedRows = [];

  for (var i = 0; i < downRows.length; i++) {
    var r = downRows[i] || {};
    var loc = normalizeLocation(r.Location || r.DcLocation || r.location || r.dcLocation);
    if (!loc) {
      skippedNoLocation.push(r);
      continue;
    }
    if (!isAllowedLocation(loc)) {
      var copy1 = Object.assign({}, r);
      copy1.Location = loc;
      skippedNotAllowlisted.push(copy1);
      continue;
    }
    var copy = Object.assign({}, r);
    copy.Location = loc;
    copy.DcLocation = loc;
    routedRows.push(copy);
  }

  var byDc = {};
  for (var rr = 0; rr < routedRows.length; rr++) {
    var row = routedRows[rr];
    var clusterId = norm(row.ClusterId || row.clusterId);
    var location = normalizeLocation(row.Location || row.DcLocation);
    if (!clusterId || !location) continue;
    var key = clusterId + "|" + location;
    if (!byDc[key]) {
      byDc[key] = {
        ClusterId: clusterId,
        ClusterName: norm(row.ClusterName || row.clusterName),
        Location: location,
        Rows: []
      };
    }
    byDc[key].Rows.push(row);
    if (!byDc[key].ClusterName && row.ClusterName) byDc[key].ClusterName = norm(row.ClusterName);
  }

  var candidates = [];
  var keys = Object.keys(byDc).sort();
  for (var k = 0; k < keys.length; k++) {
    var g = byDc[keys[k]];
    var rows = g.Rows;
    var clusterName = g.ClusterName || g.ClusterId;
    var location = g.Location;
    var ips = {};
    var slaves = {};
    for (var x = 0; x < rows.length; x++) {
      if (rows[x].NodeIP) ips[rows[x].NodeIP] = true;
      if (rows[x].Slave) slaves[rows[x].Slave] = true;
    }

    var correlationId = dcCorrelationPrefix + "_" + g.ClusterId + "_" + location.replace(/\s+/g, "_");
    var shortDescription = "Cohesity Interface DOWN - DC - " + location + " - " + clusterName;
    var markdown = buildMarkdown(rows);
    var description =
      "Cohesity Interface DOWN confirmed for DC routing.\n\n" +
      "Location: " + location + "\n" +
      "Cluster: " + clusterName + "\n" +
      "ClusterId: " + g.ClusterId + "\n" +
      "NodeIPs: " + Object.keys(ips).join(", ") + "\n" +
      "Interfaces: " + Object.keys(slaves).join(", ") + "\n" +
      "ConfirmedRows: " + rows.length + "\n\n" +
      markdown;

    candidates.push({
      Type: "DC",
      Location: location,
      DcLocation: location,
      ClusterName: clusterName,
      ClusterId: g.ClusterId,
      CorrelationId: correlationId,
      ShortDescription: shortDescription,
      Description: description,
      WorkNotes: description,
      DownCount: rows.length,
      Rows: rows,
      MarkdownTable: markdown
    });
  }

  return {
    inputDownRows: downRows.length,
    routedRows: routedRows.length,
    skippedNoLocationCount: skippedNoLocation.length,
    skippedNotAllowlistedCount: skippedNotAllowlisted.length,
    skippedNoLocation: skippedNoLocation,
    skippedNotAllowlisted: skippedNotAllowlisted,
    dcCount: candidates.length,
    dcIncidentCandidates: candidates,
    dcIterations: candidates,
    iterations: candidates
  };
}
