// Dynatrace JS action: Build Team incident iterations
// Purpose: One Team incident candidate per affected Cohesity cluster.

import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  var validateStepName = "validate_interfaces";
  var teamCorrelationPrefix = "cohesity_ifdown_bkup_team";

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function toArrayMaybe(v) {
    if (!v) return [];
    return Array.isArray(v) ? v : [v];
  }

  function safeText(v) {
    return norm(v).replace(/\|/g, " ");
  }

  function buildMarkdown(rows) {
    var cols = ["Location", "ClusterName", "ClusterId", "NodeIP", "NodeID", "BondName", "Slave", "LinkState", "Speed", "MAC", "SlotType"];
    var header = "| " + cols.join(" | ") + " |";
    var sep = "| " + cols.map(function () { return "---"; }).join(" | ") + " |";
    var body = rows.map(function (r) {
      return "| " + cols.map(function (c) { return safeText(r[c]); }).join(" | ") + " |";
    });
    return [header, sep].concat(body).join("\n");
  }

  var validateExec = await execution(validateStepName);
  var validateResult = (validateExec && validateExec.result) ? validateExec.result : (validateExec || {});
  var downRows = toArrayMaybe(validateResult.downRows || []);

  var byCluster = {};
  for (var i = 0; i < downRows.length; i++) {
    var r = downRows[i] || {};
    var clusterId = norm(r.ClusterId || r.clusterId);
    if (!clusterId) continue;
    if (!byCluster[clusterId]) {
      byCluster[clusterId] = {
        ClusterId: clusterId,
        ClusterName: norm(r.ClusterName || r.clusterName),
        Rows: []
      };
    }
    byCluster[clusterId].Rows.push(r);
    if (!byCluster[clusterId].ClusterName && r.ClusterName) byCluster[clusterId].ClusterName = norm(r.ClusterName);
  }

  var candidates = [];
  var clusterIds = Object.keys(byCluster).sort();
  for (var c = 0; c < clusterIds.length; c++) {
    var cid = clusterIds[c];
    var g = byCluster[cid];
    var rows = g.Rows;
    var clusterName = g.ClusterName || cid;
    var ips = {};
    var slaves = {};
    for (var ri = 0; ri < rows.length; ri++) {
      if (rows[ri].NodeIP) ips[rows[ri].NodeIP] = true;
      if (rows[ri].Slave) slaves[rows[ri].Slave] = true;
    }

    var correlationId = teamCorrelationPrefix + "_" + cid;
    var shortDescription = "Cohesity Interface DOWN - Team - " + clusterName;
    var markdown = buildMarkdown(rows);
    var description =
      "Cohesity Interface DOWN confirmed via Helios alert + /public/interface.\n\n" +
      "Cluster: " + clusterName + "\n" +
      "ClusterId: " + cid + "\n" +
      "NodeIPs: " + Object.keys(ips).join(", ") + "\n" +
      "Interfaces: " + Object.keys(slaves).join(", ") + "\n" +
      "ConfirmedRows: " + rows.length + "\n\n" +
      markdown;

    candidates.push({
      Type: "TEAM",
      ClusterName: clusterName,
      ClusterId: cid,
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
    teamCount: candidates.length,
    teamIncidentCandidates: candidates,
    teamIterations: candidates,
    iterations: candidates
  };
}
