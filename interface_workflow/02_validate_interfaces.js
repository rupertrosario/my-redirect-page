// Dynatrace JS action: Validate Cohesity interface DOWN rows
// Purpose: Use alerts from 01_get_alerts.js, call /public/interface, and preserve Location for DC routing.
// Cohesity calls are GET only.

import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";
import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  var baseUrl = "https://helios.cohesity.com";
  var vaultName = "Cohesity_API_Key";
  var vaultId = "credentials_vault-312312";
  var alertStepName = "get_alerts";

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function toArrayMaybe(v) {
    if (!v) return [];
    return Array.isArray(v) ? v : [v];
  }

  function buildQuery(params) {
    var usp = new URLSearchParams();
    for (var k in params) {
      if (!Object.prototype.hasOwnProperty.call(params, k)) continue;
      var v = params[k];
      if (v === undefined || v === null) continue;
      if (Array.isArray(v)) {
        for (var i = 0; i < v.length; i++) usp.append(k, String(v[i]));
      } else {
        usp.append(k, String(v));
      }
    }
    return usp.toString();
  }

  async function getJson(url, headers) {
    var resp = await fetch(url, { method: "GET", headers: headers });
    if (!resp.ok) {
      var txt = "";
      try { txt = await resp.text(); } catch (e) {}
      throw new Error("GET " + url + " -> HTTP " + resp.status + " " + txt);
    }
    return resp.json();
  }

  async function getKeyByName(name) {
    var all = await credentialVaultClient.getCredentials();
    var creds = (all && all.credentials) ? all.credentials : [];
    for (var i = 0; i < creds.length; i++) {
      if (creds[i] && creds[i].name === name) {
        var detail = await credentialVaultClient.getCredentialsDetails({ id: creds[i].id });
        return (detail && (detail.token || detail.password)) || null;
      }
    }
    return null;
  }

  var apiKey = null;
  var authMode = "vault-name";
  try {
    apiKey = await getKeyByName(vaultName);
    if (!apiKey) throw new Error("vault name not found");
  } catch (e) {
    var d2 = await credentialVaultClient.getCredentialsDetails({ id: vaultId });
    apiKey = (d2 && (d2.token || d2.password)) || null;
    authMode = "vault-id";
  }
  if (!apiKey) throw new Error("No Helios API key available.");

  function looksDown(v) {
    v = norm(v).toLowerCase();
    return v.indexOf("down") >= 0 || v.indexOf("error") >= 0 || v.indexOf("disabled") >= 0 || v.indexOf("unknown") >= 0;
  }

  function indexSlaveDetails(details) {
    var map = {};
    var arr = toArrayMaybe(details);
    for (var i = 0; i < arr.length; i++) {
      var d = arr[i] || {};
      var name = norm(d.name || d["@name"] || d.ifaceName || d.interfaceName || d.iface);
      if (name) map[name] = d;
    }
    return map;
  }

  function safeCell(v) {
    if (v === null || v === undefined) return "";
    return String(v).replace(/\|/g, " ");
  }

  var alertExec = await execution(alertStepName);
  var alertResult = (alertExec && alertExec.result) ? alertExec.result : (alertExec || {});
  var alertTargets = toArrayMaybe(alertResult.results || alertResult.alertTargets || alertResult.alerts || []);

  var uniq = {};
  var targets = [];
  for (var t = 0; t < alertTargets.length; t++) {
    var a = alertTargets[t] || {};
    var clusterId = norm(a.ClusterId || a.clusterId);
    var ip = norm(a.NodeIP || a.IP || a.ip);
    if (!clusterId || !ip) continue;
    var key = clusterId + "|" + ip;
    if (uniq[key]) continue;
    uniq[key] = true;
    targets.push({
      ClusterId: clusterId,
      ClusterName: norm(a.ClusterName || a.clusterName),
      Location: norm(a.Location || a.DcLocation || a.location),
      DcLocation: norm(a.DcLocation || a.Location || a.location),
      NodeIP: ip,
      NodeID: norm(a.NodeID || a.ClusterNodeId || a.nodeId),
      AlertCode: norm(a.AlertCode || a.alertCode),
      AlertCause: norm(a.AlertCause || a.alertCause),
      LatestTimeET: norm(a.LatestTimeET || a.latestTimeET)
    });
  }

  if (!targets.length) {
    return {
      authMode: authMode,
      inputAlertRows: alertTargets.length,
      targetsFound: 0,
      downCount: 0,
      downRows: [],
      markdownTable: "No alert targets with ClusterId + NodeIP found."
    };
  }

  var byCluster = {};
  for (var i = 0; i < targets.length; i++) {
    var r = targets[i];
    if (!byCluster[r.ClusterId]) byCluster[r.ClusterId] = { ClusterId: r.ClusterId, ClusterName: r.ClusterName, Ips: {}, Targets: {} };
    byCluster[r.ClusterId].Ips[r.NodeIP] = true;
    byCluster[r.ClusterId].Targets[r.NodeIP] = r;
    if (!byCluster[r.ClusterId].ClusterName && r.ClusterName) byCluster[r.ClusterId].ClusterName = r.ClusterName;
  }

  var ifaceQs = buildQuery({
    bondInterfaceOnly: "true",
    ifaceGroupAssignedOnly: "true",
    includeUplinkSwitchInfo: "true",
    includeBondSlaveDetails: "true"
  });
  var ifaceUrl = baseUrl + "/irisservices/api/v1/public/interface?" + ifaceQs;

  var downRows = [];
  var clusterIds = Object.keys(byCluster);

  for (var c = 0; c < clusterIds.length; c++) {
    var cid = clusterIds[c];
    var entry = byCluster[cid];
    var headers = { accept: "application/json", apiKey: apiKey, accessClusterId: cid };

    var ifaceData;
    try {
      ifaceData = await getJson(ifaceUrl, headers);
    } catch (e) {
      console.log("Interface fetch failed for " + (entry.ClusterName || cid) + ": " + norm(e.message || e));
      continue;
    }

    var nodes = toArrayMaybe(ifaceData);
    for (var ni = 0; ni < nodes.length; ni++) {
      var node = nodes[ni] || {};
      var nodeIp = norm(node.nodeIp);
      if (!nodeIp || !entry.Ips[nodeIp]) continue;

      var matchedAlert = entry.Targets[nodeIp] || {};
      var ifaces = toArrayMaybe(node.interfaces);

      for (var bi = 0; bi < ifaces.length; bi++) {
        var bond = ifaces[bi] || {};
        var bondName = norm(bond.name);
        var mtu = (bond.mtu === null || bond.mtu === undefined) ? "" : String(bond.mtu);
        var slaves = toArrayMaybe(bond.bondSlaves);
        var slotTypes = toArrayMaybe(bond.bondSlavesSlotTypes);
        var dmap = indexSlaveDetails(bond.bondSlavesDetails);

        for (var si = 0; si < slaves.length; si++) {
          var s = slaves[si];
          var slaveName = norm(typeof s === "string" ? s : (s && (s.name || s["@name"] || s.ifaceName)));
          if (!slaveName) continue;
          var d = dmap[slaveName] || {};
          var linkState = norm(d.linkState || d.link_state || d.state);
          if (!looksDown(linkState)) continue;

          downRows.push({
            Location: norm(matchedAlert.Location || matchedAlert.DcLocation),
            DcLocation: norm(matchedAlert.DcLocation || matchedAlert.Location),
            ClusterName: entry.ClusterName || matchedAlert.ClusterName,
            ClusterId: cid,
            NodeIP: nodeIp,
            NodeID: norm(node.nodeId || matchedAlert.NodeID),
            ChassisSerial: norm(node.chassisSerial),
            BondName: bondName,
            MTU: mtu,
            Slave: slaveName,
            LinkState: linkState,
            MAC: norm(d.macAddr || d.mac || d.mac_address),
            Speed: norm(d.speed || d.linkSpeed || d.link_speed),
            SlotType: (slotTypes[si] !== null && slotTypes[si] !== undefined) ? String(slotTypes[si]) : "",
            AlertCode: matchedAlert.AlertCode || "",
            AlertCause: matchedAlert.AlertCause || "",
            LatestTimeET: matchedAlert.LatestTimeET || ""
          });
        }
      }
    }
  }

  downRows.sort(function (a, b) {
    return safeCell(a.ClusterName).localeCompare(safeCell(b.ClusterName)) ||
      safeCell(a.Location).localeCompare(safeCell(b.Location)) ||
      safeCell(a.NodeIP).localeCompare(safeCell(b.NodeIP)) ||
      safeCell(a.Slave).localeCompare(safeCell(b.Slave));
  });

  function toMarkdownTable(rows) {
    if (!rows.length) return "No confirmed DOWN slave interfaces found.";
    var cols = ["Location", "ClusterName", "ClusterId", "NodeIP", "NodeID", "BondName", "Slave", "LinkState", "Speed", "MAC", "SlotType"];
    var header = "| " + cols.join(" | ") + " |";
    var sep = "| " + cols.map(function () { return "---"; }).join(" | ") + " |";
    var body = rows.map(function (r) { return "| " + cols.map(function (c) { return safeCell(r[c]); }).join(" | ") + " |"; });
    return [header, sep].concat(body).join("\n");
  }

  return {
    authMode: authMode,
    inputAlertRows: alertTargets.length,
    targetsFound: targets.length,
    downCount: downRows.length,
    missingLocationCount: downRows.filter(function (r) { return !r.Location && !r.DcLocation; }).length,
    downRows: downRows,
    markdownTable: toMarkdownTable(downRows),
    markdownEmail: toMarkdownTable(downRows)
  };
}
