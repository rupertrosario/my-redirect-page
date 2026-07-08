// Dynatrace JS action: Validate Cohesity interface DOWN rows
// Current workflow:
//   get_alerts -> validate_interfaces -> snow_search_team -> normalize_team_search
//
// Purpose:
// - Read get_alerts results
// - Call Cohesity /irisservices/api/v1/public/interface per affected cluster
// - Keep only confirmed DOWN interfaces
// - Return teamIncidents[] and dcIncidents[] for downstream ServiceNow search loops
//
// Notes:
// - Cohesity calls are GET only
// - Team incident identity is cluster-level correlation_id
// - DC incident identity is location + cluster correlation_id

import { execution } from "@dynatrace-sdk/automation-utils";
import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const vaultName = "Cohesity_API_Key";
  const vaultId = "CREDENTIALS_VAULT-7F2FF2BB6BCD9B63";

  const dcLocationAllowList = [
    "San Antonio",
    "Carrollton",
    "Detroit",
    "Ashburn"
  ];

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function toArray(v) {
    if (!v) return [];
    return Array.isArray(v) ? v : [v];
  }

  function safeText(v) {
    return norm(v).replace(/\|/g, " ");
  }

  function isDown(v) {
    const s = norm(v).toLowerCase();
    return s === "down" || s === "kdown" || s === "link_down" || s === "linkdown";
  }

  function getExecResult(execObj) {
    if (!execObj) return {};
    return execObj.result || execObj;
  }

  async function getKeyByName(name) {
    const all = await credentialVaultClient.getCredentials();
    const creds = all && all.credentials ? all.credentials : [];
    const found = creds.find(function (c) { return c && c.name === name; });
    if (!found) return null;

    const detail = await credentialVaultClient.getCredentialsDetails({ id: found.id });
    return detail && (detail.token || detail.password) ? (detail.token || detail.password) : null;
  }

  async function getApiKey() {
    try {
      const byName = await getKeyByName(vaultName);
      if (byName) return byName;
    } catch (e1) {}

    try {
      const detail = await credentialVaultClient.getCredentialsDetails({ id: vaultId });
      if (detail && (detail.token || detail.password)) return detail.token || detail.password;
    } catch (e2) {}

    throw new Error("No Cohesity API key found in Dynatrace credential vault.");
  }

  async function getJson(url, headers) {
    const resp = await fetch(url, {
      method: "GET",
      headers: headers
    });

    if (!resp.ok) {
      let txt = "";
      try { txt = await resp.text(); } catch (e) {}
      throw new Error("GET " + url + " -> HTTP " + resp.status + " " + txt);
    }

    return resp.json();
  }

  function buildMarkdown(rows) {
    const cols = [
      "Location",
      "ClusterName",
      "ClusterId",
      "NodeIP",
      "NodeID",
      "BondName",
      "Slave",
      "LinkState",
      "Speed",
      "MAC",
      "SlotType"
    ];

    const header = "| " + cols.join(" | ") + " |";
    const sep = "| " + cols.map(function () { return "---"; }).join(" | ") + " |";
    const body = rows.map(function (r) {
      return "| " + cols.map(function (c) { return safeText(r[c]); }).join(" | ") + " |";
    });

    return [header, sep].concat(body).join("\n");
  }

  function normalizeInterfaceRows(json) {
    const candidates = [];

    if (Array.isArray(json)) candidates.push.apply(candidates, json);
    if (json && Array.isArray(json.interfaces)) candidates.push.apply(candidates, json.interfaces);
    if (json && Array.isArray(json.result)) candidates.push.apply(candidates, json.result);
    if (json && Array.isArray(json.items)) candidates.push.apply(candidates, json.items);

    if (json && json.nodes && Array.isArray(json.nodes)) {
      for (const node of json.nodes) {
        const ifaces = toArray(node.interfaces || node.networkInterfaces || node.bondInterfaces);
        for (const iface of ifaces) {
          candidates.push({
            ...iface,
            nodeId: iface.nodeId || node.nodeId || node.id,
            nodeIp: iface.nodeIp || node.ip || node.nodeIp
          });
        }
      }
    }

    return candidates;
  }

  function field(obj, names) {
    for (const n of names) {
      if (obj && obj[n] !== undefined && obj[n] !== null && String(obj[n]).trim() !== "") {
        return obj[n];
      }
    }
    return "";
  }

  function getValidatedDownRows(alertRows, ifaceRows) {
    const rows = [];

    for (const alert of alertRows) {
      const alertClusterId = norm(alert.ClusterId || alert.clusterId);
      const alertClusterName = norm(alert.ClusterName || alert.clusterName);
      const alertLocation = norm(alert.Location || alert.location || "Unknown");
      const alertNodeId = norm(alert.ClusterNodeId || alert.NodeID || alert.nodeId || alert.node_id);
      const alertIp = norm(alert.IP || alert.NodeIP || alert.ip || alert.nodeIp);

      const matchingIfaces = ifaceRows.filter(function (iface) {
        const ifaceNodeId = norm(field(iface, ["nodeId", "NodeID", "node_id", "nodeID"]));
        const ifaceIp = norm(field(iface, ["nodeIp", "NodeIP", "ip", "IP", "ipAddress"]));

        const nodeMatch = alertNodeId && ifaceNodeId && alertNodeId === ifaceNodeId;
        const ipMatch = alertIp && ifaceIp && alertIp === ifaceIp;

        return nodeMatch || ipMatch || (!alertNodeId && !alertIp);
      });

      const scanRows = matchingIfaces.length ? matchingIfaces : ifaceRows;

      for (const iface of scanRows) {
        const linkState = norm(field(iface, ["linkState", "LinkState", "state", "State", "status", "Status"]));
        const adminState = norm(field(iface, ["adminState", "AdminState"]));
        const operState = norm(field(iface, ["operState", "OperState", "operStatus"]));

        if (!(isDown(linkState) || isDown(operState))) {
          continue;
        }

        const slave = norm(field(iface, ["slave", "Slave", "name", "Name", "interfaceName", "ifaceName"]));
        const bondName = norm(field(iface, ["bondName", "BondName", "bond", "Bond", "interfaceGroupName", "interfaceGroup"]));
        const mac = norm(field(iface, ["mac", "MAC", "macAddress", "MacAddress"]));
        const speed = norm(field(iface, ["speed", "Speed", "speedMbps", "linkSpeed"]));
        const slotType = norm(field(iface, ["slotType", "SlotType", "type", "Type"]));
        const nodeId = norm(field(iface, ["nodeId", "NodeID", "node_id", "nodeID"])) || alertNodeId;
        const nodeIp = norm(field(iface, ["nodeIp", "NodeIP", "ip", "IP", "ipAddress"])) || alertIp;

        rows.push({
          Location: alertLocation,
          ClusterName: alertClusterName,
          ClusterId: alertClusterId,
          NodeIP: nodeIp,
          NodeID: nodeId,
          BondName: bondName,
          Slave: slave,
          LinkState: linkState || operState,
          AdminState: adminState,
          Speed: speed,
          MAC: mac,
          SlotType: slotType,
          AlertCode: norm(alert.AlertCode || alert.alertCode),
          AlertCause: norm(alert.AlertCause || alert.alertCause),
          LatestTimeET: norm(alert.LatestTimeET || alert.latestTimeEt)
        });
      }
    }

    const seen = {};
    return rows.filter(function (r) {
      const key = [r.ClusterId, r.NodeID, r.NodeIP, r.Slave, r.BondName].join("|");
      if (seen[key]) return false;
      seen[key] = true;
      return true;
    });
  }

  function buildTeamIncidents(downRows) {
    const byCluster = {};

    for (const r of downRows) {
      const clusterId = norm(r.ClusterId);
      if (!clusterId) continue;

      if (!byCluster[clusterId]) {
        byCluster[clusterId] = {
          cluster_id: clusterId,
          cluster_name: norm(r.ClusterName) || clusterId,
          rows: []
        };
      }

      byCluster[clusterId].rows.push(r);
    }

    return Object.keys(byCluster).sort().map(function (clusterId) {
      const g = byCluster[clusterId];
      const rows = g.rows;
      const fingerprint = rows.map(function (r) {
        return [norm(r.NodeIP), norm(r.Slave || r.BondName), norm(r.LinkState)].join("|");
      }).sort().join(";");

      const markdown = buildMarkdown(rows);
      const description =
        "Cohesity Interface DOWN confirmed via Helios alert + /public/interface.\n\n" +
        "Cluster: " + g.cluster_name + "\n" +
        "ClusterId: " + g.cluster_id + "\n" +
        "ConfirmedRows: " + rows.length + "\n\n" +
        markdown;

      return {
        type: "TEAM",
        cluster_id: g.cluster_id,
        cluster_name: g.cluster_name,
        correlation_id: "cohesity_ifdown_team_" + g.cluster_id,
        short_description: "Cohesity Interface DOWN - Team - " + g.cluster_name,
        fingerprint: fingerprint,
        description: description,
        work_notes: description,
        rows: rows
      };
    });
  }

  function buildDcIncidents(downRows) {
    const allow = {};
    for (const loc of dcLocationAllowList) allow[loc.toLowerCase()] = true;

    const byDc = {};

    for (const r of downRows) {
      const clusterId = norm(r.ClusterId);
      const location = norm(r.Location || "Unknown");
      if (!clusterId) continue;

      if (!allow[location.toLowerCase()]) {
        continue;
      }

      const key = location + "|" + clusterId;
      if (!byDc[key]) {
        byDc[key] = {
          location: location,
          cluster_id: clusterId,
          cluster_name: norm(r.ClusterName) || clusterId,
          rows: []
        };
      }

      byDc[key].rows.push(r);
    }

    return Object.keys(byDc).sort().map(function (key) {
      const g = byDc[key];
      const rows = g.rows;
      const safeLocation = g.location.replace(/[^A-Za-z0-9]+/g, "_");
      const fingerprint = rows.map(function (r) {
        return [norm(r.NodeIP), norm(r.Slave || r.BondName), norm(r.LinkState)].join("|");
      }).sort().join(";");

      const markdown = buildMarkdown(rows);
      const description =
        "Cohesity Interface DOWN confirmed via Helios alert + /public/interface.\n\n" +
        "Location: " + g.location + "\n" +
        "Cluster: " + g.cluster_name + "\n" +
        "ClusterId: " + g.cluster_id + "\n" +
        "ConfirmedRows: " + rows.length + "\n\n" +
        markdown;

      return {
        type: "DC",
        location: g.location,
        cluster_id: g.cluster_id,
        cluster_name: g.cluster_name,
        correlation_id: "cohesity_ifdown_dc_" + safeLocation + "_" + g.cluster_id,
        short_description: "Cohesity Interface DOWN - DC - " + g.location + " - " + g.cluster_name,
        fingerprint: fingerprint,
        description: description,
        work_notes: description,
        rows: rows
      };
    });
  }

  try {
    const getAlertsExec = await execution("get_alerts");
    const getAlertsResult = getExecResult(getAlertsExec);
    const alertRows = toArray(getAlertsResult.results || getAlertsResult.alertTargets || []);

    if (!alertRows.length) {
      return {
        ok: true,
        alertCount: 0,
        downCount: 0,
        teamCount: 0,
        dcCount: 0,
        downRows: [],
        teamIncidents: [],
        dcIncidents: []
      };
    }

    const apiKey = await getApiKey();
    const byCluster = {};

    for (const alert of alertRows) {
      const clusterId = norm(alert.ClusterId || alert.clusterId);
      if (!clusterId) continue;
      if (!byCluster[clusterId]) byCluster[clusterId] = [];
      byCluster[clusterId].push(alert);
    }

    let downRows = [];
    const clusterErrors = [];

    for (const clusterId of Object.keys(byCluster)) {
      const headers = {
        accept: "application/json",
        apiKey: apiKey,
        accessClusterId: clusterId
      };

      const url =
        baseUrl +
        "/irisservices/api/v1/public/interface?bondInterfaceOnly=false&ifaceGroupAssignedOnly=false&includeUplinkSwitchInfo=true&includeBondSlaveDetails=true";

      try {
        const json = await getJson(url, headers);
        const ifaceRows = normalizeInterfaceRows(json);
        const clusterDownRows = getValidatedDownRows(byCluster[clusterId], ifaceRows);
        downRows = downRows.concat(clusterDownRows);
      } catch (e) {
        clusterErrors.push({
          clusterId: clusterId,
          error: String(e && e.message ? e.message : e)
        });
      }
    }

    const teamIncidents = buildTeamIncidents(downRows);
    const dcIncidents = buildDcIncidents(downRows);

    return {
      ok: true,
      alertCount: alertRows.length,
      clusterCount: Object.keys(byCluster).length,
      downCount: downRows.length,
      teamCount: teamIncidents.length,
      dcCount: dcIncidents.length,
      clusterErrors: clusterErrors,
      downRows: downRows,
      teamIncidents: teamIncidents,
      dcIncidents: dcIncidents
    };

  } catch (e) {
    return {
      ok: false,
      error: String(e && e.message ? e.message : e),
      alertCount: 0,
      downCount: 0,
      teamCount: 0,
      dcCount: 0,
      downRows: [],
      teamIncidents: [],
      dcIncidents: []
    };
  }
}
