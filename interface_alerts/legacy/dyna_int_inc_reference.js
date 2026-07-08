// Cohesity Helios - Interface DOWN legacy reference script
//
// This is the earlier combined collector/validator pattern kept for reference.
// It pulls 1105 networking alerts, extracts node_ip, deduplicates by ClusterId+IP,
// then calls /public/interface to confirm currently DOWN slave interfaces.
//
// Current cleaned workflow separates alert collection, validation, Team logic,
// and DC logic, but this file preserves the previously used working approach.

import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";

  let apiKey = null;
  let authMode = "vault-name";

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  function toArray(v) {
    if (!v) return [];
    return Array.isArray(v) ? v : [v];
  }

  function buildQuery(params) {
    const usp = new URLSearchParams();
    for (const k of Object.keys(params || {})) {
      const v = params[k];
      if (v === undefined || v === null) continue;
      if (Array.isArray(v)) {
        for (const item of v) usp.append(k, String(item));
      } else {
        usp.append(k, String(v));
      }
    }
    return usp.toString();
  }

  async function getKeyByName(name) {
    const all = await credentialVaultClient.getCredentials();
    const creds = all && all.credentials ? all.credentials : [];
    const found = creds.find(function (c) { return c && c.name === name; });
    if (!found) return null;
    const detail = await credentialVaultClient.getCredentialsDetails({ id: found.id });
    return detail && (detail.token || detail.password) ? (detail.token || detail.password) : null;
  }

  async function getJson(url, headers) {
    const resp = await fetch(url, { method: "GET", headers: headers });
    if (!resp.ok) {
      let txt = "";
      try { txt = await resp.text(); } catch (e) {}
      throw new Error("GET " + url + " -> HTTP " + resp.status + " " + txt);
    }
    return resp.json();
  }

  function looksDown(v) {
    return norm(v).toLowerCase().indexOf("down") >= 0;
  }

  function extractNodeIpFromAlert(alertObj) {
    const plist = alertObj ? alertObj.propertyList : null;
    if (!Array.isArray(plist)) return "";

    for (const kv of plist) {
      if (norm(kv && kv.key) !== "node_ip") continue;
      let ip = kv.value || kv.values;
      if (Array.isArray(ip)) return ip.length ? norm(ip[0]) : "";
      return norm(ip);
    }

    return "";
  }

  function indexSlaveDetails(details) {
    const map = {};
    for (const d of toArray(details)) {
      const name = norm(d && (d.name || d["@name"] || d.ifaceName || d.interfaceName || d.iface));
      if (name) map[name] = d;
    }
    return map;
  }

  try {
    apiKey = await getKeyByName(vaultName);
    if (!apiKey) throw new Error("not-found");
  } catch (e1) {
    try {
      const detail = await credentialVaultClient.getCredentialsDetails({ id: vaultId });
      apiKey = detail && (detail.token || detail.password) ? (detail.token || detail.password) : null;
      authMode = "vault-id";
    } catch (e2) {
      authMode = "manual";
      apiKey = "PASTE_YOUR_API_KEY_HERE";
    }
  }

  if (!apiKey) throw new Error("No Helios API key available.");

  const commonHeaders = { accept: "application/json", apiKey: apiKey };
  const clusterUrl = baseUrl + "/v2/mcm/cluster-mgmt/info";
  const clusterData = await getJson(clusterUrl, commonHeaders);
  const clusters = toArray(clusterData && clusterData.cohesityClusters);

  const alertsUrl =
    baseUrl +
    "/v2/alerts?" +
    buildQuery({
      maxAlerts: 200,
      alertTypes: "1105",
      alertStates: "kOpen,kNote",
      alertCategories: "kNetworking"
    });

  const alertTargets = [];
  let alertsCountTotal = 0;

  for (const cluster of clusters) {
    const clusterName = norm(cluster && cluster.clusterName);
    const clusterId = norm(cluster && cluster.clusterId);
    if (!clusterId) continue;

    const headers = { accept: "application/json", apiKey: apiKey, accessClusterId: clusterId };

    let aData;
    try {
      aData = await getJson(alertsUrl, headers);
    } catch (e) {
      console.log("Alerts fetch failed for " + clusterName + " (" + clusterId + "): " + norm(e.message || e));
      continue;
    }

    const alerts = toArray(aData && aData.alerts);
    alertsCountTotal += alerts.length;

    for (const alert of alerts) {
      alertTargets.push({
        AlertCode: norm(alert && alert.alertCode),
        IP: extractNodeIpFromAlert(alert),
        ClusterName: clusterName,
        ClusterId: clusterId,
        Severity: norm(alert && alert.severity),
        AlertState: norm(alert && alert.alertState),
        Id: norm(alert && alert.id)
      });
    }
  }

  const seenTargets = {};
  const uniqTargets = [];

  for (const target of alertTargets) {
    if (!target.ClusterId || !target.IP) continue;
    const key = target.ClusterId + "|" + target.IP;
    if (seenTargets[key]) continue;
    seenTargets[key] = true;
    uniqTargets.push(target);
  }

  const byCluster = {};
  for (const target of uniqTargets) {
    const cid = target.ClusterId;
    if (!byCluster[cid]) byCluster[cid] = { ClusterId: cid, ClusterName: target.ClusterName, Ips: {} };
    byCluster[cid].Ips[target.IP] = true;
  }

  const ifaceUrl =
    baseUrl +
    "/irisservices/api/v1/public/interface?" +
    buildQuery({
      bondInterfaceOnly: "true",
      ifaceGroupAssignedOnly: "true",
      includeUplinkSwitchInfo: "true",
      includeBondSlaveDetails: "true"
    });

  const downRows = [];

  for (const clusterId of Object.keys(byCluster)) {
    const entry = byCluster[clusterId];
    const headers = { accept: "application/json", apiKey: apiKey, accessClusterId: String(clusterId) };

    let ifaceData;
    try {
      ifaceData = await getJson(ifaceUrl, headers);
    } catch (e) {
      console.log("Interface fetch failed for " + entry.ClusterName + " (" + clusterId + "): " + norm(e.message || e));
      continue;
    }

    for (const node of toArray(ifaceData)) {
      const nodeIp = norm(node && node.nodeIp);
      if (!nodeIp || !entry.Ips[nodeIp]) continue;

      const nodeId = norm(node.nodeId);
      const chassisSerial = norm(node.chassisSerial);

      for (const bond of toArray(node.interfaces)) {
        const bondName = norm(bond && bond.name);
        const mtu = bond && bond.mtu !== undefined && bond.mtu !== null ? String(bond.mtu) : "";
        const slaves = toArray(bond && bond.bondSlaves);
        const slotTypes = toArray(bond && bond.bondSlavesSlotTypes);
        const detailMap = indexSlaveDetails(bond && bond.bondSlavesDetails);

        for (let i = 0; i < slaves.length; i++) {
          const slaveName = norm(typeof slaves[i] === "string" ? slaves[i] : (slaves[i] && (slaves[i].name || slaves[i]["@name"] || slaves[i].ifaceName)));
          if (!slaveName) continue;

          const d = detailMap[slaveName] || {};
          const linkState = norm(d.linkState || d.link_state || d.state);
          if (!looksDown(linkState)) continue;

          downRows.push({
            ClusterName: entry.ClusterName,
            ClusterId: String(clusterId),
            NodeIP: nodeIp,
            NodeID: nodeId,
            ChassisSerial: chassisSerial,
            BondName: bondName,
            MTU: mtu,
            Slave: slaveName,
            LinkState: linkState,
            MAC: norm(d.macAddr || d.mac || d.mac_address),
            Speed: norm(d.speed || d.linkSpeed || d.link_speed),
            SlotType: slotTypes[i] !== undefined && slotTypes[i] !== null ? String(slotTypes[i]) : ""
          });
        }
      }
    }
  }

  return {
    authMode: authMode,
    alertsCount: alertsCountTotal,
    targetsFound: uniqTargets.length,
    downCount: downRows.length,
    alertTargets: uniqTargets,
    downRows: downRows
  };
}
