// ------------------------------------------------------------
// Dynatrace JS3 | Cohesity Helios Interface DOWN validator
// GET ONLY | SNOW payload ready
// TEAM = ONE INCIDENT PER CLUSTER
// DC   = ONE INCIDENT PER LOCATION + CLUSTER
// ------------------------------------------------------------
import { credentialVaultClient as vault } from "@dynatrace-sdk/client-classic-environment-v2";
import { result as wfResult } from "@dynatrace-sdk/automation-utils";
export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const PREDECESSOR_TASK = "get_alerts";
  // ------------------------------------------------------------
  // DC Allowlist
  // ------------------------------------------------------------
  const DC_ALLOWLIST_ENABLED = true;
  const DC_ALLOWLIST = [
    "San Antonio",
    "Carrollton",
    "Detroit",
    "Ashburn"
  ];
  // ------------------------------------------------------------
  // Trigger Codes
  // ------------------------------------------------------------
  const TRIGGER_CODES = [
    "1105",
    "13023",
    "CE030601105",
    "CE02513023"
  ];
  // ------------------------------------------------------------
  // Vault
  // ------------------------------------------------------------
  const vaultName = "Cohesity_API_Key";
  const vaultId = "CREDENTIALS_VAULT-7F2FF2BB6BCD9B63";
  // ------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------
  const norm = (v) =>
    (v === null || v === undefined)
      ? ""
      : String(v).trim();
  const normLoc = (s) =>
    norm(s)
      .toLowerCase()
      .replace(/\s+/g, " ");
  const slug = (s) =>
    norm(s)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "");
  function toArray(v) {
    if (Array.isArray(v)) return v;
    if (Array.isArray(v?.results)) return v.results;
    if (Array.isArray(v?.result?.results)) return v.result.results;
    if (Array.isArray(v?.result)) return v.result;
    if (Array.isArray(v?.items)) return v.items;
    if (Array.isArray(v?.records)) return v.records;
    return [];
  }
  function isTrigger(code) {
    code = norm(code);
    if (!code) return false;
    if (code === "1105") return true;
    if (code === "13023") return true;
    if (code === "CE030601105") return true;
    if (code === "CE02513023") return true;
    if (code.includes("1105")) return true;
    if (code.includes("13023")) return true;
    return false;
  }
  function pickIP(r) {
    return norm(
      r?.IP ||
      r?.ip ||
      r?.NodeIP ||
      r?.nodeIp ||
      r?.node_ip ||
      r?.NodeIp ||
      r?.IPAddress ||
      r?.IpAddress ||
      r?.ipAddress
    );
  }
  async function getJson(url, headers) {
    const resp = await fetch(url, {
      method: "GET",
      headers
    });
    if (!resp.ok) {
      return null;
    }
    return resp.json();
  }
  function indexDetails(details) {
    const map = {};
    const arr = Array.isArray(details) ? details : (details ? [details] : []);
    for (const d of arr) {
      const name = norm(
        d?.name ||
        d?.["@name"] ||
        d?.ifaceName ||
        d?.interfaceName ||
        d?.interface ||
        d?.nicName
      );
      if (name) {
        map[name] = d;
      }
    }
    return map;
  }
  function looksDown(v) {
    return norm(v)
      .toLowerCase()
      .includes("down");
  }
  // ------------------------------------------------------------
  // DC Assignment Group Mapping
  // ------------------------------------------------------------
  function getAssignmentGroup(loc) {
    const l = normLoc(loc);
    if (l.includes("ashburn")) {
      return "ALLY - FACILITIES DATA CENTER ASHBURN DC2";
    }
    if (l.includes("detroit")) {
      return "ALLY - HOSTING BACKUP";
    }
    if (l.includes("san antonio")) {
      return "ALLY - FACILITIES DATA CENTER SAN ANTONIO";
    }
    if (l.includes("carrollton")) {
      return "ALLY - FACILITIES DATA CENTER CARROLLTON";
    }
    return null;
  }
  // ------------------------------------------------------------
  // Read JS1 Results
  // ------------------------------------------------------------
  let upstream;
  try {
    upstream = await wfResult(PREDECESSOR_TASK);
  } catch (e) {
    return {
      ok: false,
      error: `Cannot read result("${PREDECESSOR_TASK}")`,
      teamIncidents: [],
      dcIncidents: []
    };
  }
  const alertRows =
    toArray(
      upstream?.results ??
      upstream
    );
  const interfaceTargets = [];
  const alertCauseByKey = {};
  for (const r of alertRows) {
    const alertCode = norm(
      r?.AlertCode ||
      r?.alertCode ||
      r?.AlertType ||
      r?.alertType ||
      r?.Code ||
      r?.code
    );
    if (!isTrigger(alertCode)) {
      continue;
    }
    const clusterId = norm(
      r?.ClusterId ||
      r?.clusterId ||
      r?.ClusterID ||
      r?.cluster_id
    );
    const ip =
      pickIP(r);
    if (!clusterId || !ip) {
      continue;
    }
    const clusterName = norm(
      r?.ClusterName ||
      r?.clusterName ||
      r?.cluster ||
      r?.Cluster
    );
    const location = norm(
      r?.Location ||
      r?.location ||
      r?.Site ||
      r?.site ||
      r?.DataCenter ||
      r?.dataCenter
    );
    const alertCause = norm(
      r?.AlertCause ||
      r?.alertCause ||
      r?.Cause ||
      r?.cause ||
      r?.AlertSummary ||
      r?.summary
    );
    interfaceTargets.push({
      ClusterId:
        clusterId,
      ClusterName:
        clusterName,
      Location:
        location,
      IP:
        ip,
      AlertCode:
        alertCode,
      AlertCause:
        alertCause
    });
    const key =
      `${clusterId}|${ip}`;
    if (alertCause && !alertCauseByKey[key]) {
      alertCauseByKey[key] = alertCause;
    }
  }
  if (!interfaceTargets.length) {
    return {
      ok: true,
      message:
        "No trigger alerts found.",
      downRows: [],
      teamIncidents: [],
      dcIncidents: [],
      dcAllowlistEnabled:
        DC_ALLOWLIST_ENABLED,
      dcAllowlist:
        DC_ALLOWLIST
    };
  }
  // ------------------------------------------------------------
  // Group By Cluster
  // ------------------------------------------------------------
  const byCluster = {};
  for (const t of interfaceTargets) {
    const cid = t.ClusterId;
    if (!byCluster[cid]) {
      byCluster[cid] = {
        ClusterId:
          cid,
        ClusterName:
          t.ClusterName,
        Location:
          t.Location,
        Ips:
          {}
      };
    }
    byCluster[cid].Ips[t.IP] = true;
  }
  const clusterIds =
    Object.keys(byCluster);
  // ------------------------------------------------------------
  // API Key
  // ------------------------------------------------------------
  let apiKey = null;
  async function getApiKeyByName(name) {
    const all =
      await vault.getCredentials();
    const creds =
      (
        all &&
        all.credentials
      )
        ? all.credentials
        : [];
    const found =
      creds.find((c) =>
        c &&
        c.name === name
      );
    if (!found) {
      return null;
    }
    const detail =
      await vault.getCredentialsDetails({
        id:
          found.id
      });
    return (
      detail &&
      (
        detail.token ||
        detail.password
      )
    ) || null;
  }
  try {
    apiKey =
      await getApiKeyByName(vaultName);
    if (!apiKey) {
      throw new Error("not-found");
    }
  } catch {
    try {
      const d2 =
        await vault.getCredentialsDetails({
          id:
            vaultId
        });
      apiKey =
        (
          d2 &&
          (
            d2.token ||
            d2.password
          )
        ) || null;
    } catch {
      return {
        ok: false,
        error:
          "No valid Helios API key available.",
        teamIncidents:
          [],
        dcIncidents:
          []
      };
    }
  }
  if (!apiKey) {
    return {
      ok: false,
      error:
        "No valid Helios API key available.",
      teamIncidents:
        [],
      dcIncidents:
        []
    };
  }
  // ------------------------------------------------------------
  // Interface API
  // ------------------------------------------------------------
  const ifaceUrl =
    baseUrl +
    "/irisservices/api/v1/public/interface" +
    "?bondInterfaceOnly=true" +
    "&ifaceGroupAssignedOnly=true" +
    "&includeUplinkSwitchInfo=true" +
    "&includeBondSlaveDetails=true";
  const downRows = [];

  // ------------------------------------------------------------
  // Validate Interfaces
  // ------------------------------------------------------------
  for (const cid of clusterIds) {
    const entry = byCluster[cid];
    let data;

    try {
      data =
        await getJson(
          ifaceUrl,
          {
            accept:
              "application/json",
            apiKey,
            accessClusterId:
              String(cid)
          }
        );
    } catch {
      continue;
    }

    if (!data) {
      continue;
    }

    const nodes =
      Array.isArray(data)
        ? data
        : (
            Array.isArray(data?.nodes)
              ? data.nodes
              : (
                  Array.isArray(data?.result)
                    ? data.result
                    : [data]
                )
          );

    for (const node of nodes) {
      const nodeIp = norm(
        node?.nodeIp ||
        node?.NodeIP ||
        node?.ip ||
        node?.IP
      );

      if (!entry.Ips[nodeIp]) {
        continue;
      }

      const serial = norm(
        node?.chassisSerial ||
        node?.serial ||
        node?.Serial ||
        node?.nodeSerial
      );

      const ifaces =
        Array.isArray(node?.interfaces)
          ? node.interfaces
          : (
              Array.isArray(node?.bondInterfaces)
                ? node.bondInterfaces
                : []
            );

      for (const bond of ifaces) {
        const bondName = norm(
          bond?.name ||
          bond?.Name ||
          bond?.bondName ||
          bond?.interfaceName
        );

        const slaves =
          Array.isArray(bond?.bondSlaves)
            ? bond.bondSlaves
            : (
                bond?.bondSlaves
                  ? [bond.bondSlaves]
                  : []
              );

        const dmap =
          indexDetails(
            bond?.bondSlavesDetails
          );

        for (const s of slaves) {
          const slaveName = norm(
            typeof s === "string"
              ? s
              : (
                  s?.name ||
                  s?.["@name"] ||
                  s?.ifaceName ||
                  s?.interfaceName
                )
          );

          if (!slaveName) {
            continue;
          }

          const d =
            dmap[slaveName] || {};

          const linkState = norm(
            d.linkState ||
            d.state ||
            d.status
          );

          if (!looksDown(linkState)) {
            continue;
          }

          const mac = norm(
            d.macAddr ||
            d.mac ||
            d.mac_address ||
            d.macAddress
          );

          const causeKey =
            `${cid}|${nodeIp}`;

          const alertCause =
            alertCauseByKey[causeKey] || "";

          const duplicated =
            downRows.some((r) =>
              r.ClusterId === String(cid) &&
              r.IP === nodeIp &&
              r.BondSlave === slaveName
            );

          if (duplicated) {
            continue;
          }

          downRows.push({
            Location:
              entry.Location,
            ClusterName:
              entry.ClusterName,
            ClusterId:
              String(cid),
            IP:
              nodeIp,
            Serial:
              serial,
            MAC:
              mac,
            BondName:
              bondName,
            BondSlave:
              slaveName,
            LinkState:
              linkState,
            AlertCause:
              alertCause
          });
        }
      }
    }
  }

  // ------------------------------------------------------------
  // No Down Interfaces
  // ------------------------------------------------------------
  if (!downRows.length) {

    return {
      ok: true,
      message:
        "No confirmed DOWN interfaces found.",
      downRows: [],
      teamIncidents: [],
      dcIncidents: [],
      dcAllowlistEnabled:
        DC_ALLOWLIST_ENABLED,
      dcAllowlist:
        DC_ALLOWLIST
    };
  }

  // ------------------------------------------------------------
  // SNOW Formatting
  // ------------------------------------------------------------
  function shortLine(r) {

    return (
      "Cohesity Interface DOWN | " +
      r.ClusterName
      // " | IP: " + r.IP +
      // " | Interface: " + r.BondSlave
    );
  }

  function fullLine(r) {

    const lines = [];

    lines.push(
      "Cluster Name : " + r.ClusterName
    );

    lines.push(
      "Cluster ID   : " + r.ClusterId
    );

    lines.push(
      "Node IP      : " + r.IP
    );

    lines.push(
      "Interface    : " + r.BondSlave
    );

    lines.push(
      "MAC Address  : " + r.MAC
    );

    lines.push(
      "Serial No    : " + r.Serial
    );

    if (r.Location) {

      lines.push(
        "Location     : " + r.Location
      );
    }

    if (r.AlertCause) {

      lines.push(
        "Alert Summary: " + r.AlertCause
      );
    }

    return lines.join("\n");
  }

  // ------------------------------------------------------------
  // Team Incidents
  // ------------------------------------------------------------
  const teamClusterMap = {};

  for (const r of downRows) {

    const cluster =
      r.ClusterName ||
      "Unknown";

    if (!teamClusterMap[cluster]) {
      teamClusterMap[cluster] = [];
    }

    teamClusterMap[cluster].push(r);
  }

  const teamIncidents = [];

  for (const clusterName of Object.keys(teamClusterMap)) {

    const rows =
      teamClusterMap[clusterName];

    const first =
      rows[0];

    const fingerprint =
      rows
        .map(r =>
          r.IP + "|" + r.BondSlave
        )
        .sort()
        .join(",");

    teamIncidents.push({

      cluster:
        clusterName,

      fingerprint:
        fingerprint,

      correlation_id:
        "DT_cohesity_ifdown_" +
        slug(clusterName),

      short_description:
        shortLine(first),

      description:
        rows
          .map(fullLine)
          .join(
            "\n\n--------------------------------\n\n"
          )
    });
  }

  // ------------------------------------------------------------
  // DC Incidents
  // ------------------------------------------------------------
  const allowSet =
    new Set(
      DC_ALLOWLIST.map(normLoc)
    );

  const dcByLocCluster = {};

  for (const r of downRows) {

    const loc =
      norm(r.Location) ||
      "Unknown";

    const cluster =
      norm(r.ClusterName) ||
      "Unknown";

    const allowed =
      !DC_ALLOWLIST_ENABLED ||
      allowSet.has(normLoc(loc));

    if (!allowed) {
      continue;
    }

    const key =
      loc + "|" + cluster;

    if (!dcByLocCluster[key]) {
      dcByLocCluster[key] = [];
    }

    dcByLocCluster[key].push(r);
  }

  const dcIncidents = [];

  for (const key of Object.keys(dcByLocCluster)) {

    const rows =
      dcByLocCluster[key];

    const first =
      rows[0];

    const loc =
      first.Location;

    const cluster =
      first.ClusterName;

    const fingerprint =
      rows
        .map(r =>
          r.IP + "|" + r.BondSlave
        )
        .sort()
        .join(",");

    const assignmentGroup =
      getAssignmentGroup(loc);

    if (!assignmentGroup) {
      continue;
    }

    dcIncidents.push({

      location:
        loc,

      cluster:
        cluster,

      fingerprint:
        fingerprint,

      assignment_group:
        assignmentGroup,

      correlation_id:
        "DT_cohesity_ifdown_dc_" +
        slug(loc) + "_" +
        slug(cluster),

      short_description:
        shortLine(first),

      description:
        rows
          .map(fullLine)
          .join(
            "\n\n--------------------------------\n\n"
          )
    });
  }

  // ------------------------------------------------------------
  // Return
  // ------------------------------------------------------------
  return {
    ok: true,
    downRows,
    teamIncidents,
    dcIncidents,
    dcAllowlistEnabled:
      DC_ALLOWLIST_ENABLED,
    dcAllowlist:
      DC_ALLOWLIST
  };
}
