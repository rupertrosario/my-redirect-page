// Cohesity Helios - Protection Group Status Inventory
// Dynatrace Workflow JavaScript
// STRICTLY READ-ONLY / GET-only
//
// Output:
// {
//   authMode,
//   count,
//   activeCount,
//   pausedCount,
//   deletedCount,
//   rows: [
//     { ClusterName, Environment, ProtectionGroupName, Status }
//   ],
//   clusterWarningCount,
//   clusterWarnings,
//   csvText
// }

import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function () {
  const baseUrl = "https://helios.cohesity.com";

  // -------------------------------------------------------------
  // 1) Credential Vault - same pattern as existing Cohesity JS
  // -------------------------------------------------------------
  const vaultName = "Cohesity_API_Key";
  const vaultId   = "credentials_vault-312312";

  let apiKey = null;
  let authMode = "vault-name";

  async function getKeyByName(name) {
    const all = await credentialVaultClient.getCredentials();
    const found = (all?.credentials || []).find(function (c) {
      return c?.name === name;
    });

    if (!found) return null;

    const detail = await credentialVaultClient.getCredentialsDetails({
      id: found.id
    });

    return (detail && (detail.token || detail.password)) || null;
  }

  try {
    apiKey = await getKeyByName(vaultName);

    if (!apiKey) {
      throw new Error("Credential not found by name.");
    }

    console.log("Helios key loaded from credential vault by name.");
  }
  catch (_) {
    const detail = await credentialVaultClient.getCredentialsDetails({
      id: vaultId
    });

    apiKey = (detail && (detail.token || detail.password)) || null;
    authMode = "vault-id";

    console.log("Helios key loaded from credential vault by id.");
  }

  apiKey = String(apiKey || "").trim();

  if (!apiKey) {
    throw new Error("No Helios API key available from Dynatrace Credential Vault.");
  }

  const commonHeaders = {
    accept: "application/json",
    apiKey: apiKey
  };

  // -------------------------------------------------------------
  // 2) GET-only helper
  // -------------------------------------------------------------
  const REQUEST_TIMEOUT_MS = 45000;

  async function getJson(url, headers) {
    const controller = new AbortController();

    const timeoutId = setTimeout(function () {
      controller.abort();
    }, REQUEST_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        method: "GET",
        headers: headers,
        signal: controller.signal
      });

      if (!response.ok) {
        let text = "";

        try {
          text = await response.text();
        }
        catch (_) {}

        throw new Error(
          "GET " + url +
          " -> HTTP " + response.status +
          (text ? " " + text : "")
        );
      }

      return await response.json();
    }
    catch (e) {
      if (e && e.name === "AbortError") {
        throw new Error(
          "GET timeout after " +
          (REQUEST_TIMEOUT_MS / 1000) +
          " seconds: " +
          url
        );
      }

      throw e;
    }
    finally {
      clearTimeout(timeoutId);
    }
  }

  // -------------------------------------------------------------
  // 3) Helpers
  // -------------------------------------------------------------
  function getClusterName(cluster) {
    return String(
      cluster?.name ||
      cluster?.clusterName ||
      cluster?.displayName ||
      ""
    ).trim() || ("Unknown-" + String(cluster?.clusterId || ""));
  }

  function getEnvironment(pg) {
    let env = null;

    if (pg?.environment) {
      env = pg.environment;
    }
    else if (pg?.environmentType) {
      env = pg.environmentType;
    }
    else if (Array.isArray(pg?.environmentTypes) && pg.environmentTypes.length > 0) {
      env = pg.environmentTypes[0];
    }
    else if (Array.isArray(pg?.environments) && pg.environments.length > 0) {
      env = pg.environments[0];
    }
    else if (pg?.environments) {
      env = pg.environments;
    }

    if (!env) {
      return "Unknown";
    }

    env = String(env).trim();

    if (env.startsWith("k") && env.length > 1) {
      return env.substring(1);
    }

    return env;
  }

  function csvValue(value) {
    const text = String(value ?? "");

    if (
      text.includes(",") ||
      text.includes('"') ||
      text.includes("\n") ||
      text.includes("\r")
    ) {
      return '"' + text.replace(/"/g, '""') + '"';
    }

    return text;
  }

  // -------------------------------------------------------------
  // 4) Get all Helios clusters
  // -------------------------------------------------------------
  const clusterJson = await getJson(
    baseUrl + "/v2/mcm/cluster-mgmt/info",
    commonHeaders
  );

  const clusters = Array.isArray(clusterJson?.cohesityClusters)
    ? clusterJson.cohesityClusters
    : [];

  if (!clusters.length) {
    throw new Error("No clusters returned from Helios.");
  }

  clusters.sort(function (a, b) {
    return getClusterName(a).localeCompare(getClusterName(b));
  });

  // -------------------------------------------------------------
  // 5) Status queries
  //
  // Status is assigned from the explicit query scope.
  // All calls below remain GET-only.
  // -------------------------------------------------------------
  const statusQueries = [
    {
      status: "Active",
      query: "isDeleted=false&isPaused=false&isActive=true"
    },
    {
      status: "Paused",
      query: "isDeleted=false&isPaused=true"
    },
    {
      status: "Deleted",
      query: "isDeleted=true"
    }
  ];

  const rows = [];
  const clusterWarnings = [];

  for (const cluster of clusters) {
    const clusterId = cluster?.clusterId;
    const clusterName = getClusterName(cluster);

    if (!clusterId) {
      clusterWarnings.push({
        Cluster: clusterName,
        Message: "Cluster has no clusterId and was skipped."
      });

      continue;
    }

    const headers = {
      ...commonHeaders,
      accessClusterId: String(clusterId)
    };

    console.log("Processing cluster: " + clusterName);

    for (const item of statusQueries) {
      const url =
        baseUrl +
        "/v2/data-protect/protection-groups?" +
        item.query;

      let pgJson;

      try {
        pgJson = await getJson(url, headers);
      }
      catch (e) {
        clusterWarnings.push({
          Cluster: clusterName,
          Status: item.status,
          Message: e?.message || String(e)
        });

        console.log(
          "Failed " +
          item.status +
          " PG query for " +
          clusterName
        );

        continue;
      }

      const pgs = Array.isArray(pgJson?.protectionGroups)
        ? pgJson.protectionGroups
        : [];

      console.log(
        clusterName +
        " - " +
        item.status +
        ": " +
        pgs.length
      );

      for (const pg of pgs) {
        const pgName = String(pg?.name || "").trim();

        if (!pgName) {
          continue;
        }

        rows.push({
          ClusterName: clusterName,
          Environment: getEnvironment(pg),
          ProtectionGroupName: pgName,
          Status: item.status
        });
      }
    }
  }

  // -------------------------------------------------------------
  // 6) De-duplicate and sort
  // -------------------------------------------------------------
  const uniqueMap = new Map();

  for (const row of rows) {
    const key = [
      row.ClusterName,
      row.Environment,
      row.ProtectionGroupName,
      row.Status
    ].join("|");

    if (!uniqueMap.has(key)) {
      uniqueMap.set(key, row);
    }
  }

  const finalRows = Array.from(uniqueMap.values());

  finalRows.sort(function (a, b) {
    return (
      a.ClusterName.localeCompare(b.ClusterName) ||
      a.Environment.localeCompare(b.Environment) ||
      a.ProtectionGroupName.localeCompare(b.ProtectionGroupName) ||
      a.Status.localeCompare(b.Status)
    );
  });

  // -------------------------------------------------------------
  // 7) Counts
  // -------------------------------------------------------------
  const activeCount = finalRows.filter(function (r) {
    return r.Status === "Active";
  }).length;

  const pausedCount = finalRows.filter(function (r) {
    return r.Status === "Paused";
  }).length;

  const deletedCount = finalRows.filter(function (r) {
    return r.Status === "Deleted";
  }).length;

  // -------------------------------------------------------------
  // 8) CSV text for downstream workflow step
  // -------------------------------------------------------------
  const csvLines = [
    "ClusterName,Environment,ProtectionGroupName,Status"
  ];

  for (const row of finalRows) {
    csvLines.push(
      [
        csvValue(row.ClusterName),
        csvValue(row.Environment),
        csvValue(row.ProtectionGroupName),
        csvValue(row.Status)
      ].join(",")
    );
  }

  const csvText = csvLines.join("\n");

  console.log(
    "PG inventory complete. Total=" +
    finalRows.length +
    " Active=" +
    activeCount +
    " Paused=" +
    pausedCount +
    " Deleted=" +
    deletedCount
  );

  return {
    authMode: authMode,
    count: finalRows.length,
    activeCount: activeCount,
    pausedCount: pausedCount,
    deletedCount: deletedCount,
    rows: finalRows,
    clusterWarningCount: clusterWarnings.length,
    clusterWarnings: clusterWarnings,
    csvText: csvText
  };
}
