// ==========================================================
// Dynatrace JS Task
// Task name: dtsk_get_cluster_map
//
// Purpose:
// - Get all Cohesity clusters from Helios
// - Build cluster list
// - Build clusterId/intId/id -> clusterName map
// - Runs once before the loop validation task
// ==========================================================

import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function () {

  const HELIOS_BASE_URL = "https://helios.cohesity.com";
  const COHESITY_API_KEY_CREDENTIAL_ID = "credentials_vault-312312";

  const cohesityCred = await credentialVaultClient.getCredentialsDetails({
    id: COHESITY_API_KEY_CREDENTIAL_ID
  });

  const COHESITY_API_KEY = cohesityCred?.token;

  if (!COHESITY_API_KEY) {
    throw new Error("Cohesity API key token was not returned from Dynatrace Credential Vault.");
  }

  function toText(value) {
    if (value === null || value === undefined) return "";

    if (Array.isArray(value)) {
      for (const item of value) {
        const t = toText(item);
        if (t) return t;
      }
      return "";
    }

    if (typeof value === "object") {
      const candidates = [
        value.display_value,
        value.displayName,
        value.name,
        value.value,
        value.id,
        value.clusterId,
        value.clusterID,
        value.intId
      ];

      for (const c of candidates) {
        const t = toText(c);
        if (t) return t;
      }

      return "";
    }

    return String(value).trim();
  }

  function firstNonBlank(...values) {
    for (const v of values) {
      const t = toText(v);
      if (t) return t;
    }
    return "";
  }

  function addClusterMapEntry(clusterMap, idValue, clusterName) {
    const id = toText(idValue);

    if (!id) return;

    clusterMap[id] = clusterName;
  }

  async function getJson(url, headers) {
    const response = await fetch(url, {
      method: "GET",
      headers
    });

    if (!response.ok) {
      throw new Error(
        `GET failed: ${response.status} ${response.statusText} URL=${url}`
      );
    }

    return await response.json();
  }

  const clusterUrl = `${HELIOS_BASE_URL}/v2/mcm/cluster-mgmt/info`;

  const clusterJson = await getJson(clusterUrl, {
    accept: "application/json",
    apiKey: COHESITY_API_KEY
  });

  const rawClusters = Array.isArray(clusterJson?.cohesityClusters)
    ? clusterJson.cohesityClusters
    : [];

  if (rawClusters.length === 0) {
    throw new Error("No clusters returned from Helios /v2/mcm/cluster-mgmt/info.");
  }

  const clusters = [];
  const clusterMap = {};

  for (const c of rawClusters) {

    const clusterId = firstNonBlank(
      c.clusterId,
      c.id,
      c.intId,
      c.clusterIdentifier?.clusterId,
      c.clusterInfo?.clusterId
    );

    const clusterName = firstNonBlank(
      c.clusterName,
      c.name,
      c.displayName
    ) || `Unknown-${clusterId}`;

    if (!clusterId) {
      continue;
    }

    clusters.push({
      clusterId: String(clusterId),
      clusterName: String(clusterName)
    });

    // Multiple possible IDs can appear in Cohesity API responses.
    // Map all known ID forms back to the same cluster name.
    addClusterMapEntry(clusterMap, c.clusterId, clusterName);
    addClusterMapEntry(clusterMap, c.id, clusterName);
    addClusterMapEntry(clusterMap, c.intId, clusterName);
    addClusterMapEntry(clusterMap, c.clusterIdentifier?.clusterId, clusterName);
    addClusterMapEntry(clusterMap, c.clusterInfo?.clusterId, clusterName);
  }

  const summary = {
    clustersLoaded: clusters.length,
    clusterMapEntries: Object.keys(clusterMap).length
  };

  console.log("==== COHESITY CLUSTER MAP SUMMARY ====");
  console.log(JSON.stringify(summary, null, 2));

  console.log("==== CLUSTERS ====");
  console.log(JSON.stringify(clusters, null, 2));

  return {
    clusters,
    clusterMap,
    summary
  };
}
