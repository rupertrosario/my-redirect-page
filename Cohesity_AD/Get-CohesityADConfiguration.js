import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Cohesity Active Directory Configuration Report
 *
 * Multi-cluster | Helios | GET-only | Dynatrace JavaScript action
 *
 * Output columns:
 * Cluster, ADConfigured, DomainName, OrganizationalUnit, WorkGroupName,
 * MachineAccounts, PreferredDomainControllers, DomainControllersDenyList,
 * TrustedDomains, ADConfigurationId
 */
export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const MAX_ROWS = 1000;

  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";
  let apiKey = null;

  async function getKeyByName(name) {
    const all = await credentialVaultClient.getCredentials();
    const credentials = all?.credentials || [];
    const found = credentials.find(
      (credential) => credential?.name === name
    );

    if (!found) return null;

    const detail =
      await credentialVaultClient.getCredentialsDetails({
        id: found.id
      });

    return detail?.token || detail?.password || null;
  }

  try {
    apiKey = await getKeyByName(vaultName);

    if (!apiKey) {
      throw new Error("Vault name lookup returned no key");
    }
  } catch {
    const detail =
      await credentialVaultClient.getCredentialsDetails({
        id: vaultId
      });

    apiKey = detail?.token || detail?.password || null;
  }

  if (!apiKey) {
    return {
      error: "No Cohesity API key available"
    };
  }

  const commonHeaders = {
    accept: "application/json",
    apiKey
  };

  const norm = (value) =>
    value === null || value === undefined
      ? ""
      : String(value).trim();

  const valueOrNA = (value) => {
    if (value === null || value === undefined) {
      return "N/A";
    }

    if (Array.isArray(value)) {
      const items = value
        .map((item) => norm(item))
        .filter(Boolean);

      return items.length ? items.join(", ") : "N/A";
    }

    const text = norm(value);
    return text || "N/A";
  };

  const toArray = (value) =>
    !value ? [] : Array.isArray(value) ? value : [value];

  const safeCell = (value) =>
    value === null || value === undefined
      ? ""
      : String(value)
          .replace(/\|/g, " ")
          .replace(/\r?\n/g, " ");

  function mdTable(headers, rows) {
    const header = `| ${headers.join(" | ")} |`;
    const separator =
      `| ${headers.map(() => "---").join(" | ")} |`;

    const body = rows.map(
      (row) =>
        `| ${headers
          .map((key) => safeCell(row[key]))
          .join(" | ")} |`
    );

    return [header, separator, ...body].join("\n");
  }

  function formatNameStatusList(items) {
    const values = [];

    for (const item of toArray(items)) {
      const name =
        norm(item?.name) ||
        norm(item?.dnsHostName) ||
        norm(item?.hostName);

      const status =
        norm(item?.status) ||
        norm(item?.state);

      if (name && status) {
        values.push(`${name} [${status}]`);
      } else if (name) {
        values.push(name);
      } else if (status) {
        values.push(`Status=${status}`);
      }
    }

    return values.length ? values.join("; ") : "N/A";
  }

  function formatMachineAccounts(machineAccounts) {
    const values = [];

    for (const account of toArray(machineAccounts)) {
      const name = norm(account?.name);
      const dnsName = norm(account?.dnsHostName);
      const parts = [];

      if (name) parts.push(name);

      if (dnsName && dnsName !== name) {
        parts.push(`DNS=${dnsName}`);
      }

      if (parts.length) {
        values.push(parts.join("; "));
      }
    }

    return values.length ? values.join(" | ") : "N/A";
  }

  function formatTrustedDomains(trustedDomainParams) {
    if (!trustedDomainParams) return "N/A";

    const values = [];

    for (
      const trustedDomain of
      toArray(trustedDomainParams?.trustedDomains)
    ) {
      const domainName = norm(trustedDomain?.domainName);

      if (domainName && !values.includes(domainName)) {
        values.push(domainName);
      }
    }

    for (
      const domain of
      toArray(trustedDomainParams?.whitelistedDomains)
    ) {
      const domainName = norm(domain);

      if (domainName && !values.includes(domainName)) {
        values.push(domainName);
      }
    }

    return values.length ? values.join(", ") : "N/A";
  }

  function emptyRow(cluster, adConfigured) {
    return {
      Cluster: cluster,
      ADConfigured: adConfigured,
      DomainName: "N/A",
      OrganizationalUnit: "N/A",
      WorkGroupName: "N/A",
      MachineAccounts: "N/A",
      PreferredDomainControllers: "N/A",
      DomainControllersDenyList: "N/A",
      TrustedDomains: "N/A",
      ADConfigurationId: "N/A"
    };
  }

  async function getJsonSafe(url, headers) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers
      });

      if (!response.ok) {
        let text = "";

        try {
          text = await response.text();
        } catch {}

        return {
          ok: false,
          status: response.status,
          error: text,
          data: null
        };
      }

      return {
        ok: true,
        status: response.status,
        error: "",
        data: await response.json()
      };
    } catch (error) {
      return {
        ok: false,
        status: 0,
        error: String(error),
        data: null
      };
    }
  }

  const clusterResponse = await getJsonSafe(
    `${baseUrl}/v2/mcm/cluster-mgmt/info`,
    commonHeaders
  );

  if (!clusterResponse.ok) {
    return {
      error: "Failed to fetch clusters",
      status: clusterResponse.status,
      details: clusterResponse.error
    };
  }

  const clusterData = clusterResponse.data || {};

  const clusters = toArray(
    clusterData.cohesityClusters ||
      clusterData.clusters ||
      clusterData.clusterInfos ||
      clusterData?.mcmInfo?.clusterInfos
  ).sort((a, b) => {
    const aName =
      norm(a?.clusterName) ||
      norm(a?.name) ||
      norm(a?.displayName);

    const bName =
      norm(b?.clusterName) ||
      norm(b?.name) ||
      norm(b?.displayName);

    return aName.localeCompare(
      bName,
      undefined,
      { sensitivity: "base" }
    );
  });

  const rows = [];
  const errors = [];
  let configuredClusterCount = 0;
  let notConfiguredClusterCount = 0;

  for (const cluster of clusters) {
    const clusterId =
      norm(cluster?.clusterId) ||
      norm(cluster?.id);

    const clusterName =
      norm(cluster?.clusterName) ||
      norm(cluster?.name) ||
      norm(cluster?.displayName) ||
      (clusterId ? `Cluster-${clusterId}` : "Unknown");

    if (!clusterId) {
      rows.push(emptyRow(clusterName, "Unknown"));

      errors.push({
        cluster: clusterName,
        status: 0,
        error: "Cluster ID missing"
      });

      continue;
    }

    const headers = {
      ...commonHeaders,
      accessClusterId: clusterId
    };

    const adResponse = await getJsonSafe(
      `${baseUrl}/v2/active-directories?includeTenants=true`,
      headers
    );

    if (!adResponse.ok) {
      rows.push(emptyRow(clusterName, "Unknown"));

      errors.push({
        cluster: clusterName,
        status: adResponse.status,
        error: adResponse.error
      });

      continue;
    }

    const adData = adResponse.data;
    let activeDirectories = [];

    if (Array.isArray(adData)) {
      activeDirectories = adData.filter(Boolean);
    } else if (Array.isArray(adData?.activeDirectories)) {
      activeDirectories =
        adData.activeDirectories.filter(Boolean);
    } else if (
      adData &&
      (
        adData.domainName ||
        adData.connectionId ||
        adData.id
      )
    ) {
      activeDirectories = [adData];
    }

    if (!activeDirectories.length) {
      rows.push(emptyRow(clusterName, "No"));
      notConfiguredClusterCount++;
      continue;
    }

    configuredClusterCount++;

    for (const activeDirectory of activeDirectories) {
      rows.push({
        Cluster: clusterName,
        ADConfigured: "Yes",
        DomainName:
          valueOrNA(activeDirectory?.domainName),
        OrganizationalUnit:
          valueOrNA(
            activeDirectory?.organizationalUnitName
          ),
        WorkGroupName:
          valueOrNA(activeDirectory?.workGroupName),
        MachineAccounts:
          formatMachineAccounts(
            activeDirectory?.machineAccounts
          ),
        PreferredDomainControllers:
          formatNameStatusList(
            activeDirectory?.preferredDomainControllers
          ),
        DomainControllersDenyList:
          valueOrNA(
            activeDirectory?.domainControllersDenyList
          ),
        TrustedDomains:
          formatTrustedDomains(
            activeDirectory?.trustedDomainParams
          ),
        ADConfigurationId:
          valueOrNA(activeDirectory?.id)
      });
    }
  }

  rows.sort(
    (a, b) =>
      a.Cluster.localeCompare(
        b.Cluster,
        undefined,
        { sensitivity: "base" }
      ) ||
      a.DomainName.localeCompare(
        b.DomainName,
        undefined,
        { sensitivity: "base" }
      )
  );

  const reportDate = new Date().toLocaleDateString(
    "en-US",
    {
      timeZone: "America/New_York",
      year: "numeric",
      month: "short",
      day: "2-digit"
    }
  );

  const headers = [
    "Cluster",
    "ADConfigured",
    "DomainName",
    "OrganizationalUnit",
    "WorkGroupName",
    "MachineAccounts",
    "PreferredDomainControllers",
    "DomainControllersDenyList",
    "TrustedDomains",
    "ADConfigurationId"
  ];

  const renderedRows = rows.slice(0, MAX_ROWS);

  const markdownEmail = [
    `### Cohesity Active Directory Configuration — ${reportDate}`,
    "",
    mdTable(headers, renderedRows),
    rows.length > MAX_ROWS
      ? `_Note: Report limited to ${MAX_ROWS} of ${rows.length} rows._`
      : null,
    errors.length
      ? `_Warning: ${errors.length} cluster query error(s). See the errors output._`
      : null
  ]
    .filter(
      (part) => part !== null && part !== undefined
    )
    .join("\n");

  return {
    clusterCount: clusters.length,
    configuredClusterCount,
    notConfiguredClusterCount,
    failedClusterCount: errors.length,
    rowCount: rows.length,
    rows,
    markdownEmail,
    errors
  };
}
