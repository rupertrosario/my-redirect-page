import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Cohesity Cluster Security Configuration and Password Compliance Inventory
 *
 * GET-only API flow:
 * 1. GET /v2/mcm/cluster-mgmt/info
 * 2. GET /v2/security-config
 *    Runs once per cluster with accessClusterId.
 *
 * No POST, PUT, PATCH, or DELETE request is used.
 */
export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";

  const standard = Object.freeze({
    passwordMinLength: 15,
    complexityRequired: 3,
    disallowedOldPasswords: 6,
    passwordMinLifetimeDays: 2,
    passwordMaxLifetimeDays: 365,
    pciPasswordMaxLifetimeDays: 90
  });

  const columns = [
    "Cluster",
    "PasswordMinLength",
    "PasswordIncludeUpperLetter",
    "PasswordIncludeLowerLetter",
    "PasswordIncludeNumber",
    "PasswordIncludeSpecialChar",
    "NumDisallowedOldPasswords",
    "NumDifferentChars",
    "PasswordMinLifetimeDays",
    "PasswordMaxLifetimeDays",
    "MaxFailedLoginAttempts",
    "FailedLoginLockTimeDurationMins",
    "AccountInactivityTimeDays",
    "AuthTokenTimeoutMinutes",
    "UIInactivityTimeoutMSecs",
    "SessionManagementEnabled",
    "SessionAbsoluteTimeoutSeconds",
    "SessionInactivityTimeoutSeconds",
    "LimitSessions",
    "SessionLimitPerUser",
    "SessionLimitSystemWide",
    "CertificateMappingAuthenticationEnabled",
    "CertificateMapping",
    "CertificateADMapping",
    "IsDataClassified",
    "ClassifiedDataMessage",
    "UnclassifiedDataMessage",
    "SSHTimeoutInMins",
    "PasswordComplexityEnabledCount",
    "PasswordLengthStatus",
    "PasswordComplexityStatus",
    "PasswordHistoryStatus",
    "PasswordMinLifetimeStatus",
    "PasswordMaxLifetime365Status",
    "MeetsPCI90DayValue",
    "OverallPasswordPolicyStatus",
    "ComplianceFindings"
  ];

  const markdownSections = [
    {
      title: "Password Standard - Actual Values",
      columns: [
        ["Cluster", "Cluster"],
        ["Min Length", "PasswordMinLength"],
        ["Complexity", "PasswordComplexityEnabledCount"],
        ["History", "NumDisallowedOldPasswords"],
        ["Min Age Days", "PasswordMinLifetimeDays"],
        ["Max Age Days", "PasswordMaxLifetimeDays"],
        ["Overall", "OverallPasswordPolicyStatus"]
      ]
    },
    {
      title: "Password Standard - Compliance",
      columns: [
        ["Cluster", "Cluster"],
        ["Length", "PasswordLengthStatus"],
        ["Complexity", "PasswordComplexityStatus"],
        ["History", "PasswordHistoryStatus"],
        ["Min Age", "PasswordMinLifetimeStatus"],
        ["Max Age 365", "PasswordMaxLifetime365Status"],
        ["PCI 90 Value", "MeetsPCI90DayValue"]
      ]
    },
    {
      title: "Password Compliance Findings",
      columns: [
        ["Cluster", "Cluster"],
        ["Overall", "OverallPasswordPolicyStatus"],
        ["Findings", "ComplianceFindings"]
      ]
    },
    {
      title: "Password Strength",
      columns: [
        ["Cluster", "Cluster"],
        ["Min Length", "PasswordMinLength"],
        ["Upper", "PasswordIncludeUpperLetter"],
        ["Lower", "PasswordIncludeLowerLetter"],
        ["Number", "PasswordIncludeNumber"],
        ["Special", "PasswordIncludeSpecialChar"]
      ]
    },
    {
      title: "Password Reuse and Lifetime",
      columns: [
        ["Cluster", "Cluster"],
        ["Old Passwords", "NumDisallowedOldPasswords"],
        ["Different Chars", "NumDifferentChars"],
        ["Min Life Days", "PasswordMinLifetimeDays"],
        ["Max Life Days", "PasswordMaxLifetimeDays"]
      ]
    },
    {
      title: "Account Lockout and General Timeouts",
      columns: [
        ["Cluster", "Cluster"],
        ["Max Failed", "MaxFailedLoginAttempts"],
        ["Lock Mins", "FailedLoginLockTimeDurationMins"],
        ["Inactive Days", "AccountInactivityTimeDays"],
        ["Token Mins", "AuthTokenTimeoutMinutes"],
        ["UI Timeout ms", "UIInactivityTimeoutMSecs"],
        ["SSH Mins", "SSHTimeoutInMins"]
      ]
    },
    {
      title: "Session Management",
      columns: [
        ["Cluster", "Cluster"],
        ["Enabled", "SessionManagementEnabled"],
        ["Absolute sec", "SessionAbsoluteTimeoutSeconds"],
        ["Inactive sec", "SessionInactivityTimeoutSeconds"],
        ["Limit Sessions", "LimitSessions"],
        ["Per User", "SessionLimitPerUser"],
        ["System Wide", "SessionLimitSystemWide"]
      ]
    },
    {
      title: "Certificate Authentication",
      columns: [
        ["Cluster", "Cluster"],
        ["Mapping Auth", "CertificateMappingAuthenticationEnabled"],
        ["Certificate Mapping", "CertificateMapping"],
        ["AD Mapping", "CertificateADMapping"]
      ]
    },
    {
      title: "Data Classification",
      columns: [
        ["Cluster", "Cluster"],
        ["Classified", "IsDataClassified"],
        ["Classified Message", "ClassifiedDataMessage"],
        ["Unclassified Message", "UnclassifiedDataMessage"]
      ]
    }
  ];

  const toArray = (value) => {
    if (value === null || value === undefined) return [];
    return Array.isArray(value) ? value : [value];
  };

  const valueOrNA = (value) => {
    if (value === null || value === undefined) return "N/A";
    if (typeof value === "boolean") return value ? "True" : "False";

    const text = String(value).trim();
    return text === "" ? "N/A" : text;
  };

  const firstValue = (object, propertyNames) => {
    if (!object) return "N/A";

    for (const propertyName of propertyNames) {
      const value = valueOrNA(object[propertyName]);
      if (value !== "N/A") return value;
    }

    return "N/A";
  };

  const toFiniteNumber = (value) => {
    if (value === null || value === undefined || value === "N/A") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  };

  const toBoolean = (value) => {
    if (typeof value === "boolean") return value;
    if (String(value).toLowerCase() === "true") return true;
    if (String(value).toLowerCase() === "false") return false;
    return null;
  };

  const statusForMinimum = (value, minimum) => {
    const number = toFiniteNumber(value);
    if (number === null) return "Not Assessed";
    return number >= minimum ? "Compliant" : "Non-Compliant";
  };

  const statusForMaximum = (value, maximum) => {
    const number = toFiniteNumber(value);
    if (number === null) return "Not Assessed";
    return number > 0 && number <= maximum
      ? "Compliant"
      : "Non-Compliant";
  };

  const pciValueStatus = (value) => {
    const number = toFiniteNumber(value);
    if (number === null) return "Not Assessed";
    return number > 0 && number <= standard.pciPasswordMaxLifetimeDays
      ? "Yes"
      : "No";
  };

  const addSoftBreaksToLongTokens = (text) =>
    text.replace(/\S{33,}/g, (token) =>
      token.match(/.{1,32}/g).join("\u200B")
    );

  const safeMarkdownCell = (value) => {
    const cleaned = valueOrNA(value)
      .replace(/\|/g, " / ")
      .replace(/\r?\n/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    return addSoftBreaksToLongTokens(cleaned);
  };

  function markdownTable(columnDefinitions, rows) {
    const labels = columnDefinitions.map(([label]) => label);
    const keys = columnDefinitions.map(([, key]) => key);

    const headerRow = `| ${labels.join(" | ")} |`;
    const separatorRow = `| ${labels.map(() => "---").join(" | ")} |`;
    const dataRows = rows.map(
      (row) =>
        `| ${keys
          .map((key) => safeMarkdownCell(row[key]))
          .join(" | ")} |`
    );

    return [headerRow, separatorRow, ...dataRows].join("\n");
  }

  function evaluatePasswordPolicy(row) {
    const complexityValues = [
      row.PasswordIncludeUpperLetter,
      row.PasswordIncludeLowerLetter,
      row.PasswordIncludeNumber,
      row.PasswordIncludeSpecialChar
    ].map(toBoolean);

    const complexityKnown = complexityValues.every(
      (value) => value !== null
    );
    const complexityEnabledCount = complexityKnown
      ? complexityValues.filter(Boolean).length
      : null;

    const passwordLengthStatus = statusForMinimum(
      row.PasswordMinLength,
      standard.passwordMinLength
    );
    const passwordComplexityStatus = !complexityKnown
      ? "Not Assessed"
      : complexityEnabledCount >= standard.complexityRequired
        ? "Compliant"
        : "Non-Compliant";
    const passwordHistoryStatus = statusForMinimum(
      row.NumDisallowedOldPasswords,
      standard.disallowedOldPasswords
    );
    const passwordMinLifetimeStatus = statusForMinimum(
      row.PasswordMinLifetimeDays,
      standard.passwordMinLifetimeDays
    );
    const passwordMaxLifetime365Status = statusForMaximum(
      row.PasswordMaxLifetimeDays,
      standard.passwordMaxLifetimeDays
    );

    const requiredStatuses = [
      passwordLengthStatus,
      passwordComplexityStatus,
      passwordHistoryStatus,
      passwordMinLifetimeStatus,
      passwordMaxLifetime365Status
    ];

    const overallStatus = requiredStatuses.includes("Not Assessed")
      ? "Not Assessed"
      : requiredStatuses.every((status) => status === "Compliant")
        ? "Compliant"
        : "Non-Compliant";

    const findings = [];

    const minLength = toFiniteNumber(row.PasswordMinLength);
    if (passwordLengthStatus === "Not Assessed") {
      findings.push("Password minimum length was not returned");
    } else if (passwordLengthStatus === "Non-Compliant") {
      findings.push(
        `Minimum length ${minLength} is below ${standard.passwordMinLength}`
      );
    }

    if (passwordComplexityStatus === "Not Assessed") {
      findings.push("One or more complexity flags were not returned");
    } else if (passwordComplexityStatus === "Non-Compliant") {
      findings.push(
        `Complexity ${complexityEnabledCount} of 4 is below ${standard.complexityRequired} of 4`
      );
    }

    const history = toFiniteNumber(row.NumDisallowedOldPasswords);
    if (passwordHistoryStatus === "Not Assessed") {
      findings.push("Password history was not returned");
    } else if (passwordHistoryStatus === "Non-Compliant") {
      findings.push(
        `Password history ${history} is below ${standard.disallowedOldPasswords}`
      );
    }

    const minAge = toFiniteNumber(row.PasswordMinLifetimeDays);
    if (passwordMinLifetimeStatus === "Not Assessed") {
      findings.push("Minimum password age was not returned");
    } else if (passwordMinLifetimeStatus === "Non-Compliant") {
      findings.push(
        `Minimum password age ${minAge} days is below ${standard.passwordMinLifetimeDays}`
      );
    }

    const maxAge = toFiniteNumber(row.PasswordMaxLifetimeDays);
    if (passwordMaxLifetime365Status === "Not Assessed") {
      findings.push("Maximum password age was not returned");
    } else if (passwordMaxLifetime365Status === "Non-Compliant") {
      findings.push(
        maxAge <= 0
          ? "Maximum password age is not enabled"
          : `Maximum password age ${maxAge} days exceeds ${standard.passwordMaxLifetimeDays}`
      );
    }

    return {
      PasswordComplexityEnabledCount:
        complexityEnabledCount === null
          ? "N/A"
          : `${complexityEnabledCount} of 4`,
      PasswordLengthStatus: passwordLengthStatus,
      PasswordComplexityStatus: passwordComplexityStatus,
      PasswordHistoryStatus: passwordHistoryStatus,
      PasswordMinLifetimeStatus: passwordMinLifetimeStatus,
      PasswordMaxLifetime365Status: passwordMaxLifetime365Status,
      MeetsPCI90DayValue: pciValueStatus(row.PasswordMaxLifetimeDays),
      OverallPasswordPolicyStatus: overallStatus,
      ComplianceFindings: findings.length ? findings.join("; ") : "None"
    };
  }

  function emptyRow(clusterName) {
    const row = { Cluster: clusterName };
    for (const column of columns.slice(1)) {
      row[column] = "N/A";
    }

    row.PasswordComplexityEnabledCount = "N/A";
    row.PasswordLengthStatus = "Not Assessed";
    row.PasswordComplexityStatus = "Not Assessed";
    row.PasswordHistoryStatus = "Not Assessed";
    row.PasswordMinLifetimeStatus = "Not Assessed";
    row.PasswordMaxLifetime365Status = "Not Assessed";
    row.MeetsPCI90DayValue = "Not Assessed";
    row.OverallPasswordPolicyStatus = "Not Assessed";
    row.ComplianceFindings = "Cluster security configuration was not returned";
    return row;
  }

  function securityRow(clusterName, securityConfig) {
    const row = {
      Cluster: clusterName,
      PasswordMinLength: valueOrNA(securityConfig?.passwordStrength?.minLength),
      PasswordIncludeUpperLetter: valueOrNA(
        securityConfig?.passwordStrength?.includeUpperLetter
      ),
      PasswordIncludeLowerLetter: valueOrNA(
        securityConfig?.passwordStrength?.includeLowerLetter
      ),
      PasswordIncludeNumber: valueOrNA(
        securityConfig?.passwordStrength?.includeNumber
      ),
      PasswordIncludeSpecialChar: valueOrNA(
        securityConfig?.passwordStrength?.includeSpecialChar
      ),
      NumDisallowedOldPasswords: valueOrNA(
        securityConfig?.passwordReuse?.numDisallowedOldPasswords
      ),
      NumDifferentChars: valueOrNA(
        securityConfig?.passwordReuse?.numDifferentChars
      ),
      PasswordMinLifetimeDays: valueOrNA(
        securityConfig?.passwordLifetime?.minLifetimeDays
      ),
      PasswordMaxLifetimeDays: valueOrNA(
        securityConfig?.passwordLifetime?.maxLifetimeDays
      ),
      MaxFailedLoginAttempts: valueOrNA(
        securityConfig?.accountLockout?.maxFailedLoginAttempts
      ),
      FailedLoginLockTimeDurationMins: valueOrNA(
        securityConfig?.accountLockout?.failedLoginLockTimeDurationMins
      ),
      AccountInactivityTimeDays: valueOrNA(
        securityConfig?.accountLockout?.inactivityTimeDays
      ),
      AuthTokenTimeoutMinutes: valueOrNA(
        securityConfig?.authTokenTimeoutMinutes
      ),
      UIInactivityTimeoutMSecs: valueOrNA(
        securityConfig?.inactivityTimeoutMSecs
      ),
      SessionManagementEnabled: valueOrNA(
        securityConfig?.sessionManagementEnabled
      ),
      SessionAbsoluteTimeoutSeconds: valueOrNA(
        securityConfig?.sessionConfiguration?.absoluteTimeout
      ),
      SessionInactivityTimeoutSeconds: valueOrNA(
        securityConfig?.sessionConfiguration?.inactivityTimeout
      ),
      LimitSessions: valueOrNA(
        securityConfig?.sessionConfiguration?.limitSessions
      ),
      SessionLimitPerUser: valueOrNA(
        securityConfig?.sessionConfiguration?.sessionLimitPerUser
      ),
      SessionLimitSystemWide: valueOrNA(
        securityConfig?.sessionConfiguration?.sessionLimitSystemWide
      ),
      CertificateMappingAuthenticationEnabled: valueOrNA(
        securityConfig?.certificateBasedAuth?.enableMappingBasedAuthentication
      ),
      CertificateMapping: valueOrNA(
        securityConfig?.certificateBasedAuth?.certificateMapping
      ),
      CertificateADMapping: valueOrNA(
        securityConfig?.certificateBasedAuth?.adMapping
      ),
      IsDataClassified: valueOrNA(
        securityConfig?.dataClassification?.isDataClassified
      ),
      ClassifiedDataMessage: valueOrNA(
        securityConfig?.dataClassification?.classifiedDataMessage
      ),
      UnclassifiedDataMessage: valueOrNA(
        securityConfig?.dataClassification?.unclassifiedDataMessage
      ),
      SSHTimeoutInMins: valueOrNA(
        securityConfig?.sshConfiguration?.sshTimeoutInMins
      )
    };

    return {
      ...row,
      ...evaluatePasswordPolicy(row)
    };
  }

  async function getApiKey() {
    try {
      const credentialList = await credentialVaultClient.getCredentials();
      const credential = toArray(credentialList?.credentials).find(
        (item) => item?.name === vaultName
      );

      if (credential?.id) {
        const detail = await credentialVaultClient.getCredentialsDetails({
          id: credential.id
        });

        const apiKey = detail?.token || detail?.password || null;
        if (apiKey) return apiKey;
      }
    } catch {
      // Continue to configured credential ID fallback.
    }

    try {
      const detail = await credentialVaultClient.getCredentialsDetails({
        id: vaultId
      });

      return detail?.token || detail?.password || null;
    } catch {
      return null;
    }
  }

  async function getJson(url, headers) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers
      });

      if (!response.ok) {
        let responseText = "";

        try {
          responseText = await response.text();
        } catch {
          responseText = "";
        }

        return {
          ok: false,
          status: response.status,
          error:
            responseText ||
            `HTTP ${response.status} ${response.statusText}`,
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
        error: error instanceof Error ? error.message : String(error),
        data: null
      };
    }
  }

  const apiKey = await getApiKey();

  if (!apiKey) {
    return {
      error: "No Cohesity API key available in Credential Vault",
      standard,
      rows: [],
      markdownEmail: "",
      errors: []
    };
  }

  const commonHeaders = {
    accept: "application/json",
    apiKey
  };

  const clusterResponse = await getJson(
    `${baseUrl}/v2/mcm/cluster-mgmt/info`,
    commonHeaders
  );

  if (!clusterResponse.ok) {
    return {
      error: "Failed to query Cohesity Helios clusters",
      status: clusterResponse.status,
      details: clusterResponse.error,
      standard,
      rows: [],
      markdownEmail: "",
      errors: []
    };
  }

  const clusterData = clusterResponse.data || {};
  const clusters = toArray(
    clusterData.cohesityClusters ||
      clusterData.clusters ||
      clusterData.clusterInfos ||
      clusterData?.mcmInfo?.clusterInfos
  ).sort((left, right) => {
    const leftName = firstValue(left, [
      "name",
      "clusterName",
      "displayName",
      "ClusterName",
      "Name"
    ]);

    const rightName = firstValue(right, [
      "name",
      "clusterName",
      "displayName",
      "ClusterName",
      "Name"
    ]);

    return leftName.localeCompare(rightName, undefined, {
      sensitivity: "base"
    });
  });

  if (!clusters.length) {
    return {
      clusterCount: 0,
      successfulClusterCount: 0,
      failedClusterCount: 0,
      compliantClusterCount: 0,
      nonCompliantClusterCount: 0,
      notAssessedClusterCount: 0,
      pci90ValueMetClusterCount: 0,
      rowCount: 0,
      standard,
      columns,
      rows: [],
      markdownEmail:
        "### Cohesity Cluster Security Configuration\n\n_No clusters returned from Cohesity Helios._",
      errors: []
    };
  }

  const rows = [];
  const errors = [];
  let successfulClusterCount = 0;

  for (const cluster of clusters) {
    const clusterNameValue = firstValue(cluster, [
      "name",
      "clusterName",
      "displayName",
      "ClusterName",
      "Name"
    ]);

    const clusterIdValue = firstValue(cluster, [
      "clusterId",
      "id",
      "ClusterId",
      "Id"
    ]);

    const clusterName =
      clusterNameValue === "N/A" ? "Unknown" : clusterNameValue;

    if (clusterIdValue === "N/A") {
      rows.push(emptyRow(clusterName));
      errors.push({
        cluster: clusterName,
        status: 0,
        error: "Cluster ID missing"
      });
      continue;
    }

    const clusterHeaders = {
      ...commonHeaders,
      accessClusterId: String(clusterIdValue)
    };

    const securityResponse = await getJson(
      `${baseUrl}/v2/security-config`,
      clusterHeaders
    );

    if (!securityResponse.ok) {
      rows.push(emptyRow(clusterName));
      errors.push({
        cluster: clusterName,
        status: securityResponse.status,
        error: securityResponse.error
      });
      continue;
    }

    rows.push(securityRow(clusterName, securityResponse.data || {}));
    successfulClusterCount++;
  }

  rows.sort((left, right) =>
    left.Cluster.localeCompare(right.Cluster, undefined, {
      sensitivity: "base"
    })
  );

  const compliantClusterCount = rows.filter(
    (row) => row.OverallPasswordPolicyStatus === "Compliant"
  ).length;
  const nonCompliantClusterCount = rows.filter(
    (row) => row.OverallPasswordPolicyStatus === "Non-Compliant"
  ).length;
  const notAssessedClusterCount = rows.filter(
    (row) => row.OverallPasswordPolicyStatus === "Not Assessed"
  ).length;
  const pci90ValueMetClusterCount = rows.filter(
    (row) => row.MeetsPCI90DayValue === "Yes"
  ).length;

  const reportDate = new Date().toLocaleDateString("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "short",
    day: "2-digit"
  });

  const markdownParts = [
    `### Cohesity Cluster Security Configuration — ${reportDate}`,
    "",
    `Password standard: minimum length ${standard.passwordMinLength}; complexity ${standard.complexityRequired} of 4; history ${standard.disallowedOldPasswords}; minimum age ${standard.passwordMinLifetimeDays} days; maximum age ${standard.passwordMaxLifetimeDays} days.`,
    "",
    "_PCI note: MeetsPCI90DayValue checks the configured numeric value only. PCI applicability and MFA usage are not exposed by /v2/security-config._"
  ];

  for (const section of markdownSections) {
    markdownParts.push(
      "",
      `#### ${section.title}`,
      markdownTable(section.columns, rows)
    );
  }

  if (errors.length) {
    markdownParts.push(
      "",
      `_Warning: ${errors.length} cluster query error(s). See the separate errors output._`
    );
  }

  const markdownEmail = markdownParts.join("\n");

  return {
    clusterCount: clusters.length,
    successfulClusterCount,
    failedClusterCount: errors.length,
    compliantClusterCount,
    nonCompliantClusterCount,
    notAssessedClusterCount,
    pci90ValueMetClusterCount,
    rowCount: rows.length,
    standard,
    columns,
    rows,
    markdownEmail,
    errors
  };
}
