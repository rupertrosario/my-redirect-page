# Cohesity Active Directory Configuration

## Purpose

Collect Active Directory integration details from every Cohesity cluster visible through Cohesity Helios.

Both collectors are strictly read-only and use only HTTP `GET` requests.

## Files

| File | Purpose |
|---|---|
| `Get-CohesityADConfiguration.ps1` | PowerShell 5.1/7 multi-cluster CSV collector |
| `Get-CohesityADConfiguration.js` | Dynatrace JavaScript action with structured rows and one Markdown table |
| `Sample_Cohesity_AD_Configuration.csv` | Illustrative output only |

## API Flow

### Cluster discovery

```text
GET /v2/mcm/cluster-mgmt/info
```

This returns every Cohesity cluster visible to the Helios API key.

### Active Directory configuration

```text
GET /v2/active-directories?includeTenants=true
```

This request is executed once per cluster with the current cluster ID in the request header:

```text
accessClusterId: <clusterId>
```

No `POST`, `PUT`, `PATCH`, or `DELETE` request is used.

## Final Output Columns

| Column | Source |
|---|---|
| `Cluster` | Helios cluster inventory |
| `ADConfigured` | Derived by the script |
| `DomainName` | AD configuration API |
| `OrganizationalUnit` | `organizationalUnitName` |
| `WorkGroupName` | `workGroupName` |
| `MachineAccounts` | `machineAccounts` |
| `PreferredDomainControllers` | `preferredDomainControllers` |
| `DomainControllersDenyList` | `domainControllersDenyList` |
| `TrustedDomains` | trusted and whitelisted domains |
| `ADConfigurationId` | AD configuration `id` |

`N/A` means the API did not return a configured value for that optional field. It does not automatically mean that the cluster is misconfigured.

## PowerShell Configuration

The PowerShell collector uses the shared AES helper pattern.

```powershell
$baseUrl            = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\ADInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
```

The script dot-sources `ApiKeyAesHelper.ps1` and calls:

```powershell
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
```

Run:

```powershell
.\Get-CohesityADConfiguration.ps1
```

Output:

```text
X:\PowerShell\Data\Cohesity\ADInventory\Cohesity_AD_Configuration_YYYYMMDD_HHmm.csv
```

## Dynatrace JavaScript Setup

### 1. Create the Credential Vault entry

Create a Dynatrace Credential Vault token/password entry containing the Cohesity Helios API key.

Use this credential name:

```text
Cohesity_API_Key
```

The script first searches the vault by name. If that lookup is unavailable, it uses the configured credential ID:

```javascript
const vaultName = "Cohesity_API_Key";
const vaultId = "credentials_vault-312312";
```

Replace `vaultId` when the credential ID differs in the target Dynatrace environment.

Do not paste the API key directly into the JavaScript.

### 2. Add the JavaScript workflow action

1. Add a **Run JavaScript** action to the Dynatrace workflow.
2. Use a task ID such as `cohesity_ad_inventory`.
3. Paste the complete contents of `Get-CohesityADConfiguration.js` into the action.
4. Allow the action to read the required Credential Vault entry.
5. Allow outbound HTTPS access to `https://helios.cohesity.com`.
6. No workflow input parameters are required.

### 3. JavaScript execution flow

The action performs the following operations:

1. Reads the Helios API key from Credential Vault.
2. Runs `GET /v2/mcm/cluster-mgmt/info` to discover clusters.
3. Reads the cluster name and cluster ID for each cluster.
4. Adds `accessClusterId` to the request headers.
5. Runs `GET /v2/active-directories?includeTenants=true` for each cluster.
6. Normalizes all results to the same ten columns used by PowerShell.
7. Creates one Markdown table.
8. Returns per-cluster failures separately in `errors`.

### 4. JavaScript return object

The action returns:

| Property | Purpose |
|---|---|
| `clusterCount` | Total clusters discovered through Helios |
| `configuredClusterCount` | Clusters returning one or more AD configurations |
| `notConfiguredClusterCount` | Clusters returning an empty successful AD response |
| `failedClusterCount` | Clusters whose ID was missing or whose AD GET failed |
| `rowCount` | Number of inventory rows |
| `columns` | Ordered list of the ten output columns |
| `rows` | Structured AD inventory rows |
| `markdownEmail` | One Markdown table for the email body |
| `errors` | Separate per-cluster query failures |

A failed cluster query creates an inventory row with:

```text
ADConfigured = Unknown
```

The failure details remain in the separate `errors` array. A failed GET is therefore not reported as AD not configured.

### 5. Configure the email action

Use the JavaScript action result field below as the email body:

```text
{{ result("cohesity_ad_inventory").markdownEmail }}
```

Replace `cohesity_ad_inventory` when a different JavaScript task ID is used.

The email body contains one Markdown table with all ten columns. It does not create a second error table.

### 6. Validation

Run the JavaScript action once and confirm:

1. `clusterCount` matches the expected number of Helios clusters.
2. `rowCount` is at least the number of discovered clusters.
3. `rows[0]` contains exactly the ten documented fields.
4. `markdownEmail` starts with the report heading and one Markdown table header.
5. `errors` is empty, or contains only known cluster-specific access/query failures.
6. No API key appears in logs, rows, Markdown output, or errors.
7. Network activity contains only the two documented Cohesity `GET` endpoints.

## Sample Output

| Cluster | ADConfigured | DomainName | OrganizationalUnit | WorkGroupName | MachineAccounts | PreferredDomainControllers | DomainControllersDenyList | TrustedDomains | ADConfigurationId |
|---|---|---|---|---|---|---|---|---|---|
| CHS-PROD-01 | Yes | `corp.example.com` | `OU=Cohesity,OU=Servers` | `N/A` | `CHS-PROD-01$` | `dc01 [Reachable]; dc02 [Reachable]` | `N/A` | `emea.example.com` | `101` |
| CHS-TEST-01 | No | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` |

The values above are illustrative and are not values retrieved from the production environment.
