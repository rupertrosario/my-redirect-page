# Cohesity Active Directory Configuration

## Purpose

Collect Active Directory integration details from every Cohesity cluster visible through Cohesity Helios.

Both collectors are strictly read-only and use only HTTP `GET` requests.

## Files

| File | Purpose |
|---|---|
| `Get-CohesityADConfiguration.ps1` | PowerShell 5.1/7 multi-cluster CSV collector |
| `Get-CohesityADConfiguration.js` | Dynatrace JavaScript action with Markdown output |
| `Sample_Cohesity_AD_Configuration.csv` | Illustrative output only |

## API Flow

1. `GET /v2/mcm/cluster-mgmt/info`
2. `GET /v2/active-directories?includeTenants=true`
   - Called once per cluster.
   - Uses the `accessClusterId` request header.

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

```powershell
$baseUrl      = "https://helios.cohesity.com"
$apikeypath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$logDirectory = "X:\PowerShell\Data\Cohesity\ADInventory"
```

Run:

```powershell
.\Get-CohesityADConfiguration.ps1
```

Output:

```text
X:\PowerShell\Data\Cohesity\ADInventory\Cohesity_AD_Configuration_YYYYMMDD_HHmm.csv
```

## Dynatrace Configuration

The JavaScript action uses the credential vault:

```javascript
const vaultName = "Cohesity_API_Key";
const vaultId = "credentials_vault-312312";
```

Update `vaultId` if the credential ID differs in the target Dynatrace environment.

The JavaScript action returns:

- `rows`
- `markdownEmail`
- cluster and row counts
- a separate `errors` array

## Sample Output

| Cluster | ADConfigured | DomainName | OrganizationalUnit | WorkGroupName | MachineAccounts | PreferredDomainControllers | DomainControllersDenyList | TrustedDomains | ADConfigurationId |
|---|---|---|---|---|---|---|---|---|---|
| CHS-PROD-01 | Yes | `corp.example.com` | `OU=Cohesity,OU=Servers` | `N/A` | `CHS-PROD-01$` | `dc01 [Reachable]; dc02 [Reachable]` | `N/A` | `emea.example.com` | `101` |
| CHS-TEST-01 | No | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` |

The values above are illustrative and are not values retrieved from the production environment.
