# Cohesity Active Directory Configuration CSV

## Purpose

`Get-CohesityADConfiguration.ps1` retrieves Active Directory integration details from every Cohesity cluster visible through Cohesity Helios and exports a timestamped CSV.

The implementation follows the same operating pattern as the Cohesity policy inventory collector:

- fixed configuration paths at the top of the script
- one shared GET wrapper
- Helios cluster discovery
- per-cluster `accessClusterId` processing
- per-cluster error isolation
- timestamped CSV output
- final console summary

Password-policy or password-standard checks are not included.

## Files

| File | Purpose |
|---|---|
| `Get-CohesityADConfiguration.ps1` | PowerShell 5.1/7-compatible GET-only collector |
| `README.md` | Configuration, API flow, execution, fields, status logic, and sample output |

## Read-Only API Flow

1. `GET /v2/mcm/cluster-mgmt/info`
   - Retrieves all Cohesity clusters visible to the Helios API key.
2. `GET /v2/active-directories?includeTenants=true`
   - Executed once per cluster.
   - The target cluster is selected using the `accessClusterId` header.

The script contains no `POST`, `PUT`, `PATCH`, or `DELETE` request.

## Requirements

- Windows PowerShell 5.1 or PowerShell 7+
- Outbound HTTPS access to `https://helios.cohesity.com`
- Helios API key with:
  - visibility to the required clusters
  - read permission for Active Directory/LDAP configuration
- Read access to the API-key file
- Write access to the output directory

The API key is read from disk and is never written to the console or CSV.

## Configuration

Edit the configuration section at the top of the script:

```powershell
$baseUrl      = "https://helios.cohesity.com"
$apikeypath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$logDirectory = "X:\PowerShell\Data\Cohesity\ADInventory"
```

The output directory is created automatically when it does not exist.

The API-key file must contain only the Helios API key.

## Run

```powershell
Set-Location X:\PowerShell\Cohesity_API_Scripts\Cohesity_AD

.\Get-CohesityADConfiguration.ps1
```

## Output File

The script writes one timestamped CSV:

```text
X:\PowerShell\Data\Cohesity\ADInventory\Cohesity_AD_Configuration_YYYYMMDD_HHMM.csv
```

The CSV contains one row per Active Directory connection.

A cluster with no returned AD configuration receives one explicit `NOT_CONFIGURED` row. A failed cluster query receives one explicit `COLLECTION_ERROR` row, allowing the report to remain complete even when one cluster cannot be queried.

## CSV Columns

| Column | Description |
|---|---|
| `Cluster` | Cohesity cluster name |
| `ADConfigured` | `Yes`, `No`, or `Unknown` |
| `DomainName` | Joined Active Directory domain |
| `OrganizationalUnit` | Configured organizational unit |
| `WorkGroupName` | Configured workgroup, when returned |
| `MachineAccounts` | AD machine-account name and DNS hostname |
| `PreferredDomainControllers` | Preferred controllers and reported reachability |
| `DomainControllers` | All returned controllers grouped by domain |
| `DomainControllersDenyList` | Controllers explicitly denied |
| `TrustedDomains` | Returned trusted and allow-listed domains |
| `IdMappingType` | User ID mapping type, such as `Rfc2307` or `Rid` |
| `LdapProviderId` | LDAP provider identifier |
| `NisProviderDomainName` | NIS provider domain, when configured |
| `ConnectionId` | AD connection identifier |
| `ADConfigurationId` | Cohesity AD configuration identifier |
| `ErrorCode` | Cohesity-reported AD configuration error code |
| `ErrorMessage` | Cohesity-reported error or collection failure message |
| `CollectionStatus` | Normalized collection result |

## Collection Status

| Status | Meaning |
|---|---|
| `CONFIGURED` | AD configuration was returned with no reported error or unreachable controller |
| `REVIEW` | AD is configured, but one or more returned controllers are not `Reachable` |
| `ERROR` | Cohesity returned an AD configuration error code or error message |
| `NOT_CONFIGURED` | The cluster returned no Active Directory configuration |
| `COLLECTION_ERROR` | The cluster-specific GET failed or the cluster ID was unavailable |

## Sample CSV Output

| Cluster | ADConfigured | DomainName | OrganizationalUnit | MachineAccounts | PreferredDomainControllers | TrustedDomains | IdMappingType | CollectionStatus |
|---|---|---|---|---|---|---|---|---|
| CHS-PROD-01 | Yes | `corp.example.com` | `OU=Cohesity,OU=Servers,DC=corp,DC=example,DC=com` | `CHS-PROD-01$; DNS=chs-prod-01.corp.example.com` | `dc01.corp.example.com [Reachable]; dc02.corp.example.com [Reachable]` | `emea.example.com` | `Rfc2307` | `CONFIGURED` |
| CHS-DR-01 | Yes | `corp.example.com` | `OU=DR,DC=corp,DC=example,DC=com` | `CHS-DR-01$` | `dc03.corp.example.com [Unreachable]` | `N/A` | `Rid` | `REVIEW` |
| CHS-TEST-01 | No | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `NOT_CONFIGURED` |
| CHS-LAB-01 | Unknown | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `N/A` | `COLLECTION_ERROR` |

## Console Summary

The script prints:

- clusters discovered
- clusters with AD
- clusters without AD
- rows requiring review
- error rows
- total CSV rows
- CSV output path
- per-cluster fetch issues

## Validation

Run the script and confirm:

```powershell
Import-Csv "X:\PowerShell\Data\Cohesity\ADInventory\Cohesity_AD_Configuration_*.csv" |
    Select-Object -First 10 |
    Format-Table Cluster, ADConfigured, DomainName, PreferredDomainControllers, CollectionStatus -AutoSize
```

Confirm that:

1. Every expected cluster appears.
2. `NOT_CONFIGURED` is present only for clusters without AD integration.
3. Any non-`Reachable` controller is marked `REVIEW`.
4. `COLLECTION_ERROR` rows are investigated rather than treated as not configured.
5. No API key or password information appears in the CSV.
