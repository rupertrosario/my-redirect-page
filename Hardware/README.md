# Cohesity Hardware / Rack Resiliency Automation

Branch: `Cohesity_Automations`

## Rack Resiliency Assessment

Script: `Cohesity_Rack_Resiliency_Assessment.ps1`

Purpose: collect READ-ONLY Cohesity data required to assess Rack-level Failure Domain / Rack Resiliency readiness. The script does not change Cohesity configuration and is intentionally blocked to HTTP GET only.

### Safety

The request wrapper verifies the HTTP method is `GET` before every API request. The script contains no POST, PUT, PATCH, DELETE, rack assignment, chassis reassignment, Storage Domain modification, Fault Tolerance modification, node modification, or cluster modification logic.

Missing information is reported as:

`NOT AVAILABLE THROUGH APPROVED READ-ONLY COLLECTION`

### GET APIs

- `GET /v2/clusters`
- `GET /v2/clusters/nodes`
- `GET /v2/node/hardware-info`
- `GET /v2/chassis`
- `GET /v2/racks`
- `GET /v2/storage-domains`
- `GET /v2/storage-domains/fault-tolerance-options?storageDomainId=<ID>` for every returned Storage Domain

All cluster-scoped calls are routed through Helios with the existing `accessClusterId` header.

### Collection

The collector gathers node/chassis/rack topology, selected hardware models, node reachability/status, Storage Domain EC/RF/capacity data, current failure-domain state, and every returned Fault Tolerance option including disabled/warning/suboptimal status and minimum failure-domain requirements when the GET response exposes them.

No hostnames, DNS names, IP addresses, IPMI addresses, interface addresses, node serials, or chassis serials are written to the generated reports. Node/chassis/Storage Domain IDs are retained only in internal outputs where required for local correlation.

### Hardware model buckets

- CX8405
- C6025
- C5066
- C5026
- C5016
- Other

### Outputs

The script generates two primary Markdown reports:

1. `Cohesity_Rack_Resiliency_Internal_<timestamp>.md`
   - internal cluster identifiers and detailed local correlation data
   - no credentials, tokens, passwords, hostnames, IPs, or serial numbers

2. `Cohesity_Rack_Resiliency_Sanitized_<timestamp>.md`
   - clusters anonymized as `Cluster-01`, `Cluster-02`, etc.
   - racks anonymized as `Rack-1`, `Rack-2`, etc.
   - Storage Domains anonymized as `SD-1`, `SD-2`, etc.
   - structured Estate Summary, Hardware Distribution, Cluster Resiliency Summary, Storage Domain Resiliency, Rack Distribution, Findings, Most Important Exceptions, and Data Quality sections

Supporting internal CSV files are also produced for node, chassis, rack, Storage Domain, and Fault Tolerance option correlation. They exclude names/IPs/serials.

### Current estate validation baseline

The script currently checks the known baseline:

- Expected clusters: 22
- Expected nodes: 169

A mismatch is reported as `CHECK REQUIRED`; it never triggers a configuration change.

### Assessment behavior

The script flags only conditions proven or calculated from GET results, including no racks configured, unassigned chassis, uneven rack/node distribution, mixed 1-node and 4-or-more-node chassis architecture, warning/suboptimal/disabled FT options, and rack count below a returned `minFailureDomainsRequired` value.

It does **not** state that Rack FT is safe merely because an option is available. Incomplete or ambiguous GET data is marked `UNKNOWN` and requires Cohesity confirmation.

### Final safety line

Every sanitized report ends with:

`READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment.`

## Existing Node + Chassis Inventory

`Cohesity_Helios_Hardware_Inventory.ps1` remains available as the earlier GET-only node/chassis CSV inventory script.
