# 00 - Source Contract and Workflow Plan

## Purpose

This document defines the new baseline for the Cohesity Interface DOWN Dynatrace workflow.

This is a planning/configuration document only. No JavaScript logic is implemented here.

## Design principle

```text
01_get_alerts + 02_validate_interfaces = source of truth
lookup tables = memory, routing, and enrichment
ServiceNow = incident system of record
```

Most incident facts must come from Cohesity alert collection and interface validation. Lookup tables must not invent incident details.

## Target workflow shape

```text
COMMON WORKFLOW
--------------
01_get_alerts
  -> 02_validate_interfaces
  -> 03_read_lookup_tables
  -> 04_build_common_context

SPLIT
-----
Team path:
  -> 05_decide_team_action
  -> 06_snow_search_team_fallback
  -> 07_create_or_update_team_incident

DC path:
  -> 05_decide_dc_action
  -> 06_snow_search_dc_fallback
  -> 07_create_or_update_dc_incident

COMMON CLOSEOUT
---------------
08_build_next_lookup_table
  -> 09_upload_lookup_table
  -> 10_report_summary
```

## Common source fields

The following fields should come from `01_get_alerts` and `02_validate_interfaces`:

```text
cluster
location
node_id
node_name
interface_name
bond_name
bond_slave_name
alert_code
alert_type
alert_time
status
fingerprint
correlation_id
short_description
description
```

## Team incident contract

Source array:

```text
teamIncidents[]
```

Identity:

```text
one incident per cluster
```

Correlation ID pattern:

```text
DT_cohesity_ifdown_<cluster_slug>
```

Team path uses:

```text
cluster
fingerprint
correlation_id
short_description
description
cmdb_ci
existing incident memory
fallback ServiceNow search result
```

## DC incident contract

Source array:

```text
dcIncidents[]
```

Identity:

```text
one incident per location + cluster
```

Correlation ID pattern:

```text
DT_cohesity_ifdown_dc_<location_slug>_<cluster_slug>
```

DC path uses:

```text
location
cluster
fingerprint
correlation_id
short_description
description
assignment_group
cmdb_ci
existing incident memory
fallback ServiceNow search result
```

## Lookup tables

### Idempotency lookup table

Temporary memory only. Do not use as history.

Suggested fields:

```text
incident_type
correlation_id
cluster_name
location
fingerprint
incident_number
incident_sys_id
cmdb_ci_sys_id
last_seen
last_action
status
```

Rules:

```text
lookup row found + same fingerprint = no-op
lookup row found + changed fingerprint = update
lookup row missing = ServiceNow fallback search
lookup row stale/suspicious = ServiceNow fallback search
multiple active ServiceNow matches = no write, manual review
```

### DC routing lookup table

Replaces hardcoded DC location allowlist and assignment group mapping.

Suggested fields:

```text
location_key
location_name
enabled
assignment_group
assignment_group_sys_id
```

Rules:

```text
validate_interfaces detects the location
DC routing lookup decides whether the location is enabled
DC routing lookup supplies the assignment group
```

### CI lookup

CI lookup only enriches the incident candidate.

Rules:

```text
cluster CI found = use cluster CI sys_id
cluster CI missing = use approved fallback CI
```

Fallback CI:

```text
Cohesity (PRODUCTION)
```

## Implementation order

```text
1. Rebuild 01_get_alerts from archived baseline.
2. Rebuild 02_validate_interfaces with clean Team/DC output contract.
3. Add lookup table read design.
4. Add common context builder.
5. Build Team path.
6. Build DC path.
7. Add lookup-table closeout.
8. Add report summary.
```

## Guardrails

```text
No JavaScript logic changes without review.
No ServiceNow write without explicit create/update decision output.
No duplicate active incidents for the same correlation_id.
No work_notes comparison for idempotency.
No sys_journal_field dependency.
No lowercase interface_alerts folder.
Do not modify backup_failures from this workflow.
```
