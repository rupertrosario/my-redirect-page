# Interface Alerts Workflow Mapping

## High-level workflow

```text
01_get_alerts
  -> validate_interfaces
  -> 03_team_iterations
  -> snow_search_team
  -> 03_compare_team_incident_state
  -> create_team_incident OR update_team_incident
```

DC flow is separate:

```text
validate_interfaces
  -> dc_iterations
  -> snow_search_dc
  -> 03_compare_dc_incident_state
  -> create_dc_incident OR update_dc_incident
```

## Team search

ServiceNow search should loop on Team iteration items.

Search query:

```text
{{ _.item.query }}
```

The generated query should resolve to:

```text
correlation_id=cohesity_ifdown_team_<cluster_id>^stateNOT IN6,7^ORDERBYDESCsys_updated_on
```

Required search return fields:

```text
sys_id,number,state,short_description,correlation_id,description,sys_updated_on
```

## Team create

Create only when no active incident exists for the cluster-level correlation ID.

Create description should include:

```text
Fingerprint: <current_fingerprint>
```

The fingerprint must represent the current DOWN interface state.

## Team update

Update only when `03_compare_team_incident_state` returns:

```json
[
  {
    "shouldUpdate": true
  }
]
```

Update mappings in Dynatrace ServiceNow action:

```text
Incident number = {{ _.item.number }}
Comment         = {{ _.item.comment }}
```

If using custom API payload helper, use `team/04_update_team_incident_payload.js`.

## No-update rule

If compare returns:

```json
[
  {
    "shouldUpdate": false
  }
]
```

then the workflow should not run update.

## Important

Do not update just because an incident exists.

Update only when current state differs from already captured description/fingerprint.
