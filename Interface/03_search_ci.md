# 03 - search_ci

## Task type

ServiceNow Search / ServiceNow Table API task.

This is not a JavaScript task.

## Position in workflow

This task runs after `02_validate_interfaces.js` and before the Team incident ServiceNow search.

```text
01_get_alerts
  ↓
02_validate_interfaces
  ↓
03_search_ci
  ↓
04_snow_search_team
  ↓
05_normalize_team_search
  ↓
create_team_incident / update_team_incident
```

## Purpose

Find the ServiceNow Configuration Item for each Cohesity cluster before incident create/update normalization.

The important design point is that both searches must stay attached to the same source list from `validate_interfaces`.

Do not loop CI search from the incident-search output.
Do not loop incident search from the CI search output.

Both searches should loop over:

```text
result("validate_interfaces").teamIncidents
```

This keeps the arrays aligned by loop index:

```text
teamIncidents[0]        -> CI search result[0]        -> incident search result[0]
teamIncidents[1]        -> CI search result[1]        -> incident search result[1]
teamIncidents[n]        -> CI search result[n]        -> incident search result[n]
```

## Loop setup

Loop must be enabled.

Loop input / item list:

```text
result("validate_interfaces").teamIncidents
```

Condition:

```text
blank
```

Do not put `| length > 0` in the loop input. The loop input must be the array itself.

## ServiceNow search parameters

### Table

```text
cmdb_ci_cohesity_cluster
```

### sysparm_query

Use exact cluster-name match first:

```text
name={{ _.item.cluster }}^ORDERBYname
```

### sysparm_limit

```text
1
```

### sysparm_fields

```text
sys_id,name,sys_class_name
```

## Expected behavior

For each Team candidate:

```text
1 matching CI -> use that CI for Configuration item / cmdb_ci
0 matching CI -> use fallback CI: Cohesity (PRODUCTION)
```

## Fallback CI

If `cmdb_ci_cohesity_cluster` does not return a matching cluster CI, use the generic production CI:

```text
Cohesity (PRODUCTION)
```

If the ServiceNow create task accepts a sys_id for Configuration item, use the sys_id for `Cohesity (PRODUCTION)` instead of the display name.

## Notes

- The cluster name comes from `validate_interfaces.teamIncidents[].cluster`.
- Incident identity remains `correlation_id`; CI is not used for incident identity.
- `04_snow_search_team` should continue to search incidents by `correlation_id`.
- `05_normalize_team_search` is where candidate + CI search + incident search should be joined.
- No JavaScript change is included in this document.
