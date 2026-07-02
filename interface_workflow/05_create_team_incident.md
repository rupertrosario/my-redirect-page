# 05 - create_team_incident

## Task type

ServiceNow Create Incident task.

This is not a JavaScript task.

## Position in workflow

```text
get_alerts
  ↓
validate_interfaces
  ↓
snow_search_team
  ↓
normalize_team_search
  ↓
create_team_incident
```

## Purpose

Create a Team incident only when `normalize_team_search` puts an item into `createTeamIncidents[]`.

Do not create directly from `validate_interfaces`.

## Loop configuration

Loop must be enabled.

Loop input / item list:

```text
result("normalize_team_search").createTeamIncidents
```

Condition:

```text
blank
```

Do not use `| length > 0` in the loop input. The loop input must be the array itself.

## ServiceNow field mapping

Use Dynatrace loop variable `_.item`.

| ServiceNow field | Dynatrace mapping |
|---|---|
| Short description | `{{ _.item.short_description }}` |
| Description | `{{ _.item.description }}` |
| Correlation ID | `{{ _.item.correlation_id }}` |
| Comment / Work notes | `{{ _.item.comment }}` |

## Expected loop item shape

`normalize_team_search.createTeamIncidents[]` returns flat objects only:

```json
[
  {
    "short_description": "Cohesity Interface DOWN - Team - <cluster>",
    "description": "<details>",
    "correlation_id": "cohesity_ifdown_team_<cluster_id>",
    "comment": "<details>"
  }
]
```

## Current test scenario

If Ashburn already has a Team incident and San Antonio does not:

```text
createTeamIncidents[] contains San Antonio only
updateTeamIncidents[] contains Ashburn only
```

So this task creates only the San Antonio Team incident.
