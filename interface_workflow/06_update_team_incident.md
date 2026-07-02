# 06 - update_team_incident

## Task type

ServiceNow Update Incident task.

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
update_team_incident
```

## Purpose

Update existing active Team incidents when `normalize_team_search` puts items into `updateTeamIncidents[]`.

This task can update multiple incidents because it is looped.

Example:

```text
updateTeamIncidents[] has 2 items
update_team_incident loop runs 2 times
```

## Loop configuration

Loop must be enabled.

Loop input / item list:

```text
result("normalize_team_search").updateTeamIncidents
```

Condition:

```text
blank
```

Do not use `| length > 0` in the loop input. The loop input must be the array itself.

## ServiceNow field mapping

Your ServiceNow Update Incident action has only these fields:

```text
Incident number
Comment
```

Use Dynatrace loop variable `_.item`.

| ServiceNow field | Dynatrace mapping |
|---|---|
| Incident number | `{{ _.item.number }}` |
| Comment | `{{ _.item.comment }}` |

## Expected loop item shape

`normalize_team_search.updateTeamIncidents[]` returns flat objects only:

```json
[
  {
    "number": "INCxxxxxxx",
    "comment": "<details>",
    "correlation_id": "cohesity_ifdown_team_<cluster_id>"
  }
]
```

## Important rule: multiple updates vs duplicate incidents

These are different cases.

### Valid case: two different incidents need update

```text
Ashburn correlation_id returns 1 active incident
San Antonio correlation_id returns 1 active incident
```

Expected normalize output:

```text
updateCount = 2
updateTeamIncidents[] = [Ashburn item, San Antonio item]
```

This is valid. The update task loops twice.

### Unsafe case: one correlation_id returns two active incidents

```text
Ashburn correlation_id returns 2 active incidents
```

Expected normalize output:

```text
noWriteTeamIncidents[]
```

The workflow must not update either incident because duplicate active incidents exist for the same correlation ID.

## Do not map from snow_search_team directly

Do not use:

```text
result("snow_search_team")[0].number
```

The update task must use the normalized loop item:

```text
{{ _.item.number }}
```
