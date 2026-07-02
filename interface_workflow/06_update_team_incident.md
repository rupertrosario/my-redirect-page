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

`update_team_incident` updates an existing active Team incident when `normalize_team_search` confirms that exactly one active incident exists for the cluster correlation ID.

Normal case:

```text
SNOW search found 1 active incident
  → normalize_team_search.updateTeamIncidents[]
  → update_team_incident
```

## Loop configuration

Loop must be enabled on this task.

Loop input:

```text
result("normalize_team_search").updateTeamIncidents
```

Condition:

```text
result("normalize_team_search").updateTeamIncidents | length > 0
```

Each loop item is one existing Team incident to update.

## ServiceNow record identifier

Use the `sys_id` from normalize output.

```text
{{ _.loopItemValue.sys_id }}
```

This `sys_id` comes from the matching ServiceNow search result.

## ServiceNow field mapping

### sys_id / Record ID

```text
{{ _.loopItemValue.sys_id }}
```

### work_notes

```text
{{ _.loopItemValue.work_notes }}
```

If `work_notes` is not available in the Dynatrace UI mapping, use:

```text
{{ _.loopItemValue.description }}
```

## Optional fields

Usually do not update `short_description` unless you want the latest cluster details reflected in the title.

If needed:

```text
short_description = {{ _.loopItemValue.short_description }}
```

## Important rules

Do not update directly from `snow_search_team`.

Update only from:

```text
result("normalize_team_search").updateTeamIncidents
```

This ensures the workflow updates only when exactly one active incident exists.

Do not update if normalize sends the item to:

```text
noWriteTeamIncidents[]
```

`noWriteTeamIncidents[]` means the search result was unsafe, usually because of duplicate active incidents or missing correlation ID.

## Expected result for current test scenario

If Ashburn already has a Team incident and San Antonio does not:

```text
createTeamIncidents[] contains San Antonio only
updateTeamIncidents[] contains Ashburn only
```

So this task should update only the Ashburn Team incident.
