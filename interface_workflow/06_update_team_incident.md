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

`update_team_incident` updates existing active Team incidents when `normalize_team_search` confirms that exactly one active incident exists for each Team correlation ID.

This task can update more than one incident when it is looped.

Example:

```text
Ashburn has one existing Team incident
San Antonio has one existing Team incident

normalize_team_search.updateTeamIncidents[] has 2 items
update_team_incident loop runs 2 times
```

Result:

```text
Loop 1 updates Ashburn incident
Loop 2 updates San Antonio incident
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

## ServiceNow field mapping

Your ServiceNow Update Incident action has only these fields:

```text
Incident number
Comment
```

Use this mapping:

| ServiceNow field | Dynatrace mapping |
|---|---|
| Incident number | `{{ _.loopItemValue.number }}` |
| Comment | `{{ _.loopItemValue.comment }}` |

## Where `comment` comes from

`normalize_team_search` now adds this field to every update item:

```js
comment: candidate.comment || candidate.work_notes || candidate.description || fallback text
```

So the update task should use:

```text
{{ _.loopItemValue.comment }}
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

## Do not use sys_id for this action

Your ServiceNow Update Incident action expects incident number, so use:

```text
{{ _.loopItemValue.number }}
```

Do not use:

```text
{{ _.loopItemValue.sys_id }}
```

unless you switch to a ServiceNow update action that specifically expects `sys_id`.
