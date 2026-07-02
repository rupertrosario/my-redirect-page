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

`create_team_incident` creates a new Team incident only when `normalize_team_search` confirms that no active Team incident exists for the cluster correlation ID.

Normal case:

```text
SNOW search found 0 active incidents
  → normalize_team_search.createTeamIncidents[]
  → create_team_incident
```

## Loop configuration

Loop must be enabled on this task.

Loop input:

```text
result("normalize_team_search").createTeamIncidents
```

Condition:

```text
result("normalize_team_search").createTeamIncidents | length > 0
```

Each loop item is one Team incident to create.

## ServiceNow field mapping

### short_description

```text
{{ _.loopItemValue.short_description }}
```

### description

```text
{{ _.loopItemValue.description }}
```

### correlation_id

```text
{{ _.loopItemValue.correlation_id }}
```

### work_notes

```text
{{ _.loopItemValue.work_notes }}
```

If `work_notes` is not available in the Dynatrace UI mapping, use:

```text
{{ _.loopItemValue.description }}
```

## Optional useful fields

Use these only if they are required in your ServiceNow environment:

```text
category
subcategory
assignment_group
impact
urgency
caller_id
contact_type
```

## Important rules

Do not create an incident directly from `validate_interfaces`.

Create only from:

```text
result("normalize_team_search").createTeamIncidents
```

This prevents duplicate incidents.

## Expected result for current test scenario

If Ashburn already has a Team incident and San Antonio does not:

```text
createTeamIncidents[] contains San Antonio only
updateTeamIncidents[] contains Ashburn only
```

So this task should create only the San Antonio Team incident.
