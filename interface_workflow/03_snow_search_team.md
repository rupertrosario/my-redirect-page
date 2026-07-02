# 03 - snow_search_team

## Task type

ServiceNow Search / ServiceNow Table API task.

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
```

## Purpose

`snow_search_team` searches ServiceNow for an existing active Team incident for each affected Cohesity cluster.

Team incident rule:

```text
One active Team incident per ClusterId
```

The search is driven by `correlation_id`.

```text
correlation_id = cohesity_ifdown_team_<ClusterId>
```

## Loop configuration

Loop must be enabled on this task.

Loop input:

```text
result("validate_interfaces").teamIncidents
```

Condition:

```text
result("validate_interfaces").teamIncidents | length > 0
```

Each loop item is one Team incident candidate / one cluster.

Example:

```text
Loop item 0 → Cluster A
Loop item 1 → Cluster B
Loop item 2 → Cluster C
```

## ServiceNow sysparams

### sysparm_query

```text
correlation_id={{ _.loopItemValue.correlation_id }}^stateNOT IN6,7^ORDERBYDESCsys_updated_on
```

### sysparm_limit

```text
2
```

Reason: normal result should be 0 or 1. Limit 2 allows duplicate active incidents to be detected instead of hidden.

### sysparm_fields

```text
sys_id,number,state,short_description,correlation_id,sys_updated_on
```

## Expected result per loop item

```text
0 records → normalize_team_search sends item to createTeamIncidents[]
1 record  → normalize_team_search sends item to updateTeamIncidents[]
2 records → normalize_team_search sends item to noWriteTeamIncidents[]
```

## Notes

Do not search by alert created time.
Do not search by IP.
Do not search by interface name.

For Team incidents, use only the stable cluster-level `correlation_id`.

IP and interface are part of the incident details/fingerprint, not the Team incident identity.
