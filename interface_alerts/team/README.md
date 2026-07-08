# Team Incident Workflow

## Purpose

Team incidents track Cohesity Interface DOWN conditions at the cluster level.

The Team workflow should not create one ticket per IP or physical interface.

## Correlation

Team incident correlation ID:

```text
cohesity_ifdown_team_<cluster_id>
```

This keeps one active Team incident per affected Cohesity cluster.

## Update logic

The meaningful change is not the cluster itself. The meaningful change is the IP/interface state inside the cluster.

Expected decision logic:

| Existing Team incident | Current IP/interface state | Action |
|---|---|---|
| No | Any confirmed DOWN state | Create |
| Yes | Same state already captured | No update |
| Yes | Different IP/interface state | Update existing incident |
| Duplicate active incidents | Any | No write / manual review |

## Comparison source

Since ServiceNow work notes are not available, compare against the existing incident `description`.

The create description should include the current fingerprint:

```text
Fingerprint: <NodeIP|Interface|LinkState;...>
```

The compare task checks whether the current fingerprint differs from the fingerprint already stored in description.

## Dynatrace mapping reminder

For looped ServiceNow tasks in the current UI, use:

```text
{{ _.item.<field> }}
```

Example update mappings:

```text
Incident number = {{ _.item.number }}
Comment         = {{ _.item.comment }}
```
