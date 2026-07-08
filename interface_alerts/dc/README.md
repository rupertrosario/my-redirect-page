# DC Incident Workflow

## Purpose

DC incidents are separate from Team incidents.

Team incidents are cluster-level. DC incidents are location + cluster level.

Recommended DC correlation ID:

```text
cohesity_ifdown_dc_<location>_<cluster_id>
```

## Current comparison approach

Since ServiceNow work notes cannot be fetched, DC update suppression compares:

```text
current generated description
vs
existing ServiceNow description
```

Decision logic:

| Existing DC incident | Description comparison | Action |
|---|---|---|
| No | N/A | Create |
| Yes | Same | No update |
| Yes | Different | Update |

## Important validation item

The current simple JS comparison uses index `[0]` style matching in some test versions.

That is acceptable only when the workflow branch is already isolated to a single DC item.

If more than one DC item is processed together, the comparison must pair current and existing records by `correlation_id` instead of blindly using `[0]`.
