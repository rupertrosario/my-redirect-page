# JIRA Update - Dynatrace Cohesity Interface DOWN Automation

## Summary

The Dynatrace workflow for Cohesity Interface DOWN alerts was reworked to reduce duplicate ServiceNow activity and align incident handling with the intended operational model.

The automation is being adjusted so that Team incidents are created at the cluster level, while update decisions are based on the actual interface-down state for that cluster. This avoids creating separate Team incidents for every node IP or physical interface, but still allows the existing cluster-level incident to be updated when a different IP/interface is detected as down.

## Why this was reworked

The earlier workflow design was creating too much risk of repeated or incorrect ServiceNow updates because the create/update logic was based mainly on whether an active incident existed.

That approach was too broad:

```text
active incident exists -> update
```

This would cause the same incident to be updated repeatedly even when the current DOWN interface state had not changed.

The rework changes the decision model to:

```text
same cluster + same IP/interface state -> no update
same cluster + different IP/interface state -> update existing Team incident
no active cluster incident -> create Team incident
```

This is required because the Team incident is intentionally cluster-level, but the meaningful change is IP/interface-level.

## Current design

### Team incident identity

Team incidents are correlated at cluster level only.

```text
cohesity_ifdown_team_<cluster_id>
```

IP address, node, and interface name must not be part of the Team correlation ID.

### Team update decision

The workflow uses a fingerprint representing the current confirmed DOWN interface state for the cluster.

The fingerprint is based on the current interface details, such as:

```text
NodeIP | Interface | LinkState
```

Decision logic:

| Scenario | Action |
|---|---|
| No active Team incident for the cluster | Create Team incident |
| Active Team incident exists and fingerprint is unchanged | No update |
| Active Team incident exists and fingerprint changed | Update existing Team incident |
| More than one active incident exists for the same correlation ID | No write / manual review |

This keeps one Team incident per affected Cohesity cluster while still updating the ticket when a new/different IP or interface becomes impacted.

### DC incident logic

DC incidents are handled separately from Team incidents.

For DC incidents, the current approach compares the current incident description with the existing ServiceNow incident description. If the description has not changed, the workflow should not update the incident. If the description changes, the existing DC incident can be updated.

This avoids dependency on ServiceNow work notes, which are not available to the workflow.

## Important constraint

ServiceNow work notes cannot be fetched by the current workflow/action.

Because of that, repeat-update suppression cannot rely on work_notes. The workflow must use data that is available from the ServiceNow search response, primarily:

```text
correlation_id
number
sys_id
description
```

The description/fingerprint approach is being used for this reason.

## Current state

Completed / working direction:

- Cohesity 1105 networking alerts are pulled by Dynatrace.
- Alert node IPs are extracted and deduplicated.
- Cohesity `/public/interface` is called to confirm the interface is currently DOWN.
- Confirmed DOWN rows are used to build incident content.
- Team incident model is cluster-level.
- Change detection is intended to be IP/interface-level using fingerprint/description comparison.
- Repeated updates should be avoided when the same cluster has the same DOWN interface state.

Still being validated:

- Final Team create/update branching using the actual `dyna_int_js*` workflow chain.
- Confirming that ServiceNow search returns `description` consistently for comparison.
- Confirming the exact loop item variable and mapping used by the Dynatrace ServiceNow actions.
- DC incident comparison logic for multiple DC/location items, especially avoiding blind `[0]` matching when more than one item exists.

## Implementation note

The intended logic is not to create a new Team incident when another IP/interface goes down on the same cluster. The existing cluster-level Team incident should be updated only when the current IP/interface fingerprint differs from what was already captured in the incident description.

This preserves the operational requirement:

```text
One active Team incident per affected cluster.
Update only when the impacted IP/interface state changes.
```

## Next steps

1. Validate the actual `dyna_int_js*` workflow chain end-to-end.
2. Confirm that Team search fetches `description` along with `correlation_id`, `number`, and `sys_id`.
3. Ensure create description includes the current fingerprint.
4. Ensure update is skipped when the existing description/fingerprint already matches.
5. Review DC logic so multiple DC items are paired correctly by correlation ID instead of relying on `[0]` only.
