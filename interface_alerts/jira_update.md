# JIRA Update - Cohesity Interface DOWN Dynatrace Automation

## Summary

The Cohesity Interface DOWN Dynatrace automation was reworked because the earlier create/update logic could repeatedly update ServiceNow incidents even when the underlying interface-down state had not changed.

The current design keeps Team incidents at the cluster level while using IP/interface-level state to decide whether an existing incident actually needs to be updated.

## Why this was reworked

The earlier approach was too broad:

```text
active incident exists -> update
```

That caused risk of repeated comments/work notes on every workflow run for the same condition.

The intended operational model is:

```text
same cluster + same IP/interface state -> no update
same cluster + different IP/interface state -> update existing cluster-level Team incident
no active cluster incident -> create new Team incident
```

This avoids ticket noise while still capturing meaningful changes.

## Current Team incident model

Team incidents are created at cluster level.

```text
correlation_id = cohesity_ifdown_team_<cluster_id>
```

IP address, node, and interface name must not be part of the Team correlation ID.

The IP/interface details are used for change detection and incident details.

## Current Team update logic

The workflow validates the current DOWN interface state and builds a fingerprint from the confirmed down interfaces.

Fingerprint basis:

```text
NodeIP | Interface | LinkState
```

Decision logic:

| Scenario | Action |
|---|---|
| No active Team incident exists for the cluster | Create Team incident |
| Active Team incident exists and fingerprint/description is unchanged | No update |
| Active Team incident exists and fingerprint/description changed | Update existing Team incident |
| More than one active incident exists for the same correlation ID | No write / manual review |

This gives us one active Team incident per affected Cohesity cluster, but still updates the incident when a different IP/interface becomes impacted.

## Current DC incident model

DC incidents are separate from Team incidents.

For DC incidents, the current comparison approach is based on the current generated description versus the existing incident description.

```text
same location+cluster + same description -> no update
same location+cluster + changed description -> update
```

This avoids the need to fetch ServiceNow work notes.

## Important constraint

The current ServiceNow workflow/action cannot reliably fetch `work_notes`.

Repeat-update suppression must use values available from the ServiceNow search result, mainly:

```text
correlation_id
number
sys_id
description
```

The description/fingerprint comparison is the practical design for suppressing repeated updates.

## Current state

Completed / confirmed direction:

- Dynatrace pulls Cohesity networking alerts.
- Alert node IP is extracted.
- Alerts are deduplicated by cluster and IP for the run.
- Cohesity `/public/interface` is used to confirm the interface is currently DOWN.
- Team incident identity is cluster-level.
- Team update detection is IP/interface-level.
- Repeated updates should be skipped when the same cluster has the same DOWN interface state.
- Work notes are not required for this comparison.

Still to validate:

- Actual `dyna_int_js*` task chain end-to-end in Dynatrace.
- That ServiceNow search returns `description` consistently.
- That create description includes the fingerprint/current state needed for later comparison.
- That Team update action only runs when the comparison task returns `shouldUpdate = true`.
- DC multiple-item handling should avoid blind `[0]` matching and should pair records by correlation ID where possible.

## Target behavior

```text
One active Team incident per affected cluster.
Update only when impacted IP/interface state changes.
No repeat update for the same cluster + same interface state.
```
