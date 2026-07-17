# JIRA Update - Cohesity Interface DOWN Dynatrace Automation

## Summary

The Cohesity Interface DOWN Dynatrace workflow was reworked because the earlier implementation was not looping correctly through the ServiceNow create/update path. This required multiple rounds of testing and manual validation to confirm how Dynatrace loop items, ServiceNow search results, and incident update conditions behave.

The current automation is being organized under:

```text
Cohesity_Automations / Interface
```

## Scripts in scope

| Script | Purpose | Current state |
|---|---|---|
| `01_get_alerts.js` | Generic Cohesity Helios alert collector | Present |
| `02_validate_interfaces.js` | Generic interface validation using Cohesity `/public/interface` | Recreated / under validation |
| `03_snow_search_team.md` | Team ServiceNow search mapping | Copied from `Old_Branch/interface_workflow` |
| `04_normalize_team_search.js` | Normalizes Team ServiceNow search output into create/update/no-write paths | Copied from `Old_Branch/interface_workflow` |

## Why this was reworked

The earlier workflow did not loop reliably. The ServiceNow actions were failing or skipping because the loop input and condition handling were not aligned with Dynatrace’s expected loop item structure.

The main issues were:

- Loop item mapping had to use the correct Dynatrace loop object.
- Boolean conditions such as `length > 0` could not be placed where Dynatrace expected a list.
- ServiceNow search output had to be normalized before create/update decisions.
- The workflow needed a clearer split between generic alert/interface validation and incident-specific Team/DC handling.

## Current status

Team incident processing is working.

The Team path now follows this operating model:

```text
get_alerts
  -> validate_interfaces
  -> snow_search_team
  -> normalize_team_search
  -> create/update Team incident
```

The Team incident model is:

```text
one active Team incident per Cohesity cluster
```

DC incident creation still needs to be finished and validated.

The DC path should follow this model:

```text
one active DC incident per Location + Cohesity cluster
```

## Idempotency and duplicate prevention

The workflow must be idempotent. It should not create accidental duplicate incidents or repeatedly update the same incident when the underlying interface state has not changed.

A key limitation is that the current ServiceNow workflow/action cannot reliably fetch incident `work_notes`.

Because `work_notes` cannot be used as the source of truth, idempotency must rely on searchable and retrievable fields such as:

```text
correlation_id
number
sys_id
description
current generated fingerprint
```

The intended pattern is:

```text
correlation_id decides whether an incident already exists
fingerprint/description decides whether the existing incident needs an update
```

This avoids these failure modes:

- creating duplicate Team incidents for the same cluster
- creating duplicate DC incidents for the same location + cluster
- updating an incident on every workflow run when the interface state is unchanged
- depending on work_notes that cannot be fetched consistently

## Team incident behavior

Team incident identity is cluster-level.

```text
Team correlation_id = cluster-level identifier
```

The interface state is not part of the Team incident identity. IP/interface details are used for fingerprinting and description content.

Expected Team behavior:

| Condition | Action |
|---|---|
| No active Team incident exists for the cluster | Create Team incident |
| Active Team incident exists and interface fingerprint is unchanged | No update |
| Active Team incident exists and interface fingerprint changed | Update existing Team incident |
| More than one active incident exists for same correlation ID | No write / manual review |

## DC incident behavior still pending

DC incident creation must still be completed.

Expected DC behavior:

| Condition | Action |
|---|---|
| No active DC incident exists for location + cluster | Create DC incident |
| Active DC incident exists and description/fingerprint unchanged | No update |
| Active DC incident exists and description/fingerprint changed | Update existing DC incident |
| More than one active DC incident exists for same correlation ID | No write / manual review |

## Current next steps

1. Validate the recreated `02_validate_interfaces.js` in Dynatrace.
2. Confirm Team create/update remains stable after the folder cleanup.
3. Finish DC incident creation mapping.
4. Add DC search and normalization logic only after Team remains stable.
5. Confirm idempotency using `correlation_id` and description/fingerprint, not `work_notes`.

## JIRA status text

The interface automation has been reworked because the original workflow did not loop correctly through the ServiceNow create/update path. Multiple tests were performed to stabilize the Team incident path. Team incident creation/update is now working. The remaining work is to complete and validate DC incident creation.

A key design requirement is idempotency. Since ServiceNow `work_notes` cannot be reliably fetched by the workflow, the automation cannot depend on work notes to determine whether an incident was already updated. Instead, the workflow must use correlation IDs for incident identity and searchable fields such as description/fingerprint to avoid duplicate incidents and unnecessary repeated updates.
