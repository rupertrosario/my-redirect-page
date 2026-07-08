# Cohesity Interface Alerts Automation

## Purpose

Dynatrace workflow automation for Cohesity Interface DOWN alerts.

The workflow pulls Cohesity Helios network alerts, validates current interface state from Cohesity API, and drives ServiceNow incident create/update decisions for:

- Team incident workflow
- DC incident workflow

## Current design

Team incidents are created at cluster level.

```text
correlation_id = cohesity_ifdown_team_<cluster_id>
```

IP address and interface name are not part of the Team incident correlation ID.

The IP/interface state is used only for update detection.

## Key rule

```text
Same cluster + same IP/interface state  -> no update
Same cluster + changed IP/interface     -> update existing Team incident
No active cluster Team incident         -> create Team incident
```

This keeps one active Team incident per affected Cohesity cluster and prevents repeated updates for the same unchanged interface state.

## Folder layout

```text
interface_alerts/
  01_get_alerts.js
  jira_update.md
  shared/
    00_data_contract.md
  team/
    README.md
    03_compare_team_incident_state.js
    04_update_team_incident_payload.js
  dc/
    README.md
    03_compare_dc_incident_state.js
  legacy/
    dyna_int_inc_reference.js
```

## Workflow state

Current workflow work is still being validated in Dynatrace. The GitHub folder is organized to keep the scripts and logic together so the final working chain can be confirmed without mixing it with policy summary or backup failure work.

## ServiceNow constraint

The workflow cannot rely on fetching ServiceNow `work_notes`.

Update suppression therefore uses available search fields such as:

```text
correlation_id
number
sys_id
description
```

The description/fingerprint is the source for deciding whether an existing incident needs another update.
