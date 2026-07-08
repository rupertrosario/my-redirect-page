# Cohesity Interface Alerts

## Purpose

This folder tracks the Dynatrace workflow for Cohesity interface alerts.

The root scripts are generic and must stay outside Team/DC folders.

## Root workflow scripts

| File | Purpose | Status |
|---|---|---|
| `01_get_alerts.js` | Generic Cohesity Helios alert collector. Pulls alert data and prepares alert rows. | Present |
| `02_validate_interfaces.js` | Generic interface validation. Confirms current interface DOWN state through Cohesity `/public/interface`. | Present - reconstructed JS3 validator |
| `03_team_iterations.js` | Team-specific ServiceNow search iteration helper. | Present |

## Correct layout

```text
interface_alerts/
  01_get_alerts.js              # generic alert collector
  02_validate_interfaces.js     # generic validator
  03_team_iterations.js         # Team ServiceNow iteration helper
  shared/
    00_data_contract.md
  team/
    04_update_team_incident_payload.js
  dc/
    README.md
```

## Source note

`02_validate_interfaces.js` is a reconstructed JS3 validator created from the screenshots and older working validator patterns.

The earlier wrong 413-line copy from `Old_Branch/interface_workflow/02_validate_interfaces.js` was removed.

Known markers included in the reconstructed validator:

```text
Dynatrace JS3 | Cohesity Helios Interface DOWN validator
TEAM = ONE INCIDENT PER CLUSTER
DC = ONE INCIDENT PER LOCATION + CLUSTER
TRIGGER_CODES = [...]
getAssignmentGroup(loc)
ALLY - FACILITIES DATA CENTER ASHBURN DC2
ALLY - HOSTING BACKUP
ALLY - FACILITIES DATA CENTER SAN ANTONIO
ALLY - FACILITIES DATA CENTER CARROLLTON
```

## Design rule

`01_get_alerts.js` and `02_validate_interfaces.js` are generic.

They should not be treated as Team-only scripts.

Team and DC handling should start only after validation has produced the relevant candidate outputs.
