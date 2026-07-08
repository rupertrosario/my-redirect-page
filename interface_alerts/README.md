# Cohesity Interface Alerts

## Purpose

This folder tracks the Dynatrace workflow for Cohesity interface alerts.

The root scripts are generic and must stay outside Team/DC folders.

## Root workflow scripts

| File | Purpose |
|---|---|
| `01_get_alerts.js` | Generic Cohesity Helios alert collector. Pulls alert data and prepares alert rows. |
| `02_validate_interfaces.js` | Generic interface validation. Confirms current interface DOWN state through Cohesity `/public/interface`. |
| `03_team_iterations.js` | Team-specific ServiceNow search iteration helper. |

## Folder layout

```text
interface_alerts/
  01_get_alerts.js
  02_validate_interfaces.js
  03_team_iterations.js
  shared/
    00_data_contract.md
  team/
    04_update_team_incident_payload.js
  dc/
    README.md
```

## Design rule

`01_get_alerts.js` and `02_validate_interfaces.js` are generic.

They should not be treated as Team-only scripts.

Team and DC handling should start only after validation has produced the relevant candidate outputs.
