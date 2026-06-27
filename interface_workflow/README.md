# Cohesity Interface-Down Dynatrace Workflow

This folder documents the current interface-down workflow shown in the screenshot and stores the JavaScript actions that should be used in the Dynatrace workflow.

## Workflow image explained

The workflow is already shaped correctly:

1. **Schedule trigger** starts the run.
2. **Get alerts** pulls Cohesity Helios alerts for the interface-related alert codes.
3. **Validate interface** calls `/irisservices/api/v1/public/interface` and confirms whether the alert node has a real DOWN slave interface.
4. The workflow then splits into two incident paths:
   - **Team incident path**: one incident per affected Cohesity cluster.
   - **DC incident path**: one incident per affected datacenter/location.
5. Each incident path should first search for an existing open incident, then either update that incident or create a new one.

## Correct data contract between actions

The main rule is: **validated interface rows must carry Location forward**.

`get_alerts` may correctly find alerts, IP, node ID, and location, and `validate_interfaces` may correctly find DOWN rows. But DC incident creation will still fail if `validate_interfaces.downRows[]` does not include `Location` or `DcLocation`.

Required fields from `validate_interfaces.downRows[]`:

| Field | Needed by |
|---|---|
| ClusterName | Team + DC incidents |
| ClusterId | Team + DC correlation/search |
| Location | DC allowlist/routing |
| DcLocation | DC allowlist/routing fallback |
| NodeIP | Incident short description/details |
| NodeID | Incident details |
| ChassisSerial | Incident details |
| BondName | Incident details |
| Slave | Incident details |
| LinkState | Incident details |
| MAC | Incident details |
| Speed | Incident details |
| SlotType | Incident details |

## Files

| File | Dynatrace action |
|---|---|
| `01_get_alerts.js` | Get Cohesity alerts: 1077, 1105, 13023 |
| `02_validate_interfaces.js` | Validate DOWN interfaces and preserve Location |
| `03_team_iterations.js` | Build one Team incident candidate per cluster |
| `04_dc_iterations.js` | Build one DC incident candidate per cluster + location |
| `05_validation_notes.md` | Fast checks for why DC incident is not created |

## Current likely failure point

If Team incident data exists but one DC incident is not created, check `dc_iterations` input first:

```js
validate_interfaces.downRows.filter(r => !r.Location && !r.DcLocation)
```

If rows are returned, the DC branch is being skipped because it cannot route without location.

## DC allowlist

Current DC incident creation should allow only these locations unless the operational scope changes:

- San Antonio
- Carrollton
- Detroit
- Ashburn
