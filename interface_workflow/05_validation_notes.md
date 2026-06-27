# Interface Workflow Validation Notes

Use these checks in the Dynatrace run output when Team incident works but DC incident is not created.

## 1. Confirm alerts are present

Expected action: `get_alerts`

Required checks:

```js
get_alerts.resultsCount > 0
get_alerts.results[0].NodeIP
get_alerts.results[0].ClusterId
get_alerts.results[0].Location
```

If `Location` is empty here, fix cluster metadata/location mapping in `01_get_alerts.js`.

## 2. Confirm validated DOWN rows are present

Expected action: `validate_interfaces`

Required checks:

```js
validate_interfaces.downCount > 0
validate_interfaces.downRows[0].NodeIP
validate_interfaces.downRows[0].Slave
validate_interfaces.downRows[0].LinkState
```

If this is empty, the alert exists but `/public/interface` does not currently confirm the slave interface as DOWN.

## 3. Confirm Location is preserved after validation

This is the most likely DC failure point.

```js
validate_interfaces.downRows.filter(r => !r.Location && !r.DcLocation)
```

Expected result: `[]`

If rows are returned, DC routing will skip them because `dc_iterations` cannot map them to a DC.

## 4. Confirm DC iteration input/output

Expected action: `dc_iterations`

Required checks:

```js
dc_iterations.inputDownRows
dc_iterations.routedRows
dc_iterations.skippedNoLocationCount
dc_iterations.skippedNotAllowlistedCount
dc_iterations.dcCount
```

Interpretation:

| Field | Meaning |
|---|---|
| `inputDownRows` | Rows received from `validate_interfaces` |
| `routedRows` | Rows with an allowlisted DC location |
| `skippedNoLocationCount` | Rows skipped because Location was empty |
| `skippedNotAllowlistedCount` | Rows skipped because Location was not in the DC allowlist |
| `dcCount` | Number of DC incident candidates created |

## 5. Confirm DC incident candidate exists

```js
dc_iterations.dcIncidentCandidates
```

Expected: at least one object with:

```js
Location
ClusterName
ClusterId
CorrelationId
ShortDescription
Description
Rows
```

If `dcIncidentCandidates` is empty, do not troubleshoot ServiceNow yet. The issue is still before incident creation.

## 6. Confirm ServiceNow search/create branch condition

For the DC path, the create/update branch should use:

```js
dc_iterations.dcCount > 0
```

or iterate directly on:

```js
dc_iterations.dcIncidentCandidates
```

Do not use `teamIncidentCandidates` or `downRows` directly in the DC create step.

## 7. Correlation ID convention

Team:

```text
cohesity_ifdown_bkup_team_<ClusterId>
```

DC:

```text
cohesity_ifdown_dc_<ClusterId>_<Location>
```

Example:

```text
cohesity_ifdown_dc_12345_San_Antonio
```
