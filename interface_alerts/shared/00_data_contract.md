# Interface Alerts Data Contract

## Scope

This contract is for the Cohesity Interface DOWN Dynatrace workflow under `interface_alerts`.

The workflow must keep Team and DC incident logic separate.

## Alert collection output

`01_get_alerts.js` returns latest alert rows for the 24h ET window.

Important fields:

| Field | Purpose |
|---|---|
| `Location` | Cluster/site location used for DC routing |
| `ClusterName` | Human-readable Cohesity cluster name |
| `ClusterId` | Cohesity cluster ID |
| `ClusterNodeId` | Node ID from alert property list |
| `IP` | Node IP extracted from alert property list |
| `AlertCode` | Alert type/code |
| `AlertState` | Alert state from Helios |
| `AlertCause` | Alert summary/cause |
| `LatestTimeET` | Alert latest time in ET |

## Validated interface state

Validation logic must confirm current interface state from Cohesity `/public/interface` before incident creation/update.

Confirmed DOWN rows should include:

| Field | Purpose |
|---|---|
| `ClusterName` | Cluster name |
| `ClusterId` | Cluster identity |
| `NodeIP` | Node IP |
| `NodeID` | Cohesity node ID |
| `BondName` | Bond/interface group |
| `Slave` | Physical slave interface |
| `LinkState` | Current interface link state |
| `MAC` | MAC address |
| `Speed` | Link speed |
| `SlotType` | Interface slot type |

## Team incident identity

Team incidents are cluster-level.

```text
correlation_id = cohesity_ifdown_team_<cluster_id>
```

Do not include IP, node, or interface in the Team `correlation_id`.

## Team update detection

Team update detection is IP/interface-level.

Fingerprint basis:

```text
NodeIP | Interface | LinkState
```

Expected behavior:

| Condition | Action |
|---|---|
| No active Team incident for cluster correlation ID | Create |
| One active Team incident and same fingerprint/description | No update |
| One active Team incident and changed fingerprint/description | Update |
| More than one active Team incident for same correlation ID | No write / manual review |

## ServiceNow search fields

ServiceNow search must return enough fields for comparison and action routing:

```text
sys_id,number,state,short_description,correlation_id,description,sys_updated_on
```

`work_notes` must not be required because it cannot be reliably fetched in the current workflow.

## Dynatrace loop variable

In the current Dynatrace UI, looped task field mappings should use:

```text
{{ _.item.<field> }}
```

Do not use old/incorrect examples such as:

```text
{{ _.loopItemValue.<field> }}
```

## DC incident identity

DC incidents are separate from Team incidents.

Recommended DC correlation ID basis:

```text
cohesity_ifdown_dc_<location>_<cluster_id>
```

DC update suppression can compare the generated current description against the existing ServiceNow description.

## Design constraint

The workflow should not update an incident just because an incident exists.

Correct rule:

```text
incident exists + state unchanged -> no update
incident exists + state changed   -> update
```
