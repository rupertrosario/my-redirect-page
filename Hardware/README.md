# Cohesity Helios Node + Chassis Inventory

Read-only PowerShell inventory for Cohesity clusters connected through Helios.

## Script
`Cohesity_Helios_Hardware_Inventory.ps1`

## Strict GET-only design
The script performs only these API reads:

```text
GET /v2/mcm/cluster-mgmt/info
GET /v2/clusters/nodes
GET /v2/chassis
```

No POST, PUT, PATCH, or DELETE requests are used.

## Collection model
- Discover the Helios-connected clusters.
- Query `GET /v2/clusters/nodes` once per cluster. Omitting `ids` returns all nodes.
- Query `GET /v2/chassis` once per cluster.
- Correlate `chassis.nodeIds[]` with each node's internal `nodeId`.
- Export one combined CSV row per node.
- `NodeId` is used internally for the join but is not displayed or exported.

## CSV columns

```text
ClusterName
Hostname
NodeIP
IPMIIP
NodeSerial
CohesityNodeSerial
NodeModel
ProductModel
SlotNumber
ChassisSerial
CohesityChassisSerial
ChassisModel
ChassisName
RackId
```

Missing values are written as `N/A`; no IP address or hardware value is inferred.

## Expected environment validation

```text
Clusters : 22
Nodes    : 169
```

A PASS additionally requires:
- zero failed clusters
- all 169 node rows returned
- all returned nodes mapped to a chassis

## Output
One timestamped CSV:

```text
Cohesity_Node_Chassis_Inventory_yyyyMMdd_HHmm.csv
```

Default output directory:

```text
X:\PowerShell\Data\Cohesity\HardwareInventory
```

The console shows the same combined node + chassis table and one summary at the end.

## Summary example

```text
Clusters discovered     : 22
Clusters successful     : 22
Clusters failed         : 0
Nodes discovered        : 169
Chassis discovered      : <actual>
Nodes mapped to chassis : 169
Unmapped nodes          : 0
Nodes with Node IP      : <actual>
Nodes with IPMI IP      : <actual>
Expected clusters       : 22
Expected nodes          : 169
Validation              : PASS
```
