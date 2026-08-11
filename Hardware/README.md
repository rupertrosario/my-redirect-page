# Cohesity Helios Hardware Inventory

Read-only PowerShell hardware inventory for Cohesity clusters connected through Helios.

## Script
`Cohesity_Helios_Hardware_Inventory.ps1`

## What it does
- Uses the existing AES-protected Cohesity API key helper.
- Discovers clusters from `GET /v2/mcm/cluster-mgmt/info`.
- Routes each cluster call through Helios using `accessClusterId`.
- Calls `GET /v2/node/hardware-info` for every discovered cluster.
- Continues if an individual cluster fails.
- Displays one final hardware table and summary, without per-cluster progress spam.
- Exports detailed inventory and cluster-status CSV files.
- Performs GET requests only.

## Inventory fields
`ClusterName`, `ClusterId`, `NodeId`, `ChassisType`, `ChassisModel`, `ChassisSerial`, `CohesityChassisSerial`, `CohesityNodeSerial`, `NodeSerial`, `NodeModel`, `ProductModel`, `ProductModelType`, `SlotNumber`, `MaxSlots`, `IpmiLanChannel`, `HbaModel`.

## Quick validation
Run the script and verify:

```text
Clusters discovered = expected Helios cluster count
Clusters failed     = 0
Hardware rows       = expected physical-node count
```

Then spot-check one known node against Helios for `chassisSerial`, `cohesityNodeSerial`, `nodeSerial`, `productModel`, and `slotNumber`.
