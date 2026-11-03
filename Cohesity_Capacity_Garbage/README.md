# Cohesity Capacity and Garbage Collection — API Parameters

PowerShell script: [Get-CohesityCapacityGarbage_v2.ps1](Get-CohesityCapacityGarbage_v2.ps1)

## Time-series parameter comparison

| Parameter | Garbage Data | Reclaimed Physical |
|---|---|---|
| Range | `day` (last 24 hours) | `day` (last 24 hours) |
| Rollup function | `latest` | `latest` |
| Rollup interval | `30` seconds | `30` seconds |
| Metric unit type | `0` | `0` |
| Start/end time | Dynamically calculated 24-hour UTC window | Dynamically calculated 24-hour UTC window |
| Schema name | `ApolloV2ClusterStats` | `MRCounters` |
| Metric name | `EstimatedGarbageBytes` | `bytes_value` |
| Entity ID | Cluster-specific name/ID | `ReclaimedBytesPhysical` |
| Entity name | Not provided | `ReclaimedBytesPhysical` |

Both queries use the Cohesity Helios **GET** endpoint
`/irisservices/api/v1/public/statistics/timeSeriesStats`
with an `accessClusterId` header for the selected cluster.

The report selects the latest timestamped data point returned for each metric, converts byte values to GB and TB using 1024-based units (GiB/TiB), and outputs CSV.

**Note:** A `latest` rollup produces a latest value within each interval; selecting the final time-series data point in the script gives the newest reported value, not a 24-hour total or average. Different metric schemas/entities mean the two numbers are not directly equivalent.
