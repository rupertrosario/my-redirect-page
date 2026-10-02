# DR Ready - Status Update

Current work is focused on a simple GET-only Cohesity Protection Group configuration export.

The script collects active Protection Group configuration for NAS, SQL, Hyper-V, Nutanix AHV, Oracle, and Physical workloads from a selected Helios cluster, saves the raw configuration as JSON, and generates a field-name/type-only structure view so the configuration can be reviewed without exposing production values.

For now, the scope is to collect, inspect, and validate the output on one cluster first. Any collection issues are recorded in `Errors.json`.
