# DR Ready - Status Update

Script: `Get-CohesityDRReadyPGConfig_v2.ps1`

Output location: `X:\PowerShell\Cohesity_API_Scripts\DR_Ready`

Purpose: Read-only Cohesity Helios collector used to capture active Protection Group configuration for DR reference and reconstruction. It collects NAS, SQL, Hyper-V, Nutanix AHV, Oracle, and Physical Protection Groups, exports the configuration in JSON format, and uses Helios V2 Protection Group data with V1 Protection Job information as supplemental configuration where applicable.

The script supports cluster and workload selection and is strictly GET-only, with no configuration changes made to Cohesity.
