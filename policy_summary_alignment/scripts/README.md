# Policy Scripts - Old_Branch Source Index

## Correct source branch
The policy scripts were found in `Old_Branch`.

## Confirmed scripts
| Source file in `Old_Branch` | Type | Purpose |
|---|---|---|
| `poli_js_inven` | Dynatrace JavaScript | Cohesity Policy Summary. Uses Dynatrace credential vault, Helios GET-only, gets all clusters, policies, and protection groups, excludes default policies, and produces compact email markdown. |
| `policy_com` | PowerShell | Cohesity Policy Summary CSV. Multi-cluster Helios GET-only export similar to Cohesity Policy Details UI; one row per non-default policy; includes PG count only. |
| `poli` | PowerShell | Cohesity Policy → PG Retention Alignment Inventory. Multi-cluster Helios GET-only validation of policy retention against PG naming/environment expectations and replication/log retention. |

## Pending source file name confirmation
There should be one more Dynatrace JavaScript script, approximately 606 lines, described as:

`Dynatrace JS | Cohesity Policy -> PPG Retention Alignment`

I have not yet identified its exact filename in `Old_Branch`. Once the filename is confirmed, it should be added to this index and included in the closure note.

## Current handling
The scripts remain in `Old_Branch` as the source of truth. This folder tracks the policy-script closure package on `Cohesity_Automations`.