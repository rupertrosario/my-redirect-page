# Jira Closure Update - Policy Summary and Alignment Scripts

## Status
Completed the current phase for the Cohesity policy summary and alignment scripts based on the scripts confirmed in `Old_Branch`.

## GitHub location
- Repository: `rupertrosario/my-redirect-page`
- Source branch: `Old_Branch`
- Tracking branch: `Cohesity_Automations`
- Tracking folder: `policy_summary_alignment/`

## Scripts confirmed in `Old_Branch`
| Script | Type | Purpose |
|---|---|---|
| `poli_js_inven` | Dynatrace JavaScript | Cohesity Policy Summary. Uses Dynatrace credential vault and Helios GET-only calls to collect clusters, policies, and protection groups. Excludes default policies and produces email-ready markdown. |
| `policy_com` | PowerShell | Cohesity Policy Summary CSV. Multi-cluster Helios GET-only export similar to the Cohesity Policy Details UI, with one row per non-default policy and PG count. |
| `poli` | PowerShell | Cohesity Policy → PG Retention Alignment Inventory. Validates policy retention against PG naming/environment expectations and includes replication/log retention details. |

## Additional script expected
One more script is expected but its exact filename still needs confirmation:

`Dynatrace JS | Cohesity Policy -> PPG Retention Alignment`

Known detail: approximately 606 lines.

## Work completed till now
- Checked the GitHub branch and identified the correct policy scripts in `Old_Branch`.
- Confirmed the policy summary JavaScript script.
- Confirmed the PowerShell policy summary CSV script.
- Confirmed the PowerShell policy-to-PG retention alignment script.
- Added a separate root-level tracking folder under `Cohesity_Automations`: `policy_summary_alignment/`.
- Added script status and Jira closure documentation under the tracking folder.

## Closure basis
Closing this Jira for the current completed phase because the policy summary and PowerShell retention-alignment scripts have been created and identified in GitHub, and the current tracking documentation has been added.

## Future enhancements
The remaining Dynatrace JS Policy → PPG Retention Alignment script should be added as a follow-up once its exact filename is confirmed. Any additional report fields, automation improvements, publishing to Confluence/Jira, or dashboard changes will be tracked separately.