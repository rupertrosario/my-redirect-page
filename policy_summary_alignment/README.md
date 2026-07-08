# Policy Summary and Alignment

## Purpose
Root-level tracking folder for the Cohesity policy summary and alignment script work.

## Correct source branch
The policy scripts are in `Old_Branch`.

## Tracking location
- Repository: `rupertrosario/my-redirect-page`
- Tracking branch: `Cohesity_Automations`
- Folder: `policy_summary_alignment/`

## Confirmed scripts in `Old_Branch`
| Script | Type | Purpose |
|---|---|---|
| `poli_js_inven` | Dynatrace JavaScript | Cohesity Policy Summary; Helios GET-only; Dynatrace credential vault; email markdown output. |
| `policy_com` | PowerShell | Cohesity Policy Summary CSV; multi-cluster; GET-only; one row per non-default policy. |
| `poli` | PowerShell | Cohesity Policy → PG Retention Alignment Inventory; retention classification and PG naming/environment alignment. |

## Expected additional script
One more script is expected but exact filename is pending confirmation:

`Dynatrace JS | Cohesity Policy -> PPG Retention Alignment`

Known detail: approximately 606 lines.

## Files in this tracking folder
| File | Purpose |
|---|---|
| `scripts/README.md` | Index of confirmed `Old_Branch` policy scripts. |
| `policy_scripts_status.md` | Status of scripts found and current/future scope. |
| `jira_closure_update_policy_scripts.md` | Jira-ready closure update for this phase. |

## Closure position
Current phase can be closed based on the confirmed policy scripts in `Old_Branch`. The remaining 606-line Dynatrace JS PPG retention-alignment script should be added as a follow-up once the exact filename is confirmed.