# Policy Scripts Status Summary

## Correct source
The policy scripts are in `Old_Branch`, not under `inventory/` and not under `dtsk_backup_status/`.

## Confirmed scripts found in `Old_Branch`
| Script | Status | Details |
|---|---|---|
| `poli_js_inven` | Found | Dynatrace JS Cohesity Policy Summary; GET-only via Helios; uses Dynatrace credential vault; excludes default policies. |
| `policy_com` | Found | PowerShell Cohesity Policy Summary CSV; multi-cluster; Helios GET-only; one CSV row per non-default policy. |
| `poli` | Found | PowerShell Cohesity Policy → PG Retention Alignment Inventory; classifies policy retention and validates PG naming/environment alignment. |

## Expected additional script
A fourth script is expected but filename is not yet confirmed:

`Dynatrace JS | Cohesity Policy -> PPG Retention Alignment`

Known detail: approximately 606 lines.

## What has been completed till now
- Located the correct source branch: `Old_Branch`.
- Confirmed three policy scripts by exact filename.
- Created separate tracking folder: `policy_summary_alignment/` on `Cohesity_Automations`.
- Added script index and closure update for the current policy-script work.

## Closure basis
Current Jira can be closed for the work completed so far: the policy summary and PowerShell retention-alignment scripts are available in `Old_Branch`, and the tracking/closure documentation is now added under `policy_summary_alignment/`.

## Future enhancement / follow-up
Add the missing 606-line Dynatrace JS Policy → PPG Retention Alignment script after its exact filename is identified or confirmed. Any additional changes/enhancements should be handled separately.