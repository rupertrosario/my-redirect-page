# Policy Summary and Alignment

## Purpose
Root-level tracking folder for the Cohesity policy summary and alignment script work.

## Location
- Repository: `rupertrosario/my-redirect-page`
- Branch: `Cohesity_Automations`
- Folder: `policy_summary_alignment/`

## Actual scripts found
The working source scripts are present under `inventory/`:

| Script | Purpose |
|---|---|
| `inventory/Get-CohesityProtectionInventory.ps1` | Cohesity protection inventory / policy summary script. |
| `inventory/Get-PhysicalPGInventory.ps1` | Physical PG inventory/alignment script covering policy, protection type, objects, exclusions, directive file, status, and last-run details. |
| `inventory/Test-CohesityProtectionInventoryCsv.ps1` | Validation for protection inventory CSV output. |
| `inventory/Test-PhysicalPGInventoryCsv.ps1` | Validation for physical PG inventory CSV output. |

## Supporting documentation found
- `inventory/Cohesity_Protection_DataContract.md`
- `inventory/GENERIC_PROTECTION_RUNBOOK.md`
- `inventory/GENERIC_PROTECTION_VM_FIELDS.md`
- `inventory/GENERIC_PROTECTION_WORKLOG.md`
- `inventory/PowerBI_Cohesity_Protection_Measures.dax`
- `inventory/PowerBI_PhysicalPG_Dashboard_Baseline.md`
- `inventory/PowerBI_PhysicalPG_Dashboard_Measures.dax`

## Tracking files in this folder
| File | Purpose |
|---|---|
| `scripts/README.md` | Exact policy script locations and purpose. |
| `policy_scripts_status.md` | What was found and what has been completed. |
| `jira_closure_update_policy_scripts.md` | Jira-ready closure update for the policy script work. |

## Closure position
Current phase can be closed because the policy summary/alignment scripts and supporting files are present in GitHub and have been documented in this folder. Future enhancements will be handled separately.