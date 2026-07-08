# Policy Scripts

## Actual script locations found in GitHub
The policy/inventory scripts already exist in the branch under `inventory/`.

| Script | Current source location | Purpose |
|---|---|---|
| `Get-CohesityProtectionInventory.ps1` | `inventory/Get-CohesityProtectionInventory.ps1` | Cohesity protection inventory / policy summary across protection groups. |
| `Get-PhysicalPGInventory.ps1` | `inventory/Get-PhysicalPGInventory.ps1` | Physical Protection Group inventory and alignment details including policy, protection type, object selection, exclude paths, and last-run status. |
| `Test-CohesityProtectionInventoryCsv.ps1` | `inventory/Test-CohesityProtectionInventoryCsv.ps1` | Validation script for Cohesity protection inventory CSV output. |
| `Test-PhysicalPGInventoryCsv.ps1` | `inventory/Test-PhysicalPGInventoryCsv.ps1` | Validation script for physical PG inventory CSV output. |

## Related documentation found
| Document | Current source location | Purpose |
|---|---|---|
| `Cohesity_Protection_DataContract.md` | `inventory/Cohesity_Protection_DataContract.md` | Data contract for Cohesity protection inventory fields. |
| `GENERIC_PROTECTION_RUNBOOK.md` | `inventory/GENERIC_PROTECTION_RUNBOOK.md` | Runbook for generic protection inventory workflow. |
| `GENERIC_PROTECTION_VM_FIELDS.md` | `inventory/GENERIC_PROTECTION_VM_FIELDS.md` | VM field mapping/reference. |
| `GENERIC_PROTECTION_WORKLOG.md` | `inventory/GENERIC_PROTECTION_WORKLOG.md` | Worklog/status notes. |
| `PowerBI_Cohesity_Protection_Measures.dax` | `inventory/PowerBI_Cohesity_Protection_Measures.dax` | Power BI measures for protection inventory reporting. |
| `PowerBI_PhysicalPG_Dashboard_Baseline.md` | `inventory/PowerBI_PhysicalPG_Dashboard_Baseline.md` | Dashboard baseline for physical PG inventory. |
| `PowerBI_PhysicalPG_Dashboard_Measures.dax` | `inventory/PowerBI_PhysicalPG_Dashboard_Measures.dax` | DAX measures for physical PG dashboard. |

## Note
This folder is the policy-summary/alignment tracking folder. The working source scripts remain under `inventory/` to avoid breaking existing references. Future enhancements can either continue in `inventory/` or move scripts into this folder with a follow-up PR/change.