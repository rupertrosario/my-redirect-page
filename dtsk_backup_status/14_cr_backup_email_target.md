# CR Backup Validation Email - Target Format

## Purpose

Email/report format for the per-CR backup validation sub-workflow.

The CR number comes from the sub-workflow input and should be referenced in the email action with:

```jinja
{{ input().crNumber }}
```

The backup validation task loops over the `cr_ci` result and returns one validation result per server.

## Subject

```jinja
Cohesity Backup Validation - {{ input().crNumber }}
```

## Target body

```markdown
# Cohesity Backup Validation - {{ input().crNumber }}

## Summary

| Metric | Count |
|---|---:|
| CIs reviewed | <count> |
| Protected | <count> |
| No Backup Found | <count> |
| Review Required | <count> |
| Validation Errors | <count> |

## Details

| Server | Backup Type | Object | Cluster | Protection Group | Latest Backup | Status |
|---|---|---|---|---|---|---|
| server01 | VM | server01 | cluster01 | VM-Daily | 09/11/2026 02:15 | Protected |
| server02 | FS | server02 | cluster02 | Physical-Daily | 09/11/2026 01:42 | Protected |
| dbserver01 | SQL | dbserver01/DB01 | cluster03 | SQL-Daily | 09/11/2026 03:05 | Protected |
| server04 | No Backup Found | server04 | N/A | - | N/A | No Backup Found |
| dbserver02 | Server Backup / No DB Backup | dbserver02 | N/A | - | N/A | Review Required |
| server06 | Validation Error | server06 | N/A | - | N/A | Unable to Validate |

NOTE:
- NAS backups are excluded from this server validation.
- **No Backup Found** is reported only when Cohesity searches completed successfully and returned no supported backup object for the CI.
- **Validation Error / Unable to Validate** means the result could not be determined because required Cohesity cluster/API/credential data was unavailable. It must not be interpreted as no backup.
- **DB Only / No Server Backup** means SQL/Oracle backup was found, but no FS/VM/Hyper-V/Nutanix backup was found.
- **Server Backup / No DB Backup** means a DB/CN-named server has server-level backup but no SQL/Oracle backup was found after the DB fallback search.
- If only some cluster/API searches fail and a backup is still found, the validator returns `ValidatedWithWarnings` and preserves the warnings for review.
```

## Validator status mapping

| BackupType / Validation State | Email Status |
|---|---|
| FS / VM / HyperV / Nutanix / SQL / Oracle | Protected |
| NoObject | No Backup Found |
| NoFSBackupFound | Review Required |
| NoDBBackupFound | Review Required |
| ValidationError | Unable to Validate |
| InvalidCI | Invalid CI |
| ValidatedWithWarnings | Protected - Warning |

## Workflow position

```text
get_cis
→ normalize_cr_ci
→ cr_ci
→ cr_backup_validate_one_ci   [LOOP over {{ result("cr_ci") }}]
→ cr_backup_aggregate_report  [next step]
→ send email                  [next step]
```

## Important test rule

Do not send to the production distribution list yet. Test with one CR and a test mailbox until these are confirmed:

1. Server names are read correctly from the nested `cr_ci` loop item (`[ { name: "server" } ]`).
2. Protected servers show the expected backup type, cluster, protection group, and latest backup time.
3. A real API/cluster failure is shown as **Unable to Validate**, not **No Backup Found**.
4. DB/CN servers trigger the database fallback logic when required.
5. Warnings are retained when only part of the cluster search fails.
