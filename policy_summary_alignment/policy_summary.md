# Cohesity Policy Script Summary

## Correct source
The policy scripts are in `Old_Branch`.

## Confirmed scripts
| Script | Type | Summary |
|---|---|---|
| `poli_js_inven` | Dynatrace JavaScript | Cohesity Policy Summary. Uses Dynatrace credential vault and Helios GET-only calls to collect clusters, policies, and protection groups. Excludes default policies: Protect Once, Silver, Gold, Bronze. Produces summary and compact policy markdown table for email output. |
| `policy_com` | PowerShell | Cohesity Policy Summary CSV. Multi-cluster Helios GET-only export similar to Cohesity Policy Details UI. One CSV row per non-default policy. Includes PGCount only, not PG names. |
| `poli` | PowerShell | Cohesity Policy → PG Retention Alignment Inventory. Multi-cluster Helios GET-only script. Produces policy summary, exception-only console output, and full-detail CSV for PG retention alignment. |

## Retention alignment logic from `poli`
- 35D / 35 days = PROD
- 6M / 6 months = PROD
- 7Y / 7YR / 7 years = PROD
- 14D / 14 days = MOD/NONPROD
- 7D / 7 days = DEV

## PG naming validation from `poli`
- PROD policy → PG should contain PROD
- MOD/NONPROD policy → PG should contain MOD / NONPROD / NON-PROD / CAP
- DEV policy → PG should contain DEV
- LogShipping policy → PG should contain LogShipping, case-insensitive

## Additional script pending
A fourth script is expected but filename is still pending confirmation:

`Dynatrace JS | Cohesity Policy -> PPG Retention Alignment`

Known detail: approximately 606 lines.