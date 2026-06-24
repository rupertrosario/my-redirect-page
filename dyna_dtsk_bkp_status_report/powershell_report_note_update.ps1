# ==========================================================
# PowerShell Report Note Update Snippet
# Purpose:
# - Use this note text in the PowerShell backup validation email/report
# - Keeps the PowerShell report aligned with the Dynatrace manager email
# - Report-only text change; no ServiceNow or Cohesity write action
# ==========================================================

$ReportNote = @"
**Note:** NAS backups are excluded from this server decommission validation. **No Backup Found** means no in-scope Cohesity backup object was found for the CI. **DB Only / No Server Backup** means a SQL/Oracle backup was found, but no FS, VM, Hyper-V, or Nutanix/AHV backup was found for the server. Servers with names containing `db` or `cn` may require DB-level backup review if only FS/VM backup is found.
"@

# Example: append to an existing Markdown/HTML-safe report body
# $EmailBody += "`n`n$ReportNote"

# Example: append to a text report
# Add-Content -Path $ReportPath -Value ""
# Add-Content -Path $ReportPath -Value $ReportNote
