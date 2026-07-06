# backup_failures guardrails

Use `Cohesity_Backup_Failures` only as reference. Do not edit it for the incident evidence workflow.

The incident evidence workflow must be a single standalone script:

```text
backup_failures/Get-CohesityBackupFailureIncidentEvidence.ps1
```

Hard rules:

1. No Excel or XLSX.
2. Output only these incident files:
   - current_failures.csv
   - recovered.csv
   - new_failures.csv
   - new_recoveries.csv
   - worknotes.txt
   - state.json
3. Cohesity API calls are GET-only.
4. Use the same 18:00 ET -> next-day 18:00 ET window as `compute_window.js`.
5. Ask for the incident number once per window, then reuse it until the next window.
6. Do not update ServiceNow; the team attaches/pastes the generated files manually.
