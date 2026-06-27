# Cohesity Interface-Down Dynatrace Workflow

The current change fixes only Team incident creation/update.

```text
Schedule -> Get Alerts -> Validate Interfaces -> Build Team Iterations
-> For Each Team Iteration -> Search Team Incident -> Normalize Team Search Result
-> action == update: update existing incident by sys_id
-> action == create: create incident
```

Branch only on normalized `action`, never on the raw ServiceNow response shape. `shared/00_data_contract.md` defines the handoff, `team/` contains the implementation, and `dc/` is reserved for later work. Existing root-level alert, validation, and DC files remain unchanged.
