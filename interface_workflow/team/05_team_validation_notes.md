# Team flow validation

Expected flow:

```text
validate_interfaces -> build_team_iterations -> for_each_team_iteration
-> search_team_incident -> normalize_team_search_result
-> action == update: update by sys_id
-> action == create: create from payload
```

Prefer this active-incident search when correlation is searchable:

```text
correlation_id=cohesity_ifdown_bkup_team_<ClusterId>^stateNOT IN6,7^ORDERBYDESCsys_updated_on
```

Otherwise search short description for both `Cohesity Interface DOWN` and the cluster name, excluding states 6 and 7. Do not search by IP because Team incidents are cluster-scoped.

For two DOWN rows in different clusters, verify `teamCount == 2`. The existing cluster must normalize to `update` with a `sys_id`; the new cluster must normalize to `create` with zero matches and no `sys_id`.
