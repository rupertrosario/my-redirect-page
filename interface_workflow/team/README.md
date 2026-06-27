# Team incident workflow

This folder implements one active ServiceNow incident per affected Cohesity cluster. Use the numbered files in order and never branch on raw ServiceNow `records`, `result`, or `body.result` fields.

Expected Dynatrace step IDs are `validate_interfaces`, `for_each_team_iteration`, `search_team_incident`, and `normalize_team_search_result`. If deployed IDs differ, update the corresponding string in the JavaScript action.
