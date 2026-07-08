# Team incident data contract

`validate_interfaces.downRows[]` is grouped by `ClusterId`. The Team builder returns one candidate per cluster through `teamIncidentCandidates`, `teamIterations`, and `iterations`.

Every candidate includes `Type`, `ClusterName`, `ClusterId`, `CorrelationId`, `ShortDescription`, `Description`, `WorkNotes`, `DownCount`, and `Rows`.

Never branch on raw ServiceNow output. The normalization action returns `action` (`update` or `create`), `matchedCount`, `sys_id`, `existingNumber`, `candidate`, and `searchRows`. Branch only on `action`; update also requires a non-empty `sys_id`.
