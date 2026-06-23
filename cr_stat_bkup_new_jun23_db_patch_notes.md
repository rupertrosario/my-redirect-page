# Backup validation DB visibility patch notes

The current script is missing some DB rows because not every SQL DB is returned as `objects[]` or `childObjects[]`.

Required correction:

1. Keep FS detection from the parent/root object:
   - `objects[].latestSnapshotsInfo`

2. Keep DB detection from:
   - `objects[].name` when the value is `SERVER/DBNAME`
   - `objects[].childObjects[]`
   - `objects[].mssqlParams`
   - `objects[].oracleParams`
   - `objects[].databaseParams`
   - `objects[].dbParams`

3. When a DB name is found only inside params, output it as:
   - `BackupType = SQL` or `Oracle`
   - `ObjectName = DBNAME`
   - `SourceName = SERVER`
   - `SyntheticReason = DBNameFromParams`
   - Use the parent object's `latestSnapshotsInfo` and protection group.

4. Fallback stop rule:
   - If valid FS and valid DB are both found, do not check other clusters.
   - If FS is missing or DB is missing, run protected-object fallback across all clusters.

5. Replication handling:
   - Do not use replication as a primary search rule.
   - Only show `ReplicatedHint` as a note when `replicationSnapshotInfo.clusterId` exists.

Debug validation columns to check:

```text
SearchTerm
SearchMode
SearchCluster
RawObjectName
ParentName
SourceName
Environment
ObjectType
Depth
SyntheticReason
BackupType
ObjectName
ProtectionGroup
LastBackupTime
```
