# Sample Output - Active Physical Inventory

## GridView / CSV columns

| Column | Meaning |
|---|---|
| Cluster | Cohesity cluster name |
| Environment | kPhysical |
| PGName | Protection group name |
| PolicyName | Assigned policy |
| ProtectionType | Physical protection type |
| PGObjectCount | Number of protected objects in the PG |
| ServerName | Protected physical server/object |
| ObjectSelection | Object-level selected paths/volumes |
| ObjectExcludePaths | Object-level exclude paths |
| GlobalExcludePaths | PG/global exclude paths |
| DirectiveFile | Directive file path if present |
| IsActive | Active flag |
| IsPaused | Paused flag |
| LastRunStatus | Last run status |
| LastRunEndET | Last run end time in ET |

## Example

```text
Cluster   PGName           PolicyName  ProtectionType  PGObjectCount  ServerName            ObjectSelection  ObjectExcludePaths  GlobalExcludePaths      IsActive  IsPaused  LastRunStatus  LastRunEndET
DTSK-CL01 PHY_WIN_PROD_01  Gold-35D    kFileVolume     2              server01.domain.com   C:\; D:\App     C:\Temp; *.log      C:\Windows\Temp       true      false     Succeeded      2026-07-02 22:15:43
```
