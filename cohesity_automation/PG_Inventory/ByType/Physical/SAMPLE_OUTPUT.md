# Cohesity ACTIVE Physical PG Inventory - Sample Output

## Scope

- Environment: `kPhysical`
- PG filter: active only: `isActive=true`, `isDeleted=false`
- Cluster selection: `[0] ALL`, single number, comma-separated numbers, or ranges
- Output: console table, `Out-GridView`, and CSV

## Cluster selection examples

```text
[0] ALL
[1] DTSK-CL01
[2] DTSK-CL02
[3] DTSK-CL03

Enter selection: 1,3
```

```text
Enter selection: 2-4
```

## GridView / CSV columns

| Column | Meaning |
|---|---|
| Cluster | Cohesity cluster name |
| Environment | Always `kPhysical` |
| PGName | Protection Group name |
| PolicyName | Policy attached to the PG |
| ProtectionType | Physical protection type from `physicalParams.protectionType` |
| PGObjectCount | Number of protected objects/servers in the PG |
| ServerName | Protected physical server/object name |
| ObjectSelection | Object-level selected paths/volumes found in the PG object payload |
| ObjectExcludePaths | Object-level excluded paths found in the protected object payload |
| GlobalExcludePaths | PG/global exclude paths found under `physicalParams` |
| DirectiveFile | Directive file/path when present |
| IsActive | PG active flag |
| IsPaused | PG paused flag |
| LastRunStatus | Last run status from PG last run info |
| LastRunEndET | Last run end time converted to ET |

## Sample rows

```text
Cluster   PGName           PolicyName  ProtectionType  PGObjectCount  ServerName            ObjectSelection  ObjectExcludePaths  GlobalExcludePaths                DirectiveFile  IsActive  IsPaused  LastRunStatus  LastRunEndET
DTSK-CL01 PHY_WIN_PROD_01  Gold-35D    kFileVolume     2              server01.domain.com   C:\; D:\App     C:\Temp; *.log      C:\Windows\Temp; C:\ProgramData                True      False     Succeeded      2026-07-02 22:15:43
DTSK-CL01 PHY_WIN_PROD_01  Gold-35D    kFileVolume     2              server02.domain.com   E:\Data          E:\Scratch          C:\Windows\Temp; C:\ProgramData                True      False     Succeeded      2026-07-02 22:15:43
```
