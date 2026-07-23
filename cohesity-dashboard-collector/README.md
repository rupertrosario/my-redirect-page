# Cohesity Protection Fleet Dashboard

Local, GET-only Cohesity Helios dashboard for multi-cluster protection inventory and operational exceptions. It uses the repository's existing AES-encrypted API-key method and requires no module, web server, Node.js, Python, database, or package installation.

## What the dashboard shows

- Fleet totals: clusters, protected inventory, open alerts, capacity used, and GC reclaimable TB.
- One row per cluster with Hyper-V, Nutanix, NAS, Oracle, SQL, and Physical inventory. Counts are unique objects found in object-level run details, not PG-level estimates.
- Each workload cell shows `Total · Successful · Failed · Cancelled` object counts.
- Active and paused Protection Groups.
- Object-level unresolved failures. SQL and Oracle are evaluated independently by Full, Incremental, and Log run type.
- Hardware alerts below failures with severity, component, node, code, occurrence, state, and description.
- `All Clusters` by default; select a cluster or click a workload badge to drill down.
- Refresh button with Running, Completed, CompletedWithWarnings, or Failed status.

There is no ServiceNow/SNOW integration in this solution.

## Requirements

- Windows PowerShell 5.1 or PowerShell 7 already present on the machine.
- Network access to `https://helios.cohesity.com`.
- Existing `ApiKeyAesHelper.ps1` and encrypted Cohesity API-key file.
- Helios API key with read access to the listed clusters, protection groups/runs, capacity statistics, and alerts.

No installation command is required.

## Folder layout

```text
cohesity-dashboard-collector/
├── Collect-CohesityDashboard.ps1       # complete GET-only collection
├── Run-CohesityDashboard.ps1           # local server + refresh endpoint
├── config.example.psd1                 # safe example configuration
├── index.html                          # dashboard UI (no external libraries)
├── modules/
│   ├── Common.ps1                      # response/status/object helpers
│   ├── Get-HeliosSession.ps1           # AES key + apiKey headers
│   ├── Get-HeliosData.ps1              # bounded parallel cluster workers
│   ├── Get-ClusterSnapshot.ps1         # inventory/capacity/GC/failures
│   └── ConvertTo-DashboardModel.ps1     # alerts, totals, stale-data merge
├── output/                              # generated locally; Git ignored
```

## Local location and first run

The existing parent folder is:

```text
X:\PowerShell\Cohesity_API_Scripts\
```

Copy the complete `cohesity-dashboard-collector` folder beneath that parent. The resulting local layout must be:

```text
X:\PowerShell\Cohesity_API_Scripts\
└── cohesity-dashboard-collector\
    ├── Run-CohesityDashboard.ps1
    ├── Collect-CohesityDashboard.ps1
    ├── config.example.psd1
    ├── index.html
    └── modules\
```

Therefore, the launcher's full path is:

```text
X:\PowerShell\Cohesity_API_Scripts\cohesity-dashboard-collector\Run-CohesityDashboard.ps1
```

Keep every included file and the `modules` folder together; nothing is installed separately. On the first run, the launcher automatically creates the local `config.psd1` from the included template. Its defaults already point to:

```powershell
ApiKeyHelperPath    = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
TargetVersion       = $null
```

`TargetVersion = $null` means actual cluster versions are displayed without applying an unapproved baseline. `config.psd1`, generated output, and encrypted key material remain local and are excluded from Git.

## Run the complete solution

Run this one command:

```powershell
& 'X:\PowerShell\Cohesity_API_Scripts\cohesity-dashboard-collector\Run-CohesityDashboard.ps1'
```

The first refresh completes, the local dashboard service starts, and the dashboard opens automatically in the default browser. You do not need to type `http://localhost:8765/`. Keep the PowerShell window open and press `Ctrl+C` to stop the dashboard.

Useful alternatives:

```powershell
# Use existing JSON immediately, without refreshing at startup
.\Run-CohesityDashboard.ps1 -SkipInitialRefresh

# Different local port
.\Run-CohesityDashboard.ps1 -Port 8877

# Collect JSON only; do not open/serve the dashboard
.\Collect-CohesityDashboard.ps1
```

## Refresh design and performance

The Refresh Data button calls only the local `POST /api/refresh` endpoint. The local server starts the collector in a background PowerShell process, prevents duplicate refreshes, and remains responsive while the browser polls `GET /api/status`.

Cluster collection uses a built-in runspace pool. `MaxConcurrency = 6` means up to six clusters are collected in parallel without installing `ThreadJob` or any other module. Increase gradually only if Helios does not return throttling/timeouts:

```powershell
MaxConcurrency = 6       # recommended starting value
RequestTimeoutSec = 90
FailureRunsPerPG = 6
```

To keep refresh time down:

1. Cluster capacity, GC, and six workload inventories run inside each parallel cluster worker.
2. Open alerts are queried separately for each cluster with `/v2/alerts` and that cluster's `accessClusterId`; the MCM fleet-alert endpoint is not used.
3. Object-detail runs are requested for every PG because all six inventory totals must be object-level.
4. SQL and Oracle Full, Incremental, and Log streams are evaluated independently; a Log success cannot clear a Full or Incremental failure.
5. Policy detail calls are not made; the dashboard needs PG state, not policy names.

## Failure counting rules

- Output is object-level: VM, NAS object/share, database, or physical server.
- Runs are processed newest first and keyed by `object ID + run type`.
- A failure/cancellation is excluded when a newer success exists for the same object and run type.
- SQL/Oracle Log success does not clear a Full or Incremental failure.
- `SucceededWithWarning` triggers object-detail inspection.
- If Cohesity marks a PG failed but returns no object details, the dashboard shows a collection warning; it does not invent a PG-level failure or replace the object count with zero silently.
- Successful object count is `protected total - unresolved failed objects - unresolved cancelled objects`.

## Cluster-gone and API error handling

Every Cohesity operation is GET-only. A 403, 404, timeout, removed cluster, or invalid response does not stop other cluster workers.

- When one cluster is unreachable, its last successful values remain visible and are marked `STALE` / `Unavailable`.
- When a cluster disappears from the Helios cluster-list response, it is shown as `Cluster Gone` with its prior values.
- Missing data is never silently replaced with zero when prior data exists.
- A refresh with partial errors ends as `CompletedWithWarnings`.
- Removal of a stale/inactive cluster from the dashboard is deliberately manual; successful collection never deletes history implicitly.
- JSON files are written to a temporary file and atomically moved into place, so the browser never reads a half-written refresh.

## Generated files

| File | Purpose |
|---|---|
| `output/dashboard.json` | Full UI data contract |
| `output/refresh-status.json` | Current/last refresh state and duration |
| `output/claude-context.json` | Compact, credential-free future Claude Code context |

Claude Code should read `claude-context.json`, not call Helios directly. That keeps prompts smaller, avoids credentials, and lets questions such as “which cluster needs attention and why?” use the same verified snapshot as the dashboard.

## Helios GET endpoints

| Data | Endpoint/pattern |
|---|---|
| Cluster list | `/v2/mcm/cluster-mgmt/info` |
| Inventory and PG state | `/v2/data-protect/protection-groups?environments=...` |
| Object run details | `/v2/data-protect/protection-groups/{id}/runs` |
| Capacity | `/irisservices/api/v1/public/stats/storage` |
| GC reclaimable | `timeSeriesStats` / `ApolloV2ClusterStats` / `EstimatedGarbageBytes` |
| Open and hardware alerts | `/v2/alerts` per cluster with `accessClusterId` |

All cluster-scoped calls send both `apiKey` and `accessClusterId`, matching `inventory/Get-CohesityProtectionInventory.ps1`.

## Fast validation after the first run

Validate one normal cluster and one cluster with known exceptions:

1. Compare the six protected-object totals and active/paused PG totals with Helios.
2. Confirm one SQL or Oracle Full/Incremental/Log failure has no newer success in the same stream.
3. Compare capacity and `EstimatedGarbageBytes` (displayed as decimal TB) with the existing GC script.
4. Compare open Hardware alert count/detail with the existing alerts workflow.
5. Temporarily use an invalid `accessClusterId` in a fixture/test and confirm other clusters complete while prior values are marked stale.

## Troubleshooting

| Symptom | Check |
|---|---|
| `Missing API key helper` | Correct `ApiKeyHelperPath` in local `config.psd1` |
| `No clusters returned` | API-key read permission and `/v2/mcm/cluster-mgmt/info` response |
| Workload count is zero unexpectedly | Open **Collection Warnings** and verify the PG `/runs?includeObjectDetails=true` response contains the expected `kVirtualMachine`, `kHost`, or `kDatabase` objects |
| Refresh is slow | `refresh-status.json`, timeouts, then increase `MaxConcurrency` from 6 to 8 only after checking for throttling |
| GC blank | Confirm the cluster entity-name format used by the existing GC script |
| Browser loads but JSON fails | Start with `Run-CohesityDashboard.ps1`; do not open `index.html` directly from disk |
| Port already in use | Run with `-Port 8877` |

## Planned extension pattern

AD, SSO, DNS/NTP, interfaces, and certificates should be added later as independent GET-only collector modules and collapsible dashboard sections. Do not add them to the base refresh path until each module has its own timeout, error result, and compact JSON contract.
