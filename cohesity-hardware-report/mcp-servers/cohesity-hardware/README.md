# cohesity-hardware-mcp

A read-only (**GET-only**) MCP server exposing Cohesity/Helios cluster
hardware inventory as tools: cluster summary, nodes, chassis, racks, disks,
responding-node hardware detail, and IPMI/BMC LAN config.

It is safe by construction: the HTTP client underneath (`client.py`) only
implements `.get()` — there is no write method anywhere in this package, so
none of the tools can issue a PUT/POST/DELETE against a cluster.

## Why this exists instead of a one-off script
- **Reusable** — register it once and any MCP client (Claude Code, Claude
  Desktop, other agents) on your team can use it, each with their own
  credentials.
- **Context-light** — tools are narrow (`get_disks`, `get_nodes`, ...), so a
  question about disks doesn't pull cluster/chassis/rack/IPMI data along
  with it. `get_full_hardware_report` exists for when the complete picture
  is actually wanted.
- **No stale field mapping** — tools pass the live API JSON straight
  through; nothing here renames or reshapes fields, so there's nothing to
  drift out of sync with reality.

## Setup

1. Install dependencies (once, in whatever Python environment your MCP
   client runs the server with):
   ```
   pip install mcp requests
   ```
2. Copy `.env.example` to `.env` in this same folder and fill in your own
   `COHESITY_BASE_URL` / `COHESITY_API_KEY` (and `COHESITY_ACCESS_CLUSTER_ID`
   if you're going through `helios.cohesity.com`). This file is gitignored —
   it never gets committed, and nobody else's credentials are shared through
   it.
3. Register the server with your MCP client.

### Claude Code (this repo)
Already registered for you via the project's `.mcp.json` at the repo root —
open this project in Claude Code and the `cohesity-hardware` tools are
available automatically, as long as step 1–2 above are done locally.

### Claude Desktop (or any other MCP client)
Add this to your client's MCP config (e.g. `claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "cohesity-hardware": {
      "command": "python",
      "args": ["-m", "cohesity_hardware_mcp"],
      "cwd": "<path-to-this-repo>/mcp-servers/cohesity-hardware/src"
    }
  }
}
```

## Tools

| Tool | Endpoint | Purpose |
|---|---|---|
| `get_cluster_summary` | `GET /v2/clusters` | Cluster-level hardware summary |
| `get_nodes` | `GET /v2/clusters/nodes` | Per-node hardware inventory (optional `ids` filter) |
| `get_chassis` | `GET /v2/chassis` | Chassis inventory |
| `get_racks` | `GET /v2/racks` | Rack inventory |
| `get_disks` | `GET /v2/disks/local` | Disk inventory |
| `get_node_hardware_info` | `GET /v2/node/hardware-info` | Extra detail for the *responding* node only (no per-id targeting — a Cohesity API limitation, not this server's) |
| `get_ipmi_lan_info` | `GET /v2/ipmi/get-lan-info` | IPMI/BMC LAN config |
| `get_full_hardware_report` | all of the above | Convenience bundle — use only when the full report is explicitly wanted |

Every tool returns `{"ok": true, "data": ...}` on success or
`{"ok": false, "error": "...", "status_code": ...}` on failure. Transient
errors (429/500/502/503/504, connection resets, timeouts) are retried with
exponential backoff automatically before a failure is ever returned;
auth/permission/not-found errors (401/403/404) are surfaced immediately
without retrying.

## Field honesty
Within `data`, a JSON `null` means the API returned that field as null; a
key that's simply absent means the field wasn't present in the response at
all. These are reported as-is — nothing here is invented to fill a gap.

## Local testing without an MCP client
```
python -m cohesity_hardware_mcp
```
starts the server on stdio. Use the `mcp` CLI's inspector
(`mcp dev src/cohesity_hardware_mcp/server.py`) if you have the `mcp[cli]`
extra installed and want to call tools interactively before wiring up a
real client.
