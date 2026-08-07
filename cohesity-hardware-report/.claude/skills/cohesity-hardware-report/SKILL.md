---
name: cohesity-hardware-report
description: Use when the user asks about Cohesity/Helios cluster hardware — nodes, chassis, racks, disks, IPMI/BMC config, or a "cluster hardware report/table". Talks to the cohesity-hardware MCP server (GET-only, never modifies cluster state).
---

# Cohesity Cluster Hardware Report

Orchestration layer over the `cohesity-hardware` MCP server
(`mcp-servers/cohesity-hardware/`). The server does the GET calls; this
skill decides *which* tools to call and *how much* to pull into the main
conversation.

This is module 1 of a planned Cohesity ops toolkit — later modules
(backup-failure triage, compliance/SSO audits, etc.) should follow the same
pattern: a narrow-tool MCP server + a thin skill like this one, not a
monolithic script.

## Setup check (do this first, silently)
The MCP tools (`get_cluster_summary`, `get_nodes`, `get_chassis`,
`get_racks`, `get_disks`, `get_node_hardware_info`, `get_ipmi_lan_info`,
`get_full_hardware_report`) should already be registered via the project's
`.mcp.json`. If calling any of them errors with "not found"/unavailable
rather than an `{"ok": false, ...}` envelope, the server likely isn't
running — tell the user to check `mcp-servers/cohesity-hardware/README.md`
(needs `pip install mcp requests` and a filled-in `.env`), don't try to
work around it by writing ad hoc HTTP calls.

## Tool selection — stay narrow
Match the tool to the actual question. Do not call
`get_full_hardware_report` (bundles all 7 endpoints) unless the user
explicitly wants the complete report — a question about disks only needs
`get_disks`, a question about one node only needs `get_nodes(ids=[...])`.
This is the main lever for keeping context usage low.

## Reading tool results honestly
Every tool returns `{"ok": true, "data": ...}` or `{"ok": false, "error":
"...", "status_code": ...}`.
- `ok: false` → report the `error` message plainly (it already distinguishes
  auth failure / not-found / transient-after-retries). Don't retry it
  yourself or guess a fix — the client already retried transient errors
  before giving up.
- Within `data`, preserve the real JSON semantics when you render a table:
  a field present with value `null` should be shown as `null`; a field the
  API didn't return at all should be shown as such (e.g. "not returned")
  rather than silently blank. Don't collapse the two — that's exactly the
  kind of quiet data-loss the user has asked to avoid. Since tool output is
  live JSON, this distinction is just "what keys does this object have",
  not something you need to guess.
- Never invent a field that isn't in the returned JSON. If the user wants a
  column you don't see in the data, say it wasn't in the response instead
  of filling it in.

## Rendering
When the user wants a table, render standard GitHub-flavored Markdown
tables. For byte-count fields (memory, capacity), show both the raw number
and a human-readable size, e.g. `68719476736000 (62.50 TB)`.

## Context management — when to fork
If a full-report request is going to be large — as a rule of thumb, more
than ~25 nodes or ~100 disks, or the combined tool output is clearly going
to run into many thousands of tokens — don't inline the whole thing into
the main conversation. Instead:
1. Launch a subagent (general-purpose) to call the needed MCP tools,
   assemble the full Markdown report, and write it to a file
   (e.g. `cohesity_hardware_report.md`).
2. Have it report back a short summary (row counts per section, any `ok:
   false` failures, the file path) — not the full tables.
3. Relay that summary to the user and point them at the file, offering to
   pull out any specific section on request.

For small/targeted questions (one node, one disk, the cluster summary),
just call the tool directly and answer inline — no need to fork anything.

## Hard constraint
Every call this skill makes is read-only (GET). Never suggest or attempt a
write against the cluster (no chassis/rack renames, no node
power/removal/upgrade actions, no IPMI updates) even if the user asks in
passing — that's out of scope for this skill; say so and stop.
