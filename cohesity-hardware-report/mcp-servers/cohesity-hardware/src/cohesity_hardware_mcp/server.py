"""
MCP server exposing read-only Cohesity/Helios cluster hardware inventory as
tools. Every tool ultimately calls CohesityClient.get() — a class that has
no write methods at all — so this server structurally cannot issue a
PUT/POST/DELETE against a cluster.

Each tool returns a uniform envelope:
    {"ok": true,  "data": <raw API JSON, untouched>}
    {"ok": false, "error": "<message>", "status_code": <int or null>}

The raw JSON is passed straight through — this server does not rename or
reshape fields, so whatever key names the live API actually returns are
exactly what the caller sees. No pre-baked/summarized field mapping to go
stale or drift from reality.
"""

from typing import List, Optional

from mcp.server.mcpserver import MCPServer

from .client import CohesityClient, client_from_env

mcp = MCPServer(
    "cohesity-hardware",
    instructions=(
        "Read-only Cohesity/Helios cluster hardware inventory tools. Every "
        "tool issues a single GET request (with automatic retry/backoff on "
        "transient errors) — none of them can modify cluster state.\n\n"
        "Prefer the narrowest tool for the question at hand (e.g. get_disks "
        "for a disk question) instead of get_full_hardware_report, to keep "
        "responses small. Only call get_full_hardware_report when the user "
        "explicitly wants the complete report.\n\n"
        "Every tool returns {\"ok\": true, \"data\": ...} on success or "
        "{\"ok\": false, \"error\": ..., \"status_code\": ...} on failure — "
        "check `ok` before reading `data`. Within `data`, a JSON `null` "
        "means the API returned that field as null; a key that's simply "
        "absent means the field wasn't in the response at all. Report both "
        "honestly rather than treating them as the same thing."
    ),
)

# Built lazily (first tool call) and reused for the life of the server
# process, so repeated calls share one connection pool instead of paying
# TCP/TLS setup every time.
_client: Optional[CohesityClient] = None


def _get_client() -> CohesityClient:
    global _client
    if _client is None:
        _client = client_from_env()
    return _client


@mcp.tool()
def get_cluster_summary() -> dict:
    """Cluster-level hardware summary: id, incarnation id, cluster/environment
    type, chassis count, node count, hardware vendors/models, SED encryption
    status, patch version, disk counts by storage tier. Maps to a single
    GET /v2/clusters call."""
    return _get_client().get("/v2/clusters").to_dict()


@mcp.tool()
def get_nodes(ids: Optional[List[int]] = None,
              include_marked_for_removal: bool = False,
              show_system_disks: bool = True) -> dict:
    """Per-node hardware inventory: hostname, IP, node type, hardware model,
    vendor, serials, CPU, memory, network, chassis/rack linkage, max
    capacity, disk counts by tier, software version. Maps to a single
    GET /v2/clusters/nodes call. Pass `ids` to narrow to specific node IDs
    instead of returning every node."""
    params = {
        "showSystemDisks": show_system_disks,
        "includeMarkedForRemoval": include_marked_for_removal,
    }
    if ids:
        params["ids"] = ids
    return _get_client().get("/v2/clusters/nodes", params=params).to_dict()


@mcp.tool()
def get_chassis() -> dict:
    """Chassis inventory: id, name, serial number, hardware model, rack id,
    and the list of node ids housed in each chassis. Maps to a single
    GET /v2/chassis call."""
    return _get_client().get("/v2/chassis").to_dict()


@mcp.tool()
def get_racks() -> dict:
    """Rack inventory: id, name, physical location, and the list of chassis
    ids installed in each rack. Maps to a single GET /v2/racks call."""
    return _get_client().get("/v2/racks").to_dict()


@mcp.tool()
def get_disks() -> dict:
    """Local disk inventory: id, serial number, model, owning node id,
    capacity, disk type (SSD/HDD/NVMe/etc.), status, physical location,
    encryption status, SSD wear percentage/level. Maps to a single
    GET /v2/disks/local call."""
    return _get_client().get("/v2/disks/local").to_dict()


@mcp.tool()
def get_node_hardware_info() -> dict:
    """Extra hardware detail (chassis/node serials, max slots, slot number,
    IPMI LAN channel, HBA model) for whichever node happens to answer the
    request — this Cohesity endpoint takes no node-id parameter, so it
    cannot be targeted at a specific node. Maps to a single
    GET /v2/node/hardware-info call."""
    return _get_client().get("/v2/node/hardware-info").to_dict()


@mcp.tool()
def get_ipmi_lan_info() -> dict:
    """IPMI/BMC LAN configuration: channel, IP address source, LAN IP,
    subnet mask, default gateway IP/MAC. Maps to a single
    GET /v2/ipmi/get-lan-info call."""
    return _get_client().get("/v2/ipmi/get-lan-info").to_dict()


@mcp.tool()
def get_full_hardware_report() -> dict:
    """Convenience tool: calls every hardware endpoint (cluster summary,
    nodes, chassis, racks, disks, responding-node hardware-info, IPMI LAN
    info) in one shot and returns them together, keyed by section. Use only
    when the user explicitly wants the complete hardware report — for a
    single targeted question, call the specific get_* tool instead, since
    this combined payload can be large for bigger clusters."""
    client = _get_client()
    return {
        "cluster": client.get("/v2/clusters").to_dict(),
        "nodes": client.get("/v2/clusters/nodes", params={"showSystemDisks": True}).to_dict(),
        "chassis": client.get("/v2/chassis").to_dict(),
        "racks": client.get("/v2/racks").to_dict(),
        "disks": client.get("/v2/disks/local").to_dict(),
        "node_hardware_info": client.get("/v2/node/hardware-info").to_dict(),
        "ipmi_lan_info": client.get("/v2/ipmi/get-lan-info").to_dict(),
    }
