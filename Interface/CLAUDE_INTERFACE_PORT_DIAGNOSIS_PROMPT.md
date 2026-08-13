# Claude Code Prompt — Cohesity Interface / Switch Port Diagnosis

Run:

`/cohesity-interface-port-diagnosis device=<DEVICE_NAME> switch=<SWITCH_NAME> port=<SWITCH_INTERFACE>`

Use the existing repository interface logic and diagnose only the supplied device, switch, and port.

Mandatory rules:
- HTTP/API requests are GET only.
- Never use POST, PUT, PATCH, DELETE, or any configuration-changing operation.
- Do not change interface state, speed, duplex, auto-negotiation, bonding, VLAN, LACP, SFP/transceiver, or switch configuration.
- Use actual returned data only; never infer missing values.
- If a requested value is not available through GET-only data, report `UNKNOWN`.

Return one concise table containing:
- Cohesity device/node and node IP
- bond and physical/slave NIC
- link state
- Cohesity NIC/slave speed
- attached switch and exact port
- switch configured speed
- switch operational/negotiated speed
- SFP/transceiver type and speed/capability
- auto-negotiation enabled/disabled/unknown
- duplex if available
- speed match: MATCH / MISMATCH / UNKNOWN

Finish with one verdict sentence only. Do not remediate.
