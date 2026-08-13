---
name: cohesity-interface-port-diagnosis
description: Read-only Cohesity interface diagnosis for a specified device, switch and port.
disable-model-invocation: true
---

# Interface Port Diagnosis

Invocation:
`/cohesity-interface-port-diagnosis device=<DEVICE> switch=<SWITCH> port=<PORT>`

Rules:
- Read repository files first: `Interface/02_validate_interfaces.js` and `Cohesity_Interface_Health_Stats.ps1`.
- Every HTTP/API request must use GET only.
- Never use POST, PUT, PATCH, DELETE, or any configuration-changing operation.
- Diagnose only the supplied device, switch, and port.
- Use actual returned data; never infer missing values.
- If a value is not available through approved read-only data, return `UNKNOWN`.

Collect and report:
- Cohesity node/device and node IP
- bond and slave interface
- link state
- Cohesity-reported slave/NIC speed
- attached switch and switch port
- configured port speed
- operational/negotiated port speed
- SFP/transceiver type and speed/capability
- auto-negotiation state
- duplex if available

Return one concise table and one final verdict sentence. Do not perform remediation.
