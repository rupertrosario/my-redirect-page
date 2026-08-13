# Claude Prompt — Interface Diagnosis

Use the existing Cohesity interface scripts in this repo and diagnose only this target:

- Device: <DEVICE_NAME>
- Switch: <SWITCH_NAME>
- Port: <PORT>

STRICTLY GET-ONLY / READ-ONLY.
Never use POST, PUT, PATCH, DELETE and never make any configuration change.

Report only:
- physical interface / bond slave
- link state
- Cohesity-reported NIC speed
- attached switch and port
- SFP/transceiver type and supported speed
- configured switch-port speed
- actual/negotiated speed
- auto-negotiation ON/OFF
- duplex if available
- MATCH / MISMATCH and likely issue

If any value cannot be proven from GET-only data, say UNKNOWN. Do not guess. Do not remediate.
