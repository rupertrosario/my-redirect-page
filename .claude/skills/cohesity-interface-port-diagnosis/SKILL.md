---
name: cohesity-interface-port-diagnosis
description: Read-only Cohesity interface diagnosis using the repository's proven interface collector.
disable-model-invocation: true
---

# Cohesity Interface Port Diagnosis

## Non-negotiable implementation rule

Do NOT redesign this workflow when modifying it.

Always start from and reuse the working collection logic in:
- `Cohesity_Interface_Health_Stats.ps1`
- `Interface/02_validate_interfaces.js`
- `Hardware/Cohesity_Rack_Resiliency_Assessment.ps1` for AES/master-password API-key handling

If additional information is required, add fields already present in the returned Cohesity endpoint payload. Do not replace a working endpoint, payload hierarchy, parser, or credential flow with a different implementation unless an actual GET response proves the existing implementation is wrong.

## Safety

- STRICTLY READ-ONLY.
- Every Cohesity HTTP request must be GET.
- Never use POST, PUT, PATCH, DELETE, or configuration-changing operations.
- Never remediate.

## Local path convention

For PowerShell defaults in this project, use the explicit local X: drive paths rather than `$PSScriptRoot` unless the user explicitly asks for portable paths.

Use:

```powershell
param(
    [string]$TargetsFile = 'X:\PowerShell\Cohesity_Automations\Interface\Interface_Diagnosis_Targets.txt',
    [string]$HistoryDir  = 'X:\PowerShell\Data\Cohesity\InterfaceDiagnosis'
)
```

Do not omit the `\Interface\` directory from the target-file path.

## Input

Use:

`X:\PowerShell\Cohesity_Automations\Interface\Interface_Diagnosis_Targets.txt`

One target per line:

`<SWITCH> <INTERFACE>`

Example:

`switch01 WEC11`

No Cohesity node/IP is required as input.

## Proven collection logic

Reuse the same request as the existing interface health collector:

`GET https://helios.cohesity.com/irisservices/api/v1/public/interface`

with:
- `bondInterfaceOnly=true`
- `ifaceGroupAssignedOnly=true`
- `includeUplinkSwitchInfo=true`
- `includeBondSlaveDetails=true`
- `includeStats=true`

Reuse the returned hierarchy:

`node -> interfaces[] -> bondSlavesDetails[] -> uplinkSwitchInfo[]`

Existing fields used by the working scripts include:
- node: `nodeId`, `nodeIp`, `chassisSerial`
- interface: `name`, `mtu`, `bondingMode`, `activeBondSlave`, `stats`
- bond slave: `name`, `linkState`, `speed`, `macAddr`, `slotType/slot`
- uplink: `sysName`, `portId`
- stats: RX/TX packets, bytes, errors and drops

Use `iface.stats` exactly as the existing health script does unless a real response demonstrates a different scope is required.

## Lookup logic

1. Collect the complete interface dataset first using the proven collector.
2. Extend each bond-slave row with `uplinkSwitchInfo.sysName` and `uplinkSwitchInfo.portId`.
3. Search the collected rows using the TXT target.
4. Switch matching must understand short hostname vs FQDN:
   - `switch01` matches `switch01.example.company.com`
   - matching is case-insensitive
5. Match the requested interface against returned `portId` exactly after normalization.
6. If the switch exists but the interface does not, show the actual `portId` values returned for that switch. Do not replace the whole row with `UNKNOWN`.
7. Preserve the actual FQDN returned by Cohesity in output.

## Authentication

PowerShell scripts must reuse the existing encrypted key flow:
- helper: `X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1`
- encrypted key: `X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc`
- `Get-CohesityApiKeyFromAes`

Do not fall back to plaintext `apikey.txt` unless the user explicitly asks.

## Output

Return concise NOC-focused data:
- requested switch/interface
- actual switch FQDN and portId
- cluster
- node identity/IP when returned
- bond and bond slave
- link state
- slave speed
- MTU
- RX errors/drops
- TX errors/drops
- clear status

Current counters are cumulative. Historical day/week analysis requires stored snapshots; do not claim the current GET alone contains past-day history.
