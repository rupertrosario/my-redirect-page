# Cohesity Fleet Dashboard

Complete collector and clickable dashboard for the current mock-up. It collects every cluster available through Helios and shows fleet KPIs, a cluster table, and a selected-cluster detail panel.

## Authentication

Uses the same working method as `inventory/Get-CohesityProtectionInventory.ps1`:

- AES-encrypted API key loaded through `ApiKeyAesHelper.ps1`
- Helios `apiKey` header
- Cluster-scoped `accessClusterId` header

No credential is stored in this folder.

## One-time setup

```powershell
Copy-Item .\config.example.psd1 .\config.psd1
notepad .\config.psd1
```

Set `ApiKeyHelperPath`, `EncryptedApiKeyPath`, target version, and endpoint paths if your tenant differs.

## Run the complete solution

From Windows PowerShell or PowerShell 7:

```powershell
Set-Location .\cohesity-dashboard-collector
.\Run-CohesityDashboard.ps1
```

The launcher collects all clusters into `output/dashboard.json`, starts a local web server, and opens the clickable dashboard at `http://localhost:8765/`. Press Ctrl+C in PowerShell to stop it.

To generate JSON only:

```powershell
.\Collect-CohesityDashboard.ps1
```

## Output

The page shows total/healthy/warning clusters, critical alerts, cluster name, location, version baseline, health, and capacity. Clicking a cluster shows capacity, protected sources, 7-day backup success, active policies, and open alerts.

`sample/dashboard.sample.json` shows the expected schema. The SNOW button currently copies a safe incident payload; connect it to the approved ServiceNow API or Dynatrace webhook only after the endpoint and authentication method are confirmed.

## Validation

Run once and compare one selected cluster's seven displayed values with Helios. If a value is blank, adjust only the response aliases in `modules/ConvertTo-DashboardModel.ps1` or the endpoint in `config.psd1`.

Do not commit `config.psd1`, output JSON, or encrypted key material.
