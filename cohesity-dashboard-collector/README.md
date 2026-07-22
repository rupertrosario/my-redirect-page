# Cohesity dashboard collector

This package creates the JSON needed for the current mock-up: KPI cards, cluster list, and the selected-cluster overview.

## Authentication method

The collector uses the same method as `inventory/Get-CohesityProtectionInventory.ps1`:

- AES-encrypted API key loaded by `ApiKeyAesHelper.ps1`
- Helios `apiKey` request header
- `accessClusterId` header for cluster-scoped API calls

No username, password, Bearer token, or API key is stored in this folder.

## Run

1. Copy `config.example.psd1` to `config.psd1`.
2. Confirm `ApiKeyHelperPath` and `EncryptedApiKeyPath`.
3. Run:

```powershell
./Collect-CohesityDashboard.ps1
```

Output: `output/dashboard.json`.

## Validation

Run against two clusters first and compare the generated capacity, protected-source, protection-group, run-success, and alert counts with Helios. Tenant response mappings remain isolated in `ConvertTo-DashboardModel.ps1`.

For offline mapping, place `Clusters.json`, `Alerts.json`, `ProtectionGroups.json`, `Sources.json`, and `Runs.json` in a folder and run:

```powershell
./Collect-CohesityDashboard.ps1 -FixtureDirectory ./fixtures
```

Do not commit `config.psd1` or encrypted key material.
