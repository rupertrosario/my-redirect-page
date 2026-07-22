# Cohesity dashboard collector

This package creates the JSON needed for the current mock-up: KPI cards, cluster list, and the selected-cluster overview (location, version, health, capacity, protected sources, seven-day backup success, active policies, and alerts).

## Run

1. Copy `config.example.psd1` to `config.psd1`.
2. Set an API key, or set the username and pass `-Password (Read-Host -AsSecureString)`.
3. Run:

```powershell
./Collect-CohesityDashboard.ps1 -Password (Read-Host -AsSecureString)
```

Output: `output/dashboard.json`.

## Important validation

Helios response envelopes and endpoint availability can differ by tenant/release. Run against two clusters, review any endpoint warning, and compare the generated counts with Helios. If a response uses different field names, update only `ConvertTo-DashboardModel.ps1`; API calls remain isolated in `Get-HeliosData.ps1`.

For offline mapping, place `Clusters.json`, `Alerts.json`, `ProtectionGroups.json`, `Sources.json`, and `Runs.json` in a folder and run:

```powershell
./Collect-CohesityDashboard.ps1 -FixtureDirectory ./fixtures
```

Do not commit `config.psd1`; it may contain credentials.
