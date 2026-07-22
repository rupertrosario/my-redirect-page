function Get-HeliosData {
    [CmdletBinding()]
    param([hashtable]$Config, [hashtable]$Headers, [string]$FixtureDirectory)

    if ($FixtureDirectory) {
        $fixtureResult = @{}
        foreach ($name in $Config.Endpoints.Keys) {
            $fixture = Join-Path $FixtureDirectory "$name.json"
            $fixtureResult[$name] = if (Test-Path $fixture) {
                Get-Content $fixture -Raw | ConvertFrom-Json
            } else { $null }
        }
        return $fixtureResult
    }

    $result = @{}
    $result.Clusters = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints.Clusters -Headers (New-HeliosHeaders -BaseHeaders $Headers) -VerifyTls $Config.VerifyTls
    $clusters = Get-Collection $result.Clusters @('cohesityClusters', 'clusters', 'clusterInfos', 'items', 'data')
    if (-not $clusters.Count) { throw 'No clusters returned from Helios.' }

    $responseCollections = @{
        Alerts = @('alerts', 'items', 'data')
        ProtectionGroups = @('protectionGroups', 'items', 'data')
        Sources = @('sources', 'items', 'data')
        Runs = @('runs', 'items', 'data')
    }

    foreach ($name in @('Alerts', 'ProtectionGroups', 'Sources', 'Runs')) {
        $collected = @()
        foreach ($cluster in $clusters) {
            $clusterId = [string](Get-PropertyValue $cluster @('clusterId', 'id'))
            if ([string]::IsNullOrWhiteSpace($clusterId)) { continue }
            try {
                $clusterHeaders = New-HeliosHeaders -BaseHeaders $Headers -ClusterId $clusterId
                $response = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints[$name] -Headers $clusterHeaders -VerifyTls $Config.VerifyTls
                foreach ($item in @(Get-Collection $response $responseCollections[$name])) {
                    if ($null -ne $item -and $null -eq $item.PSObject.Properties['clusterId']) {
                        $item | Add-Member -NotePropertyName clusterId -NotePropertyValue $clusterId
                    }
                    $collected += $item
                }
            }
            catch {
                Write-Warning "Collection failed for $name on cluster $clusterId : $($_.Exception.Message)"
            }
        }
        $result[$name] = @($collected)
    }
    return $result
}
