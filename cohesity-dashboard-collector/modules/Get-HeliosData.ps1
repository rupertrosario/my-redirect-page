function Get-HeliosData {
    [CmdletBinding()]
    param([hashtable]$Config, [hashtable]$Headers, [string]$FixtureDirectory)

    if ($FixtureDirectory) {
        $fixture = Join-Path $FixtureDirectory 'raw.json'
        if (-not (Test-Path $fixture)) { throw "Fixture not found: $fixture" }
        return (Get-Content $fixture -Raw | ConvertFrom-Json)
    }

    $clusterResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints.Clusters -Headers $Headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
    $clusters = @(Get-Collection $clusterResponse @('cohesityClusters','clusters','clusterInfos','items','data') | Where-Object { Get-PropertyValue $_ @('clusterId','id') '' })
    if (-not $clusters.Count) { throw 'No clusters returned from Helios.' }

    $commonPath = Join-Path $PSScriptRoot 'Common.ps1'
    $snapshotPath = Join-Path $PSScriptRoot 'Get-ClusterSnapshot.ps1'
    $pool = [runspacefactory]::CreateRunspacePool(1, [math]::Max(1,[int]$Config.MaxConcurrency))
    $pool.Open()
    $jobs = @()
    $worker = {
        param($CommonPath,$SnapshotPath,$Cluster,$Config,$Headers)
        . $CommonPath
        . $SnapshotPath
        Get-ClusterSnapshot -Cluster $Cluster -Config $Config -BaseHeaders $Headers
    }
    try {
        foreach ($cluster in $clusters) {
            $ps = [powershell]::Create()
            $ps.RunspacePool = $pool
            [void]$ps.AddScript($worker.ToString()).AddArgument($commonPath).AddArgument($snapshotPath).AddArgument($cluster).AddArgument($Config).AddArgument($Headers)
            $jobs += [pscustomobject]@{ PowerShell=$ps; Handle=$ps.BeginInvoke(); Cluster=$cluster }
        }
        $snapshots = foreach ($job in $jobs) {
            try {
                $result = @($job.PowerShell.EndInvoke($job.Handle))
                if ($result.Count) { $result[-1] }
                else { throw 'Cluster worker returned no result.' }
            } catch {
                $id = [string](Get-PropertyValue $job.Cluster @('clusterId','id') '')
                $name = [string](Get-PropertyValue $job.Cluster @('clusterName','name','displayName') ("Unknown-$id"))
                [ordered]@{ id=$id; name=$name; health='Unavailable'; availability='Unavailable'; stale=$false; missedRuns=1; lastSuccessfulCollectionUtc=''; collectionErrors=@($_.Exception.Message) }
            } finally { $job.PowerShell.Dispose() }
        }
    } finally { $pool.Close(); $pool.Dispose() }

    $alerts = @()
    $alertError = ''
    try {
        $alertResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints.Alerts -Headers $Headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
        $alerts = @(Get-Collection $alertResponse @('alerts','alertsList','items','data'))
    } catch { $alertError = $_.Exception.Message }

    [ordered]@{ clusters=$clusters; snapshots=@($snapshots); alerts=$alerts; alertError=$alertError }
}
