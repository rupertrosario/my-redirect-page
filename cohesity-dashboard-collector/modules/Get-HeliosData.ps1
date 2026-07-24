function Get-HeliosData {
    [CmdletBinding()]
    param([hashtable]$Config, [hashtable]$Headers, [string]$FixtureDirectory)

    if ($FixtureDirectory) {
        $fixture = Join-Path $FixtureDirectory 'raw.json'
        if (-not (Test-Path -LiteralPath $fixture -PathType Leaf)) {
            throw "Fixture not found: $fixture"
        }
        return (Get-Content -LiteralPath $fixture -Raw | ConvertFrom-Json)
    }

    $clusterResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl `
        -Path $Config.Endpoints.Clusters -Headers $Headers `
        -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
    $clusters = @(Get-Collection $clusterResponse @(
        'cohesityClusters','clusters','clusterInfos','items','data'
    ) | Where-Object {
        -not [string]::IsNullOrWhiteSpace(
            [string](Get-PropertyValue $_ @('clusterId','id') '')
        )
    })
    if (@($clusters).Count -eq 0) { throw 'No clusters returned from Helios.' }

    $commonPath = Join-Path $PSScriptRoot 'Common.ps1'
    $snapshotPath = Join-Path $PSScriptRoot 'Get-ClusterSnapshot.ps1'
    $maxConcurrency = [math]::Max(1,[int]$Config.MaxConcurrency)
    $pool = [runspacefactory]::CreateRunspacePool(1,$maxConcurrency)
    $pool.Open()
    $jobs = @()
    $worker = {
        param($CommonPath,$SnapshotPath,$Cluster,$Config,$Headers)
        . $CommonPath
        . $SnapshotPath
        Get-ClusterSnapshot -Cluster $Cluster -Config $Config -BaseHeaders $Headers
    }

    try {
        foreach ($cluster in @($clusters)) {
            $ps = [powershell]::Create()
            $ps.RunspacePool = $pool
            [void]$ps.AddScript($worker.ToString()).AddArgument($commonPath).AddArgument($snapshotPath).AddArgument($cluster).AddArgument($Config).AddArgument($Headers)
            $jobs += [pscustomobject]@{
                PowerShell=$ps
                Handle=$ps.BeginInvoke()
                Cluster=$cluster
            }
        }

        $snapshots = @()
        foreach ($job in @($jobs)) {
            try {
                $result = @($job.PowerShell.EndInvoke($job.Handle))
                if (@($result).Count -eq 0) { throw 'Cluster worker returned no result.' }
                $snapshots += $result[-1]
            } catch {
                $id = [string](Get-PropertyValue $job.Cluster @('clusterId','id') '')
                $name = [string](Get-PropertyValue $job.Cluster @(
                    'clusterName','name','displayName'
                ) ("Unknown-$id"))
                $emptyInventory = [ordered]@{}
                foreach ($environment in @(Get-EnvironmentDefinitions)) {
                    $emptyInventory[$environment.Key] = [ordered]@{
                        label=$environment.Label; total=0; successful=0; failed=0; cancelled=0
                    }
                }
                $snapshots += [ordered]@{
                    id=$id; name=$name; location='Unknown'; version='Unknown'
                    versionStatus='Unknown'; health='Unavailable'; availability='Unavailable'
                    stale=$false; missedRuns=1; lastSuccessfulCollectionUtc=''
                    capacity=[ordered]@{
                        usedBytes=$null; totalBytes=$null; availableBytes=$null; usedPercent=$null
                    }
                    gcReclaimableBytes=$null
                    protectionGroups=[ordered]@{active=0;paused=0;total=0}
                    inventory=$emptyInventory
                    openAlerts=0; hardwareAlerts=@(); failures=@()
                    collectionErrors=@($_.Exception.Message)
                }
            } finally {
                $job.PowerShell.Dispose()
            }
        }
    } finally {
        $pool.Close()
        $pool.Dispose()
    }

    return [ordered]@{
        clusters=@($clusters)
        snapshots=@($snapshots)
    }
}