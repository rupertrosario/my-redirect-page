function ConvertTo-DashboardModel {
    [CmdletBinding()]
    param([hashtable]$Raw, [hashtable]$Config)

    $clusters = Get-Collection $Raw.Clusters @('cohesityClusters', 'clusters', 'clusterInfos', 'items', 'data')
    $alerts = Get-Collection $Raw.Alerts @('alerts', 'items', 'data')
    $groups = Get-Collection $Raw.ProtectionGroups @('protectionGroups', 'items', 'data')
    $sources = Get-Collection $Raw.Sources @('sources', 'items', 'data')
    $runs = Get-Collection $Raw.Runs @('runs', 'items', 'data')

    $clusterRows = foreach ($cluster in $clusters) {
        $id = [string](Get-PropertyValue $cluster @('clusterId', 'id'))
        $name = [string](Get-PropertyValue $cluster @('clusterName', 'displayName', 'name') 'Unknown')
        $clusterAlerts = @($alerts | Where-Object { [string](Get-PropertyValue $_ @('clusterId', 'cluster_id')) -eq $id })
        $clusterGroups = @($groups | Where-Object { [string](Get-PropertyValue $_ @('clusterId', 'cluster_id')) -eq $id })
        $clusterSources = @($sources | Where-Object { [string](Get-PropertyValue $_ @('clusterId', 'cluster_id')) -eq $id })
        $clusterRuns = @($runs | Where-Object { [string](Get-PropertyValue $_ @('clusterId', 'cluster_id')) -eq $id })
        $successfulRuns = @($clusterRuns | Where-Object { [string](Get-PropertyValue $_ @('status', 'runStatus')) -match 'success|succeed' }).Count
        $successRate = if ($clusterRuns.Count) { [math]::Round(100 * $successfulRuns / $clusterRuns.Count, 1) } else { $null }
        $used = [double](Get-PropertyValue $cluster @('usedCapacityBytes', 'usedBytes') 0)
        $total = [double](Get-PropertyValue $cluster @('totalCapacityBytes', 'capacityBytes') 0)
        $health = [string](Get-PropertyValue $cluster @('healthStatus', 'health', 'status') 'Unknown')
        $version = [string](Get-PropertyValue $cluster @('version', 'clusterVersion') 'Unknown')

        [ordered]@{
            id = $id; name = $name
            location = [string](Get-PropertyValue $cluster @('location', 'regionName', 'siteName') '')
            version = $version; versionStatus = Get-VersionStatus $version $Config.TargetVersion
            health = $health
            capacity = [ordered]@{ usedBytes = $used; totalBytes = $total; usedPercent = if ($total) { [math]::Round(100 * $used / $total, 1) } else { $null } }
            protectedSources = $clusterSources.Count
            backupSuccess7dPercent = $successRate
            activePolicies = @($clusterGroups | Where-Object { (Get-PropertyValue $_ @('isPaused', 'paused') $false) -ne $true }).Count
            openAlerts = $clusterAlerts.Count
        }
    }

    $critical = @($alerts | Where-Object { [string](Get-PropertyValue $_ @('severity', 'alertSeverity')) -match 'critical' }).Count
    [ordered]@{
        generatedAtUtc = [datetime]::UtcNow.ToString('o')
        lookbackDays = $Config.LookbackDays
        summary = [ordered]@{
            totalClusters = @($clusterRows).Count
            healthyClusters = @($clusterRows | Where-Object { $_.health -match 'healthy|good|green' }).Count
            warningClusters = @($clusterRows | Where-Object { $_.health -match 'warn|yellow|degrad' }).Count
            criticalAlerts = $critical
        }
        clusters = @($clusterRows)
    }
}
