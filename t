# Assuming you already have $runs from:
# $runs = ($response.Content | ConvertFrom-Json).protectionRuns

$databaseObjects = @()

foreach ($run in $runs) {
    if (-not $run.objects) { continue }

    foreach ($obj in $run.objects) {
        # handle nested .object safely
        $objNode = if ($obj.object) { $obj.object } else { $obj }

        # pick only kDatabase
        if ($objNode.objectType -eq "kDatabase") {
            $databaseObjects += [pscustomobject]@{
                ObjectName = $objNode.name
                ObjectType = $objNode.objectType
                ProtectionGroup = $pgName
                Cluster = $cluster_name
            }
        }
    }
}

# display or export
$databaseObjects | Sort-Object ObjectName | Format-Table -AutoSize
