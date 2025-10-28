# assume $objs is your collection (from $run.objects)

$results = @()

# Step 1: collect all physical hosts (environment = kPhysical)
$hostMap = @{}
foreach ($item in $objs) {
    if ($item.environment -eq 'kPhysical') {
        $hostMap[$item.id] = $item.name
    }
}

# Step 2: collect all databases (environment = kDatabase)
foreach ($item in $objs) {
    if ($item.environment -eq 'kDatabase') {
        $hostName = if ($hostMap.ContainsKey($item.sourceId)) {
            $hostMap[$item.sourceId]
        } else {
            "(unknown host)"
        }

        $results += [pscustomobject]@{
            HostName     = $hostName
            DatabaseName = $item.name
        }
    }
}

# Step 3: show or export
$results | Sort-Object HostName,DatabaseName | Format-Table -AutoSize
