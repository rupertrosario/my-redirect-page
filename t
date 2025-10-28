# Assuming $objs holds what you showed (from $run.objects)
# Example:
# $objs = @(
#     @{ id = 1886; name = "hostname"; objectType = "kPhysical" },
#     @{ id = 1889; name = "db"; objectType = "kDatabase"; sourceId = 1886 }
# )

$results = @()

# Build a lookup: Physical host ID → Host name
$hostMap = @{}
foreach ($item in $objs) {
    if ($item.objectType -eq 'kPhysical') {
        $hostMap[$item.id] = $item.name
    }
}

# Map DB → Host using sourceId
foreach ($item in $objs) {
    if ($item.objectType -eq 'kDatabase') {
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

# Display
$results | Format-Table -AutoSize
