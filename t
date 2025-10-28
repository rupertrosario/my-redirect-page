# assume $objs = $run.objects

# Extract physical hosts from inside .object
$hosts = $objs | Where-Object { $_.object.environment -eq 'kPhysical' }

# Extract databases from inside .object
$dbs   = $objs | Where-Object { $_.object.environment -eq 'kDatabase' }

# Build Host–DB mapping
$results = foreach ($db in $dbs) {
    $host = $hosts | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
    if ($host) {
        [pscustomobject]@{
            HostName     = $host.object.name
            DatabaseName = $db.object.name
        }
    }
}

# Show results
$results
