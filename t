# Assume $objs = $run.objects

$hosts = $objs | Where-Object { $_.environment -eq 'kPhysical' }
$dbs   = $objs | Where-Object { $_.environment -eq 'kDatabase' }

$results = @()

foreach ($db in $dbs) {
    $host = $hosts | Where-Object { $_.id -eq $db.sourceId } | Select-Object -First 1
    $results += [pscustomobject]@{
        HostName     = if ($host) { $host.name } else { '(unknown host)' }
        DatabaseName = $db.name
    }
}

# Output the final results
$results
