# assume $objs = $run.objects

# collect all physical hosts first
$hosts = $objs | Where-Object { $_.environment -eq 'kPhysical' }

# collect all databases
$dbs   = $objs | Where-Object { $_.environment -eq 'kDatabase' }

# prepare results
$results = @()

foreach ($db in $dbs) {
    # find the physical host this DB belongs to (based on sourceId)
    $host = $hosts | Where-Object { $_.id -eq $db.sourceId } | Select-Object -First 1

    if ($host) {
        $results += [pscustomobject]@{
            HostName     = $host.name
            DatabaseName = $db.name
        }
    }
}

# Output the final result
$results
