# assume $objs = $run.objects

$hosts = $objs | Where-Object { $_.environment -eq 'kPhysical' }
$dbs   = $objs | Where-Object { $_.environment -eq 'kDatabase' }

foreach ($db in $dbs) {
    $host = $hosts | Where-Object { $_.id -eq $db.sourceId } | Select-Object -First 1
    [pscustomobject]@{
        HostName     = if ($host) { $host.name } else { '(unknown host)' }
        DatabaseName = $db.name
    }
} | Format-Table -AutoSize
