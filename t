# assume $objs = $run.objects

# Step 1: Get all physical hosts (objectType = kPhysical)
$physicalHosts = $objs | Where-Object { $_.object.objectType -eq 'kPhysical' }

# Step 2: Get all databases (objectType = kDatabase)
$databases = $objs | Where-Object { $_.object.objectType -eq 'kDatabase' }

# Step 3: Map each DB to its physical host via sourceId
$results = foreach ($db in $databases) {
    $hostEntry = $physicalHosts | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1

    if ($hostEntry) {
        [pscustomobject]@{
            HostName     = $hostEntry.object.name
            DatabaseName = $db.object.name
        }
    }
}

# Step 4: Display clean results
$results
