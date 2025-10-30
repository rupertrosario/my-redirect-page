# assume $objs = $run.objects

# Step 1: Collect all physical hosts
$physicalHosts = $objs | Where-Object { $_.object.environment -eq 'kPhysical' }

# Step 2: Collect all databases (kDatabase)
$databases = $objs | Where-Object { $_.object.objectType -eq 'kDatabase' }

# Step 3: Initialize an empty array (so we append, not overwrite)
$results = @()

# Step 4: For each DB, find its parent host
foreach ($db in $databases) {
    $hostEntry = $physicalHosts | Where-Object { $_.object.id -eq $db.object.sourceId } | Select-Object -First 1
    if ($hostEntry) {
        # append (+=) not overwrite (=)
        $results += [pscustomobject]@{
            HostName     = $hostEntry.object.name
            DatabaseName = $db.object.name
        }
    }
}

# Step 5: Output all rows
$results
