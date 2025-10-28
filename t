# --- call (GET only; no -Body) ---
$runUrl   = "https://helios.cohesity.com/v2/data-protect/protection-groups/$pgId/runs?numRuns=15&includeObjectDetails=true&excludeNonRestorableRuns=true"
$response = Invoke-WebRequest -Method Get -Uri $runUrl -Headers $headers -UseBasicParsing
$json     = $response.Content | ConvertFrom-Json
$runs     = if ($json -and $json.protectionRuns) { $json.protectionRuns } else { @() }

$flatRuns = @()

foreach ($run in $runs) {

    # normalize objects array
    $objs = $run.objects
    if (-not $objs) { continue }
    if ($objs -isnot [System.Collections.IEnumerable]) { $objs = @($objs) }

    foreach ($obj in $objs) {

        # prefer nested .object, but fall back if needed
        $objNode   = if ($obj.object) { $obj.object } else { $obj }
        $objType   = $objNode.objectType
        if ($objType -ne 'kDatabase') { continue }   # <-- only databases

        $objName   = $objNode.name
        if (-not $objName) { $objName = $obj.objectName }

        # localBackupInfo can be single or array
        $lbi = $obj.localBackupInfo
        if (-not $lbi) { continue }
        if ($lbi -isnot [System.Collections.IEnumerable]) { $lbi = @($lbi) }

        foreach ($info in $lbi) {
            $flatRuns += [pscustomobject]@{
                RunType         = ($info.runType -replace '^k','')
                Status          = $info.status
                Object          = $objName
                ObjectType      = ($objType -replace '^k','')
                Message         = ($info.messages -join '; ')
                StartTimeUsecs  = $info.stats.startTimeUsecs
                EndTimeUsecs    = $info.stats.endTimeUsecs
                # If you have UsecsToEST(), add these two:
                # StartTime     = UsecsToEST $info.stats.startTimeUsecs
                # EndTime       = UsecsToEST $info.stats.endTimeUsecs
                Cluster         = $cluster_name
                ProtectionGroup = $pgName
            }
        }
    }
}

$flatRuns | Sort-Object ProtectionGroup,Object,RunType | Format-Table -AutoSize
