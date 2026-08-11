# Cohesity Rack Resiliency Assessment - STRICT READ-ONLY GET COLLECTION
# PowerShell 5.1 compatible

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl = 'https://helios.cohesity.com'
$outRoot = 'X:\PowerShell\Data\Cohesity\RackResiliencyAssessment'
$helper  = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$keyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
$NR = 'NOT RETURNED BY THIS CLUSTER VERSION/API'
$NA = 'NOT AVAILABLE THROUGH APPROVED READ-ONLY COLLECTION'

$runDir = Join-Path $outRoot ("Run_{0}" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
$rawDir = Join-Path $runDir 'Raw'
New-Item -Path $rawDir -ItemType Directory -Force | Out-Null

. $helper
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $keyFile
if ([string]::IsNullOrWhiteSpace([string]$apiKey)) { throw 'Empty API key' }

$allowedPaths = @(
    '/v2/mcm/cluster-mgmt/info','/v2/clusters','/v2/chassis','/v2/racks',
    '/v2/clusters/nodes','/v2/storage-domains','/v2/storage-domains/fault-tolerance-options'
)

function Headers([string]$ClusterId) {
    $h = @{ accept='application/json'; apiKey=$apiKey }
    if ($ClusterId) { $h.accessClusterId = $ClusterId }
    return $h
}

function Value($Object,[string[]]$Paths) {
    foreach ($path in $Paths) {
        $x = $Object
        foreach ($part in $path.Split('.')) {
            if ($null -eq $x) { $x = $null; break }
            $p = $x.PSObject.Properties[$part]
            if ($null -eq $p -or $null -eq $p.Value) { $x = $null; break }
            $x = $p.Value
        }
        if ($null -ne $x -and -not ($x -is [string] -and [string]::IsNullOrWhiteSpace($x))) { return $x }
    }
    return $null
}

function List($Object,[string[]]$Containers) {
    if ($null -eq $Object) { return @() }
    foreach ($name in $Containers) {
        $p = $Object.PSObject.Properties[$name]
        if ($null -ne $p -and $null -ne $p.Value) { return @($p.Value) }
    }
    if ($Object -is [System.Array]) { return @($Object) }
    return @($Object)
}

function Txt($Value) {
    if ($null -eq $Value -or ($Value -is [string] -and [string]::IsNullOrWhiteSpace($Value))) { return $NR }
    return [string]$Value
}

function Json($Value) {
    if ($null -eq $Value) { return $NR }
    return ($Value | ConvertTo-Json -Depth 50 -Compress)
}

function Safe([string]$Value) {
    if ([string]::IsNullOrWhiteSpace($Value)) { return 'UNKNOWN' }
    return [regex]::Replace($Value,'[^A-Za-z0-9_.-]','_')
}

function Fields($Object) {
    $item = @(List $Object @('clusters','cluster','chassis','racks','nodes','storageDomains')) | Select-Object -First 1
    if ($null -eq $item) { return '' }
    return (@($item.PSObject.Properties.Name) | Sort-Object -Unique) -join ','
}

function GetOnly([string]$Uri,[hashtable]$Headers,[string]$RawFile) {
    $method = 'GET'
    if ($method -cne 'GET') { throw 'SAFETY BLOCK: method is not GET' }

    $u = [uri]$Uri
    if (-not $Uri.StartsWith($baseUrl,[StringComparison]::OrdinalIgnoreCase)) { throw 'SAFETY BLOCK: URI outside Helios' }
    if ($allowedPaths -cnotcontains $u.AbsolutePath) { throw "SAFETY BLOCK: endpoint $($u.AbsolutePath)" }

    try {
        if ($method -cne 'GET') { throw 'SAFETY BLOCK: method changed before request' }
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method GET -UseBasicParsing -ErrorAction Stop
        if ($null -eq $r -or [string]::IsNullOrWhiteSpace([string]$r.Content)) {
            return [pscustomobject]@{ OK=$true; Data=$null; Error=''; Fields='' }
        }
        [IO.File]::WriteAllText($RawFile,[string]$r.Content,(New-Object System.Text.UTF8Encoding -ArgumentList $false))
        $d = $r.Content | ConvertFrom-Json
        return [pscustomobject]@{ OK=$true; Data=$d; Error=''; Fields=(Fields $d) }
    } catch {
        return [pscustomobject]@{ OK=$false; Data=$null; Error=$_.Exception.Message; Fields='' }
    }
}

$api = @(); $relations = @(); $nodesOut = @(); $sdOut = @(); $summary = @()

function ApiRow($Cluster,$Version,$Endpoint,$Result) {
    $script:api += [pscustomobject]@{
        Cluster=$Cluster; Version=$Version; Endpoint=$Endpoint
        Available=if($Result.OK){'YES'}else{'NO'}
        FieldsReturned=$Result.Fields; Error=if($Result.OK){''}else{$Result.Error}
    }
}

# GET-only cluster discovery.
$discovery = GetOnly "$baseUrl/v2/mcm/cluster-mgmt/info" (Headers '') (Join-Path $rawDir 'Helios_Cluster_Discovery.json')
if (-not $discovery.OK) { throw "Cluster discovery failed: $($discovery.Error)" }
$clusters = @(List $discovery.Data @('cohesityClusters','clusters'))
if ($clusters.Count -eq 0) { throw 'No clusters returned' }

foreach ($dc in $clusters) {
    $clusterId = Value $dc @('clusterId','id')
    if ($null -eq $clusterId) { continue }
    $discoveryName = Txt (Value $dc @('clusterName','name'))
    $clusterDir = Join-Path $rawDir ("{0}_{1}" -f (Safe $discoveryName),(Safe ([string]$clusterId)))
    New-Item -Path $clusterDir -ItemType Directory -Force | Out-Null
    $h = Headers ([string]$clusterId)

    # 1. VERSION FIRST.
    $cr = GetOnly "$baseUrl/v2/clusters?includeMinimumNodesInfo=true" $h (Join-Path $clusterDir '01_Cluster.json')
    $co = @(List $cr.Data @('clusters','cluster')) | Select-Object -First 1
    $cluster = Value $co @('name','clusterName'); if ($null -eq $cluster) { $cluster = $discoveryName }
    $version = Txt (Value $co @('version','softwareVersion','clusterSoftwareVersion'))
    $clusterNodes = Txt (Value $co @('nodeCount','numNodes'))
    $minimumNodesInfo = Value $co @('minimumNodesInfo')

    $virtual = Value $co @('isVirtual','isVirtualEdition','isVirtualCluster')
    if ($virtual -is [bool]) { $type = if($virtual){'Virtual'}else{'Physical'} }
    elseif ([string]$virtual -eq 'True') { $type='Virtual' }
    elseif ([string]$virtual -eq 'False') { $type='Physical' }
    else { $type = Txt (Value $co @('clusterType','type')) }

    ApiRow $cluster $version '/v2/clusters?includeMinimumNodesInfo=true' $cr

    # 2-6. Approved inventory GETs.
    $ch = GetOnly "$baseUrl/v2/chassis" $h (Join-Path $clusterDir '02_Chassis.json')
    $nr = GetOnly "$baseUrl/v2/chassis?noRackAssigned=true" $h (Join-Path $clusterDir '03_Chassis_NoRackAssigned.json')
    $rk = GetOnly "$baseUrl/v2/racks" $h (Join-Path $clusterDir '04_Racks.json')
    $nd = GetOnly "$baseUrl/v2/clusters/nodes" $h (Join-Path $clusterDir '05_Nodes.json')
    $sd = GetOnly "$baseUrl/v2/storage-domains" $h (Join-Path $clusterDir '06_StorageDomains.json')

    ApiRow $cluster $version '/v2/chassis' $ch
    ApiRow $cluster $version '/v2/chassis?noRackAssigned=true' $nr
    ApiRow $cluster $version '/v2/racks' $rk
    ApiRow $cluster $version '/v2/clusters/nodes' $nd
    ApiRow $cluster $version '/v2/storage-domains' $sd

    $chassis = @(List $ch.Data @('chassis')); $noRack = @(List $nr.Data @('chassis'))
    $racks = @(List $rk.Data @('racks')); $nodes = @(List $nd.Data @('nodes'))
    $domains = @(List $sd.Data @('storageDomains'))

    $rackById=@{}; foreach($r in $racks){$id=Value $r @('id','rackId');if($null -ne $id){$rackById[[string]$id]=$r}}
    $chById=@{}; $withRack=0; $mapped=0

    # 4. One relationship row per chassis. Full chassis object is preserved in 02_Chassis.json.
    foreach ($c in $chassis) {
        $cid=Value $c @('id','chassisId'); if($null -ne $cid){$chById[[string]$cid]=$c}
        $rid=Value $c @('rackId'); if($null -ne $rid){$withRack++}
        $r=$null; if($null -ne $rid -and $rackById.ContainsKey([string]$rid)){$r=$rackById[[string]$rid];$mapped++}
        $nodeIds=Value $c @('nodeIds')
        $relations += [pscustomobject][ordered]@{
            Cluster=$cluster;Version=$version;ChassisID=Txt $cid;ChassisSerial=Txt (Value $c @('serialNumber'))
            Model=Txt (Value $c @('hardwareModel'));NodeIDs=if($null -ne $nodeIds){@($nodeIds) -join ','}else{$NR}
            NodeCount=if($null -ne $nodeIds){@($nodeIds).Count}else{$NR};CohesityRackID=Txt $rid
            CohesityRackName=Txt (Value $r @('name','rackName'));ChassisLocation=Txt (Value $c @('location'))
            RackLocation=Txt (Value $r @('location'))
        }
    }

    # 5. Node -> Chassis -> configured Cohesity Rack.
    foreach ($n in $nodes) {
        $nid=Value $n @('id','nodeId'); $cid=Value $n @('chassisId','chassisInfo.chassisId')
        $c=$null; if($null -ne $cid -and $chById.ContainsKey([string]$cid)){$c=$chById[[string]$cid]}
        $rid=Value $c @('rackId'); $r=$null
        if($null -ne $rid -and $rackById.ContainsKey([string]$rid)){$r=$rackById[[string]$rid]}
        $nodesOut += [pscustomobject][ordered]@{
            Cluster=$cluster;Version=$version;NodeID=Txt $nid;Hostname=Txt (Value $n @('hostName','hostname','name'))
            ChassisID=Txt $cid;ChassisSerial=Txt (Value $c @('serialNumber'))
            CohesityRackID=Txt $rid;CohesityRackName=Txt (Value $r @('name','rackName'))
        }
    }

    # 6-7. Storage Domains + FT options. No version gating: try GET and record actual availability.
    $sdFt=@();$ecList=@();$rackFt=@();$minFt=@();$i=0
    foreach ($s in $domains) {
        $i++; $sid=Value $s @('id','storageDomainId'); $sname=Value $s @('name','storageDomainName')
        if($null -eq $sname){$sname="StorageDomain-$i"}
        $policy=Value $s @('storagePolicy')
        $legacyFt=[ordered]@{
            numFailuresTolerated=Value $policy @('numFailuresTolerated')
            numNodeFailuresTolerated=Value $policy @('numNodeFailuresTolerated')
            faultTolerance=Value $policy @('faultTolerance')
            faultToleranceLevel=Value $policy @('faultToleranceLevel')
        }
        $ftValues=@($legacyFt.Values|Where-Object{$null -ne $_})
        $currentFt=if($ftValues.Count){Json $legacyFt}else{$NR}
        $ec=Value $policy @('erasureCodingParams','erasureCodingInfo'); $ecText=if($null -ne $ec){Json $ec}else{$NR}

        if($null -ne $sid){
            $enc=[uri]::EscapeDataString([string]$sid)
            $fr=GetOnly "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$enc" $h (Join-Path $clusterDir ("07_FTOptions_SD_{0}.json" -f (Safe ([string]$sid))))
            ApiRow $cluster $version "/v2/storage-domains/fault-tolerance-options?storageDomainId=$sid" $fr
        } else {
            $fr=[pscustomobject]@{OK=$false;Data=$null;Error='Storage Domain ID not returned';Fields=''}
            ApiRow $cluster $version '/v2/storage-domains/fault-tolerance-options?storageDomainId=<missing>' $fr
        }

        $global=if($fr.OK){Value $fr.Data @('globalTolerance')}else{$null}
        $defaultFt=if($fr.OK){Value $fr.Data @('defaultFaultTolerance')}else{$null}
        $fdCount=if($fr.OK){Value $fr.Data @('failureDomainCount')}else{$null}
        $options=if($fr.OK){@(Value $fr.Data @('faultToleranceOptions'))}else{@()}
        $levels=@();$mins=@();$disabled=@()
        $gl=Value $global @('faultToleranceLevel');if($null -ne $gl){$levels+=$gl}
        foreach($o in $options){
            $l=Value $o @('faultTolerance.faultToleranceLevel','faultToleranceLevel');if($null -ne $l){$levels+=$l}
            $m=Value $o @('minFailureDomainsRequired');if($null -ne $m){$mins+=$m}
            $d=Value $o @('disabled');if($null -ne $d){$disabled+="disabled=$d"}
            $w=Value $o @('hasWarning');if($null -ne $w){$disabled+="hasWarning=$w"}
        }
        $rackText=if($levels.Count){($levels|Sort-Object -Unique) -join ','}elseif($fr.OK){$NR}else{$NA}
        $minText=if($mins.Count){($mins|Sort-Object -Unique) -join ','}elseif($null -ne $fdCount){Txt $fdCount}elseif($fr.OK){$NR}else{$NA}

        $sdOut += [pscustomobject][ordered]@{
            Cluster=$cluster;Version=$version;StorageDomainID=Txt $sid;StorageDomainName=Txt $sname
            StoragePolicy=Json $policy;CurrentStorageDomainFT=$currentFt;CurrentEC=$ecText
            FTOptionsAPI=if($fr.OK){'AVAILABLE'}else{'UNAVAILABLE'}
            GlobalTolerance=if($fr.OK){Json $global}else{$NA};DefaultFaultTolerance=if($fr.OK){Json $defaultFt}else{$NA}
            FailureDomainCount=if($fr.OK){Txt $fdCount}else{$NA};SupportedFailureDomainLevels=$rackText
            MinimumFailureDomains=$minText;DisabledOrWarningChoices=if($disabled.Count){$disabled-join','}elseif($fr.OK){$NR}else{$NA}
        }
        $sdFt+="$sname=$currentFt";$ecList+="$sname=$ecText";$rackFt+="$sname=$rackText";$minFt+="$sname=$minText"
    }

    $rackNames=@($racks|ForEach-Object{Value $_ @('name','rackName')}|Where-Object{$null -ne $_}|Sort-Object -Unique)
    $rackLoc=@($racks|ForEach-Object{Value $_ @('location')}|Where-Object{$null -ne $_}|Sort-Object -Unique)
    $chLoc=@($chassis|ForEach-Object{Value $_ @('location')}|Where-Object{$null -ne $_}|Sort-Object -Unique)
    $failureInfo=[ordered]@{
        minimumNodesInfo=$minimumNodesInfo;faultTolerance=Value $co @('faultTolerance')
        faultToleranceLevel=Value $co @('faultToleranceLevel');minimumFailureDomainsNeeded=Value $co @('minimumFailureDomainsNeeded')
    }
    $failureText=if (@($failureInfo.Values | Where-Object { $null -ne $_ }).Count -gt 0){Json $failureInfo}else{$NR}
    $mapping=if($chassis.Count -gt 0 -and $racks.Count -gt 0 -and $mapped -eq $chassis.Count){'YES'}else{'NO'}

    $summary += [pscustomobject][ordered]@{
        Cluster=$cluster;Version=$version;PhysicalVirtual=$type;Nodes=$nodes.Count;ClusterEndpointNodeCount=$clusterNodes
        Chassis=$chassis.Count;CohesityRacksConfigured=$racks.Count
        RackConfigurationState=if($racks.Count -eq 0){'NO COHESITY RACK CONFIGURATION PRESENT'}else{'CONFIGURED'}
        ChassisWithRackId=$withRack;ChassisWithoutRackId=($chassis.Count-$withRack);NoRackAssignedApiCount=$noRack.Count
        RackNames=if($rackNames.Count){$rackNames-join','}else{$NR};RackLocations=if($rackLoc.Count){$rackLoc-join','}else{$NR}
        ChassisLocations=if($chLoc.Count){$chLoc-join','}else{$NR};CurrentFailureDomainInformation=$failureText
        CurrentStorageDomainFT=if($sdFt.Count){$sdFt-join'; '}else{$NR};CurrentEC=if($ecList.Count){$ecList-join'; '}else{$NR}
        RackFTOptionsReportedByAPI=if($rackFt.Count){$rackFt-join'; '}else{$NR}
        MinimumFailureDomainsReportedByAPI=if($minFt.Count){$minFt-join'; '}else{$NR}
        PhysicalChassisToDatacenterRackMappingKnownFromCohesity=$mapping
    }
}

$relations|Export-Csv (Join-Path $runDir 'Chassis_Rack_Relationship.csv') -NoTypeInformation -Encoding UTF8
$nodesOut|Export-Csv (Join-Path $runDir 'Node_Chassis_Rack.csv') -NoTypeInformation -Encoding UTF8
$sdOut|Export-Csv (Join-Path $runDir 'Storage_Domains.csv') -NoTypeInformation -Encoding UTF8
$api|Export-Csv (Join-Path $runDir 'Version_Specific_Findings.csv') -NoTypeInformation -Encoding UTF8
$summary|Export-Csv (Join-Path $runDir 'Cluster_Assessment.csv') -NoTypeInformation -Encoding UTF8

$report=@('COHESITY RACK RESILIENCY ASSESSMENT - READ ONLY','Cohesity API methods used: GET only','')
foreach($s in $summary){
    $report+="Cluster: $($s.Cluster)";$report+="Version: $($s.Version)";$report+="Physical/Virtual: $($s.PhysicalVirtual)"
    $report+="Nodes: $($s.Nodes)";$report+="Chassis: $($s.Chassis)";$report+="Cohesity racks configured: $($s.CohesityRacksConfigured)"
    if($s.CohesityRacksConfigured -eq 0){$report+='NO COHESITY RACK CONFIGURATION PRESENT'}
    $report+="Chassis with rackId: $($s.ChassisWithRackId)";$report+="Chassis without rackId: $($s.ChassisWithoutRackId)"
    $report+="Rack names: $($s.RackNames)";$report+="Rack locations: $($s.RackLocations)";$report+="Chassis locations: $($s.ChassisLocations)"
    $report+="Current failure-domain information: $($s.CurrentFailureDomainInformation)"
    $report+="Current Storage Domain FT: $($s.CurrentStorageDomainFT)";$report+="Current EC: $($s.CurrentEC)"
    $report+="Rack FT options reported by API: $($s.RackFTOptionsReportedByAPI)"
    $report+="Minimum failure domains reported by API: $($s.MinimumFailureDomainsReportedByAPI)"
    $report+="Physical chassis-to-datacenter-rack mapping known from Cohesity: $($s.PhysicalChassisToDatacenterRackMappingKnownFromCohesity)";$report+=''
}
$report+='VERSION-SPECIFIC FINDINGS';$report+='Observed from actual responses only; no undocumented version difference is inferred.'
foreach($a in @($api | Sort-Object Version,Cluster,Endpoint)){$report+="$($a.Version) | $($a.Cluster) | $($a.Endpoint) | $($a.Available) | fields: $($a.FieldsReturned)"}
$report|Set-Content (Join-Path $runDir 'Rack_Resiliency_Assessment.txt') -Encoding UTF8

Write-Host '';Write-Host 'COHESITY RACK RESILIENCY SUMMARY' -ForegroundColor Cyan
$summary|Select-Object Cluster,Version,PhysicalVirtual,Nodes,Chassis,CohesityRacksConfigured,ChassisWithRackId,ChassisWithoutRackId,PhysicalChassisToDatacenterRackMappingKnownFromCohesity|Format-Table -AutoSize|Out-Host
Write-Host "Output: $runDir"
Write-Host 'Cohesity API methods used: GET only' -ForegroundColor Green
