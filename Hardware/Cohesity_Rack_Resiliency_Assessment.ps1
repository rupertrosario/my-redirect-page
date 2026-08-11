# Cohesity Rack Readiness / Rack Resiliency Collector - STRICT GET ONLY
# PowerShell 5.1 compatible
$ErrorActionPreference='Stop'; $FormatEnumerationLimit=-1
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
$baseUrl='https://helios.cohesity.com'
$outDir='X:\PowerShell\Data\Cohesity\RackResiliencyAssessment'
$helper='X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$keyFile='X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
$NA='N/A'; $expectedClusters=23; $expectedNodes=173
if(!(Test-Path $outDir)){New-Item $outDir -ItemType Directory -Force|Out-Null}
. $helper; $apiKey=Get-CohesityApiKeyFromAes -EncryptedFile $keyFile
if([string]::IsNullOrWhiteSpace($apiKey)){throw 'Empty API key'}

function H([string]$cid){$h=@{accept='application/json';apiKey=$apiKey};if($cid){$h.accessClusterId=$cid};$h}
function G([string]$uri,[hashtable]$headers){
  if(!$uri.StartsWith($baseUrl,[StringComparison]::OrdinalIgnoreCase)){throw 'Blocked URI'}
  $p=([uri]$uri).AbsolutePath
  $ok=@('/v2/mcm/cluster-mgmt/info','/v2/clusters/nodes','/v2/chassis','/v2/ipmi/get-lan-info','/v2/storage-domains','/v2/storage-domains/fault-tolerance-options','/irisservices/api/v1/public/cluster')
  if($ok -cnotcontains $p){throw "Blocked GET endpoint: $p"}
  $r=Invoke-WebRequest -Uri $uri -Headers $headers -Method GET -UseBasicParsing -ErrorAction Stop
  if(!$r -or [string]::IsNullOrWhiteSpace([string]$r.Content)){return $null};$r.Content|ConvertFrom-Json
}
function M($o,[string]$n){if($null-eq$o){return $null};foreach($p in @($o.PSObject.Properties)){if($p.Name-ceq$n){return $p}};$null}
function V($o,[string]$n){$p=M $o $n;if($null-eq$p-or$null-eq$p.Value){return $NA};if($p.Value-is[string]-and[string]::IsNullOrWhiteSpace($p.Value)){return $NA};$p.Value}
function O($o,[string]$n){$p=M $o $n;if($null-eq$p-or$null-eq$p.Value){return $null};$p.Value}
function A($o,[string]$n){$p=M $o $n;if($null-eq$p-or$null-eq$p.Value){return @()};@($p.Value)}
function FT($d,$n){if($null-eq$d-or$null-eq$n-or[string]$d-eq$NA-or[string]$n-eq$NA){return $NA};'{0}D:{1}N'-f$d,$n}
function EC($o){if($null-eq$o){return $NA};$d=V $o 'numDataStripes';$c=V $o 'numCodedStripes';if([string]$d-eq$NA-or[string]$c-eq$NA){return $NA};'{0}:{1}'-f$d,$c}
function Fail($list,$cluster,$ep){$list.Add("$cluster | GET $ep | FAILED")}

function SDInfo($sds,[hashtable]$headers,[string]$cluster,$fails){
  $ecs=@();$dfs=@();$srcs=@();$gls=@();$gcs=@();$fdcs=@();$rfs=@();$opts=@()
  $i=0
  foreach($sd in @($sds|Sort-Object{[string](V $_ 'id')})){
    $i++;$id=V $sd 'id';$tag=if([string]$id-ne$NA){"SD-$i(ID=$id)"}else{"SD-$i"};$sp=O $sd 'storagePolicy'
    $ecp=O $sp 'erasureCodingParams';$eci=O $sp 'erasureCodingInfo'
    if($ecp){$ec=EC $ecp;$ece=V $ecp 'enabled';$eci2=V $ecp 'inlineEnabled'}
    elseif($eci){$ec=EC $eci;$ece=V $eci 'erasureCodingEnabled';$eci2=V $eci 'inlineErasureCoding'}
    else{$ec=$NA;$ece=$NA;$eci2=$NA}
    $ecs+="$tag=EC:$ec,Enabled:$ece,Inline:$eci2"

    $legacy=((M $sp 'numFailuresTolerated') -or (M $sp 'numNodeFailuresTolerated'))
    $df=$NA;$src=$NA;$gl=$NA;$gc=$NA;$fdc=$NA;$rf=$NA;$op=$NA
    if($legacy){
      $df=FT (V $sp 'numFailuresTolerated') (V $sp 'numNodeFailuresTolerated');$src='LegacyStoragePolicy'
    }elseif([string]$id-ne$NA){
      $fr=$null
      try{$fr=G "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$([uri]::EscapeDataString([string]$id))" $headers}catch{Fail $fails $cluster '/v2/storage-domains/fault-tolerance-options'}
      $gt=O $fr 'globalTolerance';$gl=V $gt 'faultToleranceLevel';$gc=V $gt 'count';$fdc=V $fr 'failureDomainCount'
      $dt=O $fr 'defaultFaultTolerance';$df=FT (V $dt 'numDiskFailuresTolerated') (V $dt 'numDomainFailuresTolerated');if($dt){$src='FaultToleranceOptions'}
      $ol=@();$rl=@();$oi=0
      foreach($x in @(A $fr 'faultToleranceOptions')){
        $oi++;$f=O $x 'faultTolerance';$fl=FT (V $f 'numDiskFailuresTolerated') (V $f 'numDomainFailuresTolerated')
        $dec=EC (O $x 'defaultErasureCoding');$el=@();$ei=0
        foreach($e in @(A $x 'erasureCodingOptions')){$ei++;$el+="EC-$ei=$(EC (O $e 'erasureCoding')),Inline=$(V $e 'inlineSupported'),Suboptimal=$(V $e 'isSuboptimal'),MinHeal=$(V $e 'minDomainsForHeal')"}
        $ecol=if($el){$el-join'|'}else{$NA};$rr=V $x 'defaultReplicationFactor'
        $ol+="Opt-$oi:$fl,Disabled=$(V $x 'disabled'),Warning=$(V $x 'hasWarning'),MinDomains=$(V $x 'minFailureDomainsRequired'),RF=$rr,DefaultEC=$dec,ECOpts=[$ecol]"
        $rl+="$fl=$rr"
      }
      if($ol){$op=$ol-join'||'};if($rl){$rf=$rl-join'|'}
    }
    $dfs+="$tag=$df";$srcs+="$tag=$src";$gls+="$tag=$gl";$gcs+="$tag=$gc";$fdcs+="$tag=$fdc";$rfs+="$tag=$rf";$opts+="$tag=$op"
  }
  [pscustomobject]@{Count=@($sds).Count;EC=if($ecs){$ecs-join'; '}else{$NA};DefaultFT=if($dfs){$dfs-join'; '}else{$NA};FTSource=if($srcs){$srcs-join'; '}else{$NA};GlobalLevel=if($gls){$gls-join'; '}else{$NA};GlobalCount=if($gcs){$gcs-join'; '}else{$NA};FailureDomains=if($fdcs){$fdcs-join'; '}else{$NA};RF=if($rfs){$rfs-join'; '}else{$NA};Options=if($opts){$opts-join'; '}else{$NA}}
}

$fails=New-Object System.Collections.Generic.List[string];$warnings=New-Object System.Collections.Generic.List[string]
$cr=G "$baseUrl/v2/mcm/cluster-mgmt/info" (H '');$clusters=@()
foreach($c in @(A $cr 'cohesityClusters')){$id=V $c 'clusterId';if([string]$id-ne$NA){$clusters+=[pscustomobject]@{Id=[string]$id;Name=V $c 'clusterName'}}}
$clusters=@($clusters|Sort-Object Id -Unique);if(!$clusters){throw 'No clusters returned'}
$rows=@();$sum=@();$tn=0;$tc=0;$tsd=0;$ci=0
foreach($c in $clusters){
  $ci++;$alias='Cluster-{0:D2}'-f$ci;$display="$alias [$($c.Name)]";$h=H $c.Id;Write-Host "Processing $display" -ForegroundColor Yellow
  $nodes=@();$chs=@();$sds=@();$cf=$null
  try{$nodes=@(G "$baseUrl/v2/clusters/nodes" $h)}catch{Fail $fails $display '/v2/clusters/nodes'}
  try{$x=G "$baseUrl/v2/chassis" $h;$chs=@(A $x 'chassis')}catch{Fail $fails $display '/v2/chassis'}
  try{$x=G "$baseUrl/v2/storage-domains?matchPartialNames=false&includeTenants=true&includeStats=true" $h;$sds=@(A $x 'storageDomains')}catch{Fail $fails $display '/v2/storage-domains'}
  try{$cf=G "$baseUrl/irisservices/api/v1/public/cluster?fetchStats=true" $h}catch{Fail $fails $display '/irisservices/api/v1/public/cluster?fetchStats=true'}
  $flt=V $cf 'faultToleranceLevel';$mft=V $cf 'metadataFaultToleranceFactor';$mfd=V $cf 'minimumFailureDomainsNeeded';$sd=SDInfo $sds $h $display $fails
  $cb=@{};foreach($ch in $chs){$x=V $ch 'id';if([string]$x-ne$NA){$cb[[string]$x]=$ch}}
  $cn=@()
  foreach($n in $nodes){
    $nid=V $n 'id';$hn=V $n 'hostName';$nip=V $n 'ip';$cis=O $n 'chassisInfo';$chid=V $cis 'chassisId';$ch=$null;if([string]$chid-ne$NA-and$cb.ContainsKey([string]$chid)){$ch=$cb[[string]$chid]}
    $ii=$NA;$is=$NA;$im=$NA
    if([string]$nid-ne$NA){try{$ip=G "$baseUrl/v2/ipmi/get-lan-info?nodeId=$([uri]::EscapeDataString([string]$nid))" $h;$ii=V $ip 'lanIp';$is=V $ip 'ipAddrSource';$im=V $ip 'subnetMask'}catch{Fail $fails $display '/v2/ipmi/get-lan-info'}}
    $r=[pscustomobject][ordered]@{ClusterName=$c.Name;ClusterId=$c.Id;FaultToleranceLevel=$flt;MetadataFaultToleranceFactor=$mft;MinimumFailureDomainsNeeded=$mfd;NodeId=$nid;Hostname=$hn;NodeIP=$nip;IPMIIP=$ii;IPMISource=$is;IPMISubnetMask=$im;NodeSerial=$NA;CohesityNodeSerial=V $n 'cohesityNodeSerial';NodeModel=$NA;ProductModel=V $n 'productModel';ProductModelType=$NA;SlotNumber=V $n 'slotNumber';NodeStatus=$NA;Reachable=$NA;ChassisId=$chid;ChassisSerial=V $ch 'serialNumber';CohesityChassisSerial=$NA;ChassisModel=V $ch 'hardwareModel';ChassisType=$NA;MaxSlots=$NA;StorageDomainCount=$sd.Count;StorageDomainEC=$sd.EC;StorageDomainDefaultFT=$sd.DefaultFT;GlobalToleranceLevel=$sd.GlobalLevel;GlobalToleranceCount=$sd.GlobalCount;FailureDomainCount=$sd.FailureDomains;DefaultReplicationFactor=$sd.RF;RackFTOptions=$sd.Options}
    $rows+=$r;$cn+=$r
  }
  $mix=@();foreach($g in @($cn|Group-Object ProductModel|Sort-Object Name)){$mix+="$($g.Name)=$($g.Count)"};$npc=@();foreach($ch in $chs){$npc+=@(A $ch 'nodeIds').Count}
  $sum+=[pscustomobject][ordered]@{Cluster=$alias;ClusterName=$c.Name;Nodes=$nodes.Count;Chassis=$chs.Count;HardwareMix=if($mix){$mix-join'; '}else{$NA};NodesChassis=if($npc){($npc|Sort-Object)-join','}else{$NA};EC=$sd.EC;CurrentFT=$flt;DefaultFT=$sd.DefaultFT;FTSource=$sd.FTSource}
  $tn+=$nodes.Count;$tc+=$chs.Count;$tsd+=$sd.Count
}
$stamp=Get-Date -Format 'yyyyMMdd_HHmmss';$csv=Join-Path $outDir "Cohesity_Rack_Readiness_Detail_$stamp.csv";$txt=Join-Path $outDir "Cohesity_Rack_Readiness_Summary_$stamp.txt"
$rows|Sort-Object ClusterName,Hostname,NodeId|Export-Csv $csv -NoTypeInformation -Encoding UTF8
$ev=if($clusters.Count-eq$expectedClusters-and$tn-eq$expectedNodes){'PASS'}else{"WARNING expected $expectedClusters/$expectedNodes collected $($clusters.Count)/$tn"}
$t=New-Object System.Collections.Generic.List[string];$t.Add('COHESITY RACK READINESS SUMMARY');$t.Add('');$t.Add(($sum|Format-Table Cluster,ClusterName,Nodes,Chassis,HardwareMix,NodesChassis,EC,CurrentFT,DefaultFT,FTSource -AutoSize -Wrap|Out-String -Width 320).TrimEnd());$t.Add('');$t.Add("Clusters        : $($clusters.Count)");$t.Add("Nodes           : $tn");$t.Add("Chassis         : $tc");$t.Add("Storage Domains : $tsd");$t.Add("Estate validation: $ev");$t.Add("GET failures    : $($fails.Count)");$t.Add('Non-GET calls   : 0');if($fails.Count){$t.Add('');$t.Add('GET Failures');foreach($f in $fails){$t.Add($f)}};$t|Set-Content $txt -Encoding UTF8
Write-Host '';Write-Host 'CLUSTER SUMMARY' -ForegroundColor Cyan;$sum|Format-Table Cluster,ClusterName,Nodes,Chassis,HardwareMix,NodesChassis,EC,CurrentFT,DefaultFT,FTSource -AutoSize -Wrap|Out-Host
Write-Host "Clusters=$($clusters.Count) Nodes=$tn Chassis=$tc StorageDomains=$tsd GETfailures=$($fails.Count) NonGET=0" -ForegroundColor Green
Write-Host "CSV: $csv";Write-Host "TXT: $txt"
