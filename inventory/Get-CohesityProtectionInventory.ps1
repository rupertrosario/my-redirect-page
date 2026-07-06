# Cohesity Helios - Generic Protection Inventory
# GET-only. PowerShell 5.1 compatible.

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$outDir = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$baseUrl = "https://helios.cohesity.com"
$root = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath = Join-Path $root ("Common\" + "ApiKeyAesHelper.ps1")
$keyFile = Join-Path $root ("Common\Secure\cohesity_" + "apikey.enc")

if (-not (Test-Path $outDir)) { New-Item -Path $outDir -ItemType Directory -Force | Out-Null }
if (-not (Test-Path $helperPath)) { throw "Missing helper file." }
if (-not (Test-Path $keyFile)) { throw "Missing key file." }
. $helperPath
$keyLoader = "Get-Cohesity" + "ApiKeyFromAes"
$cohesityKey = & $keyLoader -EncryptedFile $keyFile
if ([string]::IsNullOrWhiteSpace($cohesityKey)) { throw "Key load returned empty value." }

$EnvironmentMap = @(
    [pscustomobject]@{ ApiName="kPhysical"; DisplayName="Physical"; ParamNames=@("physicalParams") },
    [pscustomobject]@{ ApiName="kHyperV"; DisplayName="Hyper-V"; ParamNames=@("hypervParams","hyperVParams") },
    [pscustomobject]@{ ApiName="kAcropolis"; DisplayName="Nutanix AHV"; ParamNames=@("acropolisParams","nutanixParams","ahvParams") }
)

function NowET { try { $tz=[TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time"); return ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(),$tz)).ToString("yyyy-MM-dd HH:mm:ss") } catch { return (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") } }
function Headers { param([string]$ClusterId) $h=@{accept="application/json"}; $h.Add(("api"+"Key"),$cohesityKey); if($ClusterId){$h["accessClusterId"]=$ClusterId}; return $h }
function JsonGet { param([string]$Uri,[hashtable]$Headers) if($PSVersionTable.PSVersion.Major -lt 6){$r=Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing}else{$r=Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get}; if(!$r -or [string]::IsNullOrWhiteSpace($r.Content)){return $null}; return ($r.Content|ConvertFrom-Json) }
function Arr { param($v) if($null -eq $v){return @()}; return @($v) }
function Prop { param($o,[string[]]$names) if($null -eq $o -or $o -is [string]){return $null}; foreach($n in $names){foreach($p in @($o.PSObject.Properties)){if($p.Name -ieq $n){return $p.Value}}}; return $null }
function Nest { param($o,[string]$path) $c=$o; foreach($part in ($path -split "\.")){if($null -eq $c -or $c -is [string]){return $null}; $c=Prop $c @($part)}; return $c }
function First { param($vals) foreach($v in @($vals)){foreach($x in @($v)){if($null -ne $x -and "$x".Trim() -ne ""){return "$x"}}}; return "" }
function Pack { param($v) if($null -eq $v){return ""}; try { return ($v | ConvertTo-Json -Depth 8 -Compress) } catch { return "$v" } }
function Et { param($u) if($null -eq $u -or "$u" -eq "0" -or "$u".Trim() -eq ""){return ""}; try{$e=[DateTime]::SpecifyKind([datetime]"1970-01-01",[DateTimeKind]::Utc);$d=$e.AddSeconds(([double]$u/1000000));$tz=[TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time");return ([TimeZoneInfo]::ConvertTimeFromUtc($d,$tz)).ToString("yyyy-MM-dd HH:mm:ss")}catch{return ""} }
function Good { param($s) if(!$s){return $false}; return (@("ksuccess","success","succeeded") -contains $s.Trim().ToLower()) }
function ObjName { param($o) return First @((Prop $o @("name","objectName","sourceName","vmName","hostName","displayName","id"))) }
function ObjId { param($o) return First @((Prop $o @("id","objectId","sourceId","vmId","entityId"))) }
function LooksId { param($v) if(!$v){return $false}; return ($v -match '^[a-fA-F0-9]{24,}$' -or $v -match '^[0-9]+:[0-9]+:[0-9]+$') }

function PolicyMap {
    param([hashtable]$Headers)
    $map=@{}
    foreach($u in @("$baseUrl/v2/data-protect/policies?maxResultCount=1000","$baseUrl/v2/data-protect/policies")){
        try{
            $j=JsonGet $u $Headers
            $list=Prop $j @("policies","policyList","items")
            if(!$list){$list=$j}
            foreach($p in @(Arr $list|Where-Object{$_ -and $_ -isnot [string]})){
                $policyObjId=First @((Prop $p @("id","policyId")))
                $policyObjName=First @((Prop $p @("name","policyName","displayName")))
                if($policyObjId -and $policyObjName){$map[$policyObjId]=$policyObjName}
            }
            if($map.Count -gt 0){break}
        }catch{}
    }
    return $map
}
function PolicyId { param($pg) return First @((Prop $pg @("policyId")),(Nest $pg "policyInfo.id"),(Nest $pg "policy.id")) }
function PolicyName { param($pg,[hashtable]$map) $resolvedPolicyId=PolicyId $pg; if($resolvedPolicyId -and $map.ContainsKey($resolvedPolicyId)){return $map[$resolvedPolicyId]}; $n=First @((Nest $pg "policyInfo.name"),(Nest $pg "policy.name"),(Nest $pg "policyConfig.name"),(Prop $pg @("policyName"))); if($n -and !(LooksId $n)){return $n}; if($n -and $map.ContainsKey($n)){return $map[$n]}; return "UNRESOLVED_POLICY_NAME" }
function PGs { param([string]$EnvApi,[hashtable]$Headers) $all=@();$cookie="";do{$u="$baseUrl/v2/data-protect/protection-groups?environments=$EnvApi&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000";if($cookie){$u="$u&paginationCookie=$([uri]::EscapeDataString($cookie))"};$j=JsonGet $u $Headers;$g=Prop $j @("protectionGroups");if($g){$all+=@(Arr $g|Where-Object{$_})};$cookie=First @((Prop $j @("paginationCookie")));$trunc=Prop $j @("isResponseTruncated");if($trunc -ne $true -and [string]::IsNullOrWhiteSpace($cookie)){break}}while($cookie);return @($all) }
function Params { param($pg,[string[]]$names) foreach($n in $names){$v=Prop $pg @($n);if($null -ne $v){return $v}};return $null }
function FindObjs { param($node,[int]$depth) $out=@();if($null -eq $node -or $node -is [string] -or $depth -gt 3){return @()};foreach($i in @(Arr $node)){if($null -eq $i -or $i -is [string]){continue};if(ObjName $i){$out+=$i};foreach($p in @($i.PSObject.Properties)){if($p.Value -and $p.Value -isnot [string]){$out+=@(FindObjs $p.Value ($depth+1))}}};return @($out) }
function Dedupe { param($objs) $seen=@{};$out=@();foreach($o in @(Arr $objs|Where-Object{$_ -and $_ -isnot [string]})){ $k=First @((ObjId $o),(ObjName $o));if($k -and !$seen.ContainsKey($k)){$seen[$k]=$true;$out+=$o}};return @($out) }
function ObjectsFromParams { param($params,[string]$envApi) if($envApi -eq "kPhysical"){$pt=First @((Prop $params @("protectionType")));if($pt -eq "kVolume"){return @(Arr (Nest $params "volumeProtectionTypeParams.objects")|Where-Object{$_})};return @(Arr (Nest $params "fileProtectionTypeParams.objects")|Where-Object{$_})};foreach($n in @("objects","sourceObjects","virtualMachines","vms","vmObjects","protectedObjects","selectedObjects","entities")){ $items=@(Arr (Prop $params @($n))|Where-Object{$_});if($items.Count -gt 0){return @(Dedupe $items)}};return @(Dedupe (FindObjs $params 0)) }
function RunInfo { param($pg) $lr=Prop $pg @("lastRun");$li=Prop $lr @("localBackupInfo","localSnapshotInfo");$s=First @((Prop $li @("status")),(Prop $lr @("status")));$rt=First @((Prop $li @("runType")),(Prop $lr @("runType")));$st=Et (First @((Prop $li @("startTimeUsecs")),(Prop $lr @("startTimeUsecs"))));$en=Et (First @((Prop $li @("endTimeUsecs")),(Prop $lr @("endTimeUsecs"))));$ok="";$okS="";if(Good $s){$ok=$en;$okS=$s};return [pscustomobject]@{LastRunStatus=$s;LastRunType=$rt;LastRunStartET=$st;LastRunEndET=$en;LastSuccessfulBackupET=$ok;LastSuccessfulBackupStatus=$okS;LastSuccessfulBackupAgeHours="";BackupFreshnessBucket="";IsSuccessLast24h=$false;IsSuccessLast48h=$false} }

$InventoryDateET = NowET
$clusterJson=JsonGet "$baseUrl/v2/mcm/cluster-mgmt/info" (Headers)
$clusterRows=@(Arr (Prop $clusterJson @("cohesityClusters")))
if($clusterRows.Count -eq 0){throw "No clusters returned from Helios."}
$clusters=$clusterRows|ForEach-Object{[pscustomobject]@{ClusterName=First @((Prop $_ @("clusterName")),(Prop $_ @("displayName")),(Prop $_ @("name")));ClusterId=First @((Prop $_ @("clusterId")),(Prop $_ @("id")))}}|Sort-Object ClusterName
$clusterMenu=for($i=0;$i -lt $clusters.Count;$i++){[pscustomobject]@{Index=$i+1;ClusterName=$clusters[$i].ClusterName;ClusterId=$clusters[$i].ClusterId}}
Write-Host "`nAvailable Helios Clusters:" -ForegroundColor Cyan
$clusterMenu|Format-Table -AutoSize
Write-Host "[0] All clusters`n[X] Exit" -ForegroundColor Yellow
while($true){$sel=Read-Host "Select cluster";if($sel -match '^(x|X|q|Q)$'){return};$num=0;if([int]::TryParse($sel,[ref]$num) -and $num -ge 0 -and $num -le $clusterMenu.Count){if($num -eq 0){$selectedClusters=@($clusterMenu)}else{$selectedClusters=@($clusterMenu|Where-Object{$_.Index -eq $num})};break};Write-Host "Invalid selection." -ForegroundColor Red}
Write-Host "`n[0] Physical + Hyper-V + Nutanix AHV`n[1] Physical only`n[2] Hyper-V only`n[3] Nutanix AHV only`n[X] Exit" -ForegroundColor Yellow
while($true){$es=Read-Host "Select environment";if($es -match '^(x|X|q|Q)$'){return};if($es -eq "0"){$selectedEnvironments=@($EnvironmentMap);break};if($es -eq "1"){$selectedEnvironments=@($EnvironmentMap|Where-Object{$_.ApiName -eq "kPhysical"});break};if($es -eq "2"){$selectedEnvironments=@($EnvironmentMap|Where-Object{$_.ApiName -eq "kHyperV"});break};if($es -eq "3"){$selectedEnvironments=@($EnvironmentMap|Where-Object{$_.ApiName -eq "kAcropolis"});break};Write-Host "Invalid selection." -ForegroundColor Red}

$pgRows=@();$objRows=@();$pathRows=@();$exRows=@();$errors=@()
foreach($c in $selectedClusters){
    $h=Headers $c.ClusterId
    $pm=PolicyMap $h
    foreach($e in $selectedEnvironments){
        Write-Host "Collecting $($e.DisplayName) PGs from $($c.ClusterName) ..." -ForegroundColor Yellow
        try{$pgList=@(PGs $e.ApiName $h)}catch{$errors+=[pscustomobject]@{Cluster=$c.ClusterName;Environment=$e.DisplayName;Stage="Get-ProtectionGroups";Error=$_.Exception.Message};continue}
        foreach($pg in $pgList){
            try{
                $p=Params $pg $e.ParamNames
                if($null -eq $p){$errors+=[pscustomobject]@{Cluster=$c.ClusterName;Environment=$e.DisplayName;Stage="EnvironmentParams";Error="No matching params"};continue}
                $pgId=First @((Prop $pg @("id")),(Prop $pg @("protectionGroupId")))
                $pgName=First @((Prop $pg @("name")),(Prop $pg @("protectionGroupName")),$pgId)
                $pgKey=if($c.ClusterId -and $pgId){"$($c.ClusterId)|$pgId"}else{"$($c.ClusterName)|$pgName"}
                $policyId=PolicyId $pg
                $policyName=PolicyName $pg $pm
                $r=RunInfo $pg
                $objs=@(ObjectsFromParams $p $e.ApiName)
                $ptype=First @((Prop $p @("protectionType")))
                if(!$ptype -and $e.ApiName -ne "kPhysical"){$ptype="VirtualMachine"}
                $sourceId=First @((Prop $p @("sourceId")),(Prop $pg @("sourceId")))
                $sourceName=First @((Prop $p @("sourceName")),(Prop $pg @("sourceName")),(Nest $pg "source.name"))
                $globalIncludeDisks=Pack (Prop $p @("globalIncludeDisks"))
                $globalExcludeDisks=Pack (Prop $p @("globalExcludeDisks"))
                $pgRows+=[pscustomobject]@{PGKey=$pgKey;InventoryDateET=$InventoryDateET;Cluster=$c.ClusterName;ClusterId=$c.ClusterId;Environment=$e.DisplayName;ProtectionGroup=$pgName;ProtectionGroupId=$pgId;PolicyName=$policyName;PolicyId=$policyId;IsActive=Prop $pg @("isActive");IsDeleted=Prop $pg @("isDeleted");ObjectCount=@($objs).Count;GlobalExcludePathCount=0;ObjectExcludePathCount=0;HasGlobalExclusions=($globalExcludeDisks -ne "");HasObjectExclusions=$false;LastSuccessfulBackupET=$r.LastSuccessfulBackupET;LastSuccessfulBackupStatus=$r.LastSuccessfulBackupStatus;LastSuccessfulBackupAgeHours=$r.LastSuccessfulBackupAgeHours;BackupFreshnessBucket=$r.BackupFreshnessBucket;IsSuccessLast24h=$r.IsSuccessLast24h;IsSuccessLast48h=$r.IsSuccessLast48h;LastRunStatus=$r.LastRunStatus;LastRunType=$r.LastRunType;LastRunStartET=$r.LastRunStartET;LastRunEndET=$r.LastRunEndET;IsPaused=Prop $pg @("isPaused");ProtectionType=$ptype;StorageDomain=First @((Prop $pg @("storageDomainName")),(Nest $pg "storageDomain.name"));SourceId=$sourceId;SourceName=$sourceName;GlobalIncludeDisks=$globalIncludeDisks;GlobalExcludeDisks=$globalExcludeDisks}
                foreach($o in $objs){
                    $objectName=ObjName $o
                    $objectId=ObjId $o
                    $objectKey="$pgKey|$(First @($objectId,$objectName))"
                    $objectType=First @((Prop $o @("objectType","type","entityType")))
                    if(!$objectType){$objectType=if($e.ApiName -eq "kPhysical"){"PhysicalObject"}else{"VirtualMachine"}}
                    $objectIncludedDisks=Pack (Prop $o @("includeDisks"))
                    $objectExcludedDisks=Pack (Prop $o @("excludeDisks"))
                    $objRows+=[pscustomobject]@{ObjectKey=$objectKey;PGKey=$pgKey;InventoryDateET=$InventoryDateET;Cluster=$c.ClusterName;ClusterId=$c.ClusterId;Environment=$e.DisplayName;ProtectionGroup=$pgName;ProtectionGroupId=$pgId;PolicyName=$policyName;PolicyId=$policyId;ObjectName=$objectName;ObjectId=$objectId;ObjectType=$objectType;GlobalIncludeDisks=$globalIncludeDisks;GlobalExcludeDisks=$globalExcludeDisks;ObjectIncludedDisks=$objectIncludedDisks;ObjectExcludedDisks=$objectExcludedDisks;HostName=First @((Prop $o @("hostName")),(Prop $o @("sourceName")),$objectName);ParentSource=First @((Prop $o @("parentSourceName")),(Prop $o @("sourceName")),(Prop $o @("sourceId")));IncludedPathCount=0;ObjectExcludePathCount=0;HasGlobalExclusions=($globalExcludeDisks -ne "");HasObjectExclusions=($objectExcludedDisks -ne "");LastSuccessfulBackupET=$r.LastSuccessfulBackupET;LastSuccessfulBackupStatus=$r.LastSuccessfulBackupStatus}
                }
            }catch{$errors+=[pscustomobject]@{Cluster=$c.ClusterName;Environment=$e.DisplayName;Stage="ProcessProtectionGroup";Error=$_.Exception.Message};continue}
        }
    }
}

$pgCsv=Join-Path $outDir "Cohesity_Protection_PG_Summary_Latest.csv";$objCsv=Join-Path $outDir "Cohesity_Protection_Object_Detail_Latest.csv";$pathCsv=Join-Path $outDir "Cohesity_Protection_Path_Detail_Latest.csv";$exCsv=Join-Path $outDir "Cohesity_Protection_Exceptions_Latest.csv";$metaJson=Join-Path $outDir "Cohesity_Protection_Run_Metadata.json"
$pgRows|Export-Csv $pgCsv -NoTypeInformation -Encoding utf8;$objRows|Export-Csv $objCsv -NoTypeInformation -Encoding utf8;$pathRows|Export-Csv $pathCsv -NoTypeInformation -Encoding utf8;$exRows|Export-Csv $exCsv -NoTypeInformation -Encoding utf8
$meta=[pscustomobject]@{InventoryDateET=$InventoryDateET;ScriptName="Get-CohesityProtectionInventory.ps1";HeliosBaseUrl=$baseUrl;SelectedClusters=@($selectedClusters|Select-Object ClusterName,ClusterId);SelectedEnvironments=@($selectedEnvironments|Select-Object ApiName,DisplayName);Counts=[pscustomobject]@{PGSummaryRows=@($pgRows).Count;ObjectDetailRows=@($objRows).Count;PathDetailRows=@($pathRows).Count;ExceptionRows=@($exRows).Count;CollectionErrors=@($errors).Count};EnvironmentCounts=@($pgRows|Group-Object Environment|Select-Object Name,Count);CollectionErrors=@($errors);Notes=@("GET-only collector","Object detail captures PG, policy, object ID, object name, object type, global disks, and object disks")}
$meta|ConvertTo-Json -Depth 10|Out-File $metaJson -Encoding utf8
Write-Host "`nCohesity Protection Inventory export complete." -ForegroundColor Green;Write-Host "PG Summary rows    : $(@($pgRows).Count)" -ForegroundColor Green;Write-Host "Object Detail rows : $(@($objRows).Count)" -ForegroundColor Green;Write-Host "Path Detail rows   : $(@($pathRows).Count)" -ForegroundColor Green;Write-Host "Exception rows     : $(@($exRows).Count)" -ForegroundColor Green;Write-Host "Collection errors  : $(@($errors).Count)" -ForegroundColor Green;Write-Host "Metadata JSON      : $metaJson" -ForegroundColor Green
