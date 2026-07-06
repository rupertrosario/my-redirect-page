<#
Standalone incident evidence script for Cohesity backup failures.
Reference only: backup_failures/Cohesity_Backup_Failures
Does not modify the reference script.
Output files only: current_failures.csv, recovered.csv, new_failures.csv, new_recoveries.csv, worknotes.txt, state.json
#>
[CmdletBinding()]
param(
  [string]$BaseUrl = "https://helios.cohesity.com",
  [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
  [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
  [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
  [int]$NumRuns = 30,
  [int]$MaxClusters = 0,
  [int]$MaxProtectionGroupsPerCluster = 0,
  [string]$ClusterName = "",
  [string]$IncidentNumber = ""
)
$ErrorActionPreference = "Stop"

function Tz { try { [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") } catch { [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") } }
$script:ET = Tz
function P($o,$n,$d=$null){ if($null -eq $o){return $d}; $p=$o.PSObject.Properties[$n]; if($p){return $p.Value}; return $d }
function A($x){ if($null -eq $x){@()} elseif($x -is [array]){@($x)} else{@($x)} }
function Clean($s){ if($null -eq $s){return ""}; if($s -is [array]){$s=$s -join " | "}; return (([string]$s -replace "[\r\n]+"," ") -replace "\s+"," ").Replace('"',"'").Trim() }
function Et($usecs){ if(!$usecs){return ""}; $ms=[int64]([double]$usecs/1000); $u=[DateTimeOffset]::FromUnixTimeMilliseconds($ms).UtcDateTime; ([TimeZoneInfo]::ConvertTimeFromUtc($u,$script:ET)).ToString("yyyy-MM-dd HH:mm:ss") }
function EtToUsecs([datetime]$dt){ $u=[TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($dt,[DateTimeKind]::Unspecified),$script:ET); [int64](([DateTimeOffset]::new($u,[TimeSpan]::Zero)).ToUnixTimeMilliseconds()*1000) }
function JsonIn($p){ if(Test-Path $p){ $r=Get-Content $p -Raw; if($r){return $r|ConvertFrom-Json} }; return $null }
function JsonOut($o,$p){ $d=Split-Path $p -Parent; if(!(Test-Path $d)){New-Item $d -ItemType Directory -Force|Out-Null}; $o|ConvertTo-Json -Depth 80|Set-Content $p -Encoding UTF8 }
function CsvOut($rows,$path,$cols){ $d=Split-Path $path -Parent; if(!(Test-Path $d)){New-Item $d -ItemType Directory -Force|Out-Null}; $r=@($rows); if($r.Count -eq 0){($cols -join ",")|Set-Content $path -Encoding UTF8}else{$r|Select-Object $cols|Export-Csv $path -NoTypeInformation -Encoding UTF8} }
function Q($h){ (($h.GetEnumerator()|%{[uri]::EscapeDataString([string]$_.Key)+"="+[uri]::EscapeDataString([string]$_.Value)}) -join "&") }
function GET($u,$h){ $r=Invoke-WebRequest -Uri $u -Headers $h -Method Get -UseBasicParsing; if(!$r.Content){return $null}; $r.Content|ConvertFrom-Json }

function WindowNow{
  $n=[TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(),$script:ET)
  if($n.Hour -lt 18){$s=$n.Date.AddDays(-1).AddHours(18)}else{$s=$n.Date.AddHours(18)}
  $e=$s.AddDays(1); $sk=$s.ToString("yyyy-MM-dd"); $ek=$e.ToString("yyyy-MM-dd")
  [pscustomobject]@{Key="${sk}_1800ET";Label="$sk 18:00 ET -> $ek 18:00 ET";StartUsecs=EtToUsecs $s;EndUsecs=EtToUsecs $e;StartET=$s.ToString("yyyy-MM-dd HH:mm:ss");EndET=$e.ToString("yyyy-MM-dd HH:mm:ss");GeneratedET=([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(),$script:ET)).ToString("yyyy-MM-dd HH:mm:ss")}
}

function ResolveIncident($w){
  if(!(Test-Path $OutputRoot)){New-Item $OutputRoot -ItemType Directory -Force|Out-Null}
  $rp=Join-Path $OutputRoot "BackupFailure_IncidentRegistry.json"
  $reg=JsonIn $rp
  if(!$reg){$reg=[pscustomobject]@{WindowSource="backup_failures/compute_window.js";Windows=[pscustomobject]@{}}}
  $old=$reg.Windows.PSObject.Properties[$w.Key]
  if($old){return $old.Value}
  $inc=$IncidentNumber; if(!$inc){$inc=Read-Host "Enter incident number for this backup-failure window"}
  $inc=$inc.Trim().ToUpper(); if($inc -notmatch '^INC[0-9A-Z]+$'){throw "Invalid incident number: $inc"}
  $entry=[pscustomobject]@{IncidentNumber=$inc;WindowKey=$w.Key;WindowLabel=$w.Label;FirstRunET=$w.GeneratedET;LastRunET=$w.GeneratedET;OutputFolder=(Join-Path $OutputRoot $inc)}
  $reg.Windows|Add-Member NoteProperty $w.Key $entry -Force
  JsonOut $reg $rp
  return $entry
}

function EnvCode($pg){ $e=[string](P $pg "environment" ""); if(!$e){$t=A(P $pg "environmentTypes" @()); if($t.Count){$e=[string]$t[0]}}; $e }
function EnvLabel($e){ switch($e){"kOracle"{"Oracle"}"kSQL"{"SQL"}"kPhysical"{"Physical"}"kGenericNas"{"NAS"}"kIsilon"{"Isilon"}"kHyperV"{"HyperV"}"kAcropolis"{"Acropolis"}"kRemoteAdapter"{"RemoteAdapter"}default{if($e){$e}else{"Unknown"}}} }
function IsFail($s){ $s -in @("Failed","kFailed") }
function IsOK($s){ $s -in @("Succeeded","SucceededWithWarning","kSucceeded","kSucceededWithWarning") }
function Relevant($env,$obj){ $t=[string](P $obj "objectType" ""); switch($env){"kOracle"{$t -in @("kDatabase","kHost")}"kSQL"{$t -in @("kDatabase","kHost")}"kPhysical"{$t -eq "kHost"}"kGenericNas"{$t -eq "kHost"}"kIsilon"{$t -eq "kHost"}"kHyperV"{$t -eq "kVirtualMachine"}"kAcropolis"{$t -eq "kVirtualMachine"}default{$true}} }
function FailedAttempts($ro){ try{@($ro.localSnapshotInfo.failedAttempts)}catch{@()} }
function FailMsg($ro){ (($ro|%{FailedAttempts $_}|%{Clean $_.message}|?{$_}) -join " | ") }
function ObjKey($cid,$c,$env,$pgid,$pg,$oid,$host,$name){ if($oid){"$cid|$env|$pgid|$oid"}else{"$c|$env|$pg|$host|$name"} }
function Row($inc,$w,$cid,$c,$env,$pgid,$pg,$host,$oid,$name,$type,$rt,$kind,$usecs,$msg){ $k=ObjKey $cid $c $env $pgid $pg $oid $host $name; [pscustomobject]@{IncidentNumber=$inc;WindowKey=$w.Key;Cluster=$c;Environment=(EnvLabel $env);ProtectionGroup=$pg;Host=$host;ObjectName=$name;ObjectType=$type;RunType=$rt;EventKind=$kind;EventTimeET=(Et $usecs);EventTimeUsecs=$usecs;Message=$msg;ObjectKey=$k} }

function ExpandRun($inc,$w,$cluster,$pg,$run,$info){
  $status=[string](P $info "status" ""); $kind=if(IsFail $status){"Failed"}elseif(IsOK $status){"Success"}else{return @()}
  $start=[int64](P $info "startTimeUsecs" 0); $end=[int64](P $info "endTimeUsecs" 0); $usecs=if($end){$end}else{$start}
  if($usecs -lt $w.StartUsecs -or $usecs -ge $w.EndUsecs){return @()}
  $cid=[string](P $cluster "clusterId" ""); $c=[string](P $cluster "name" ""); if(!$c){$c=[string](P $cluster "clusterName" "Unknown-$cid")}
  $pgid=[string](P $pg "id" ""); $pgn=[string](P $pg "name" "Unknown PG"); $env=EnvCode $pg; $rt=[string](P $info "runType" ""); $msg=Clean(P $info "messages" "")
  $objs=A(P $run "objects" @()); if($objs.Count -eq 0){return @(Row $inc $w $cid $c $env $pgid $pgn "" "" $pgn "ProtectionGroup" $rt $kind $usecs $msg)}
  $idName=@{}; foreach($ro in $objs){$o=P $ro "object" $null; $id=[string](P $o "id" ""); $nm=[string](P $o "name" ""); if($id -and $nm){$idName[$id]=$nm}}
  $out=@()
  foreach($ro in $objs){
    $o=P $ro "object" $null; if(!$o -or !(Relevant $env $o)){continue}
    if($kind -eq "Failed" -and (FailedAttempts $ro).Count -eq 0){continue}
    $type=[string](P $o "objectType" ""); $oid=[string](P $o "id" ""); $name=[string](P $o "name" ""); $host=""
    if($env -in @("kOracle","kSQL") -and $type -eq "kHost"){$host=$name; if($kind -eq "Failed"){$name="No DBs Found (Host-Level Failure)"}}
    if($env -in @("kOracle","kSQL") -and $type -eq "kDatabase"){$sid=[string](P $o "sourceId" ""); if($sid -and $idName.ContainsKey($sid)){$host=$idName[$sid]}}
    $m=$msg; if($kind -eq "Failed"){$fm=FailMsg $ro; if($fm){$m=$fm}}
    $out += Row $inc $w $cid $c $env $pgid $pgn $host $oid $name $type $rt $kind $usecs $m
  }
  if($out.Count -eq 0 -and $kind -eq "Failed"){$out += Row $inc $w $cid $c $env $pgid $pgn "" "" $pgn "ProtectionGroup" $rt $kind $usecs $msg}
  return @($out)
}

function OutRow($e,$status,$first,$last,$rec,$cnt){ [pscustomobject]@{IncidentNumber=$e.IncidentNumber;WindowKey=$e.WindowKey;Status=$status;Cluster=$e.Cluster;Environment=$e.Environment;ProtectionGroup=$e.ProtectionGroup;Host=$e.Host;ObjectName=$e.ObjectName;ObjectType=$e.ObjectType;RunType=$e.RunType;FirstFailedET=$first;LastFailedET=$last;RecoveredET=$rec;ConsecutiveFailureCount=$cnt;Message=$e.Message;ObjectKey=$e.ObjectKey} }

if(!(Test-Path $HelperPath)){throw "Missing API key helper: $HelperPath"}; if(!(Test-Path $EncryptedFile)){throw "Missing encrypted key file: $EncryptedFile"}
. $HelperPath
$key=Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
if(!$key){throw "API key is blank"}

$w=WindowNow; $lock=ResolveIncident $w; $inc=$lock.IncidentNumber; $outDir=$lock.OutputFolder
if(!(Test-Path $outDir)){New-Item $outDir -ItemType Directory -Force|Out-Null}
$statePath=Join-Path $outDir "state.json"; $prev=JsonIn $statePath; $prevKeys=@{}
if($prev){foreach($x in A(P $prev "CurrentFailures" @())){$k=[string](P $x "ObjectKey" ""); if($k){$prevKeys[$k]=$true}}}

$headers=@{apiKey=$key;accept="application/json"}
$clu=(GET "$BaseUrl/v2/mcm/cluster-mgmt/info" $headers).cohesityClusters
if($ClusterName){$clu=@($clu|?{(P $_ "name" "") -eq $ClusterName -or (P $_ "clusterName" "") -eq $ClusterName -or (P $_ "displayName" "") -eq $ClusterName})}
if($MaxClusters -gt 0){$clu=@($clu|select -First $MaxClusters)}
$events=@(); $warn=@()
foreach($c in $clu){
  $cid=[string](P $c "clusterId" ""); $ch=@{apiKey=$key;accept="application/json";accessClusterId=$cid}
  try{$pgs=(GET "$BaseUrl/v2/data-protect/protection-groups?$(Q @{isDeleted='false';isPaused='false';isActive='true'})" $ch).protectionGroups}catch{$warn+=$_.Exception.Message;continue}
  if($MaxProtectionGroupsPerCluster -gt 0){$pgs=@($pgs|select -First $MaxProtectionGroupsPerCluster)}
  foreach($pg in $pgs){
    $pgid=[string](P $pg "id" "")
    try{$runs=(GET "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgid))/runs?$(Q @{numRuns=$NumRuns;excludeNonRestorableRuns='false';includeObjectDetails='true'})" $ch).runs}catch{$warn+=$_.Exception.Message;continue}
    foreach($r in $runs){foreach($i in A(P $r "localBackupInfo" @())){$events += ExpandRun $inc $w $c $pg $r $i}}
  }
}

$current=@(); $recovered=@()
foreach($g in ($events|sort ObjectKey,EventTimeUsecs|group ObjectKey)){
  $ev=@($g.Group|sort EventTimeUsecs); $fail=@($ev|?{$_.EventKind -eq "Failed"}); if($fail.Count -eq 0){continue}
  $first=$fail[0]; $last=$fail[-1]; $ok=@($ev|?{$_.EventKind -eq "Success" -and $_.EventTimeUsecs -gt $last.EventTimeUsecs}|sort EventTimeUsecs|select -First 1)
  $cnt=0; foreach($x in $ev){if($x.EventKind -eq "Success"){$cnt=0}elseif($x.EventKind -eq "Failed"){$cnt++}}
  if($ok.Count){$recovered += OutRow $last "Recovered" $first.EventTimeET $last.EventTimeET $ok[0].EventTimeET 0}else{$st=if($cnt -gt 1){"StillFailing-Consecutive"}else{"StillFailing"}; $current += OutRow $last $st $first.EventTimeET $last.EventTimeET "" $cnt}
}
$newFail=@($current|?{!$prevKeys.ContainsKey($_.ObjectKey)})
$newRec=@($recovered|?{$prevKeys.ContainsKey($_.ObjectKey)})
$cols="IncidentNumber","WindowKey","Status","Cluster","Environment","ProtectionGroup","Host","ObjectName","ObjectType","RunType","FirstFailedET","LastFailedET","RecoveredET","ConsecutiveFailureCount","Message","ObjectKey"
CsvOut $current (Join-Path $outDir "current_failures.csv") $cols
CsvOut $recovered (Join-Path $outDir "recovered.csv") $cols
CsvOut $newFail (Join-Path $outDir "new_failures.csv") $cols
CsvOut $newRec (Join-Path $outDir "new_recoveries.csv") $cols

$note=@"
Backup Failure Evidence

Incident: $inc
Window: $($w.Label)
Generated At: $($w.GeneratedET) ET

Summary:
- Current failures: $(@($current).Count)
- Recovered in this window: $(@($recovered).Count)
- New failures since last run: $(@($newFail).Count)
- New recoveries since last run: $(@($newRec).Count)

Attach/save these files in the incident:
- current_failures.csv
- recovered.csv
- new_failures.csv
- new_recoveries.csv
- worknotes.txt
- state.json

Notes:
- Cohesity calls are GET-only.
- No Excel output is generated.
- Script success means evidence was collected; it does not mean backups succeeded.
"@
if($warn.Count){$note += "`nWarnings:`n" + (($warn|%{"- $_"}) -join "`n")}
$note|Set-Content (Join-Path $outDir "worknotes.txt") -Encoding UTF8
JsonOut ([pscustomobject]@{IncidentNumber=$inc;WindowKey=$w.Key;WindowLabel=$w.Label;LastRunET=$w.GeneratedET;CurrentFailures=@($current|select ObjectKey,Cluster,Environment,ProtectionGroup,Host,ObjectName,ObjectType,RunType,LastFailedET,Message);Recovered=@($recovered|select ObjectKey,Cluster,Environment,ProtectionGroup,Host,ObjectName,ObjectType,RunType,RecoveredET)}) $statePath
Write-Host "`nIncident: $inc`nWindow  : $($w.Label)`nOutput  : $outDir`nCreated : current_failures.csv, recovered.csv, new_failures.csv, new_recoveries.csv, worknotes.txt, state.json"
