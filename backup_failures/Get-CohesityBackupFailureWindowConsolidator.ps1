<#
.SYNOPSIS
  Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
  GET-only Cohesity Helios evidence tool for backup-failure incidents.
  The script locks one incident to the Dynatrace compute_window:
  America/New_York, 18:00 ET -> next day 18:00 ET.

  First run in a new DT window asks once for the incident number.
  Later runs in the same DT window reuse BackupFailure_WindowRegistry.json.
#>

[CmdletBinding()]
param(
    [string]$HeliosBaseUrl = 'https://helios.cohesity.com',
    [string]$ApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow',
    [string]$IncidentNumber,
    [int]$MaxClusters = 0,
    [int]$MaxProtectionGroupsPerCluster = 0,
    [int]$MaxRunsPerProtectionGroup = 120,
    [bool]$ShowGridView = $true,
    [switch]$MultipleGridViews
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

function Get-Etz { try { [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') } catch { [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') } }
function Get-NowEt { [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), (Get-Etz)) }
function FmtEt($d) { if ($null -eq $d -or [string]::IsNullOrWhiteSpace([string]$d)) { '' } else { ([datetime]$d).ToString('yyyy-MM-dd HH:mm:ss') } }
function FmtUtc([datetime]$d) { $d.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss') }
function EtToUtc([datetime]$d) { [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($d,[DateTimeKind]::Unspecified),(Get-Etz)) }
function UsecToEt($u) { if (-not $u) { return $null }; $epoch=[datetime]'1970-01-01T00:00:00Z'; [TimeZoneInfo]::ConvertTimeFromUtc($epoch.AddSeconds(([double]$u)/1000000),(Get-Etz)) }
function Arr($x) { if ($null -eq $x) { @() } elseif ($x -is [array]) { @($x) } else { @($x) } }
function Prop($o,[string[]]$names) { if ($null -eq $o) { return $null }; foreach($n in $names){ if($o.PSObject.Properties.Name -contains $n){ return $o.$n } }; $null }
function FirstArr($o,[string[]]$names) { foreach($n in $names){ $v=Prop $o @($n); if($null -ne $v){ return @(Arr $v) } }; @() }
function SafeName([string]$s) { if([string]::IsNullOrWhiteSpace($s)){'Unknown'}else{(($s.Trim() -replace '[\\/:*?"<>|]','_') -replace '\s+','_')} }

function Get-DtWindow {
    $now = Get-NowEt
    $start = $now.Date.AddHours(18)
    if ($now -lt $start) { $start = $start.AddDays(-1) }
    $end = $start.AddDays(1)
    [pscustomobject]@{
        StartET = $start
        EndET = $end
        WindowKey = ('{0}_1800ET' -f $start.ToString('yyyy-MM-dd'))
        WindowLabel = ('{0} ET -> {1} ET' -f $start.ToString('yyyy-MM-dd HH:mm'), $end.ToString('yyyy-MM-dd HH:mm'))
        SnStartUtc = FmtUtc (EtToUtc $start)
        SnEndUtc = FmtUtc (EtToUtc $end)
        Source = 'Dynatrace_compute_window'
    }
}

function Read-Json($path,$default) { if(Test-Path $path){ $raw=Get-Content $path -Raw; if(-not [string]::IsNullOrWhiteSpace($raw)){ return ($raw | ConvertFrom-Json) } }; $default }
function Write-Json($obj,$path) { $dir=Split-Path $path -Parent; if(-not(Test-Path $dir)){New-Item -ItemType Directory -Path $dir -Force|Out-Null}; $obj|ConvertTo-Json -Depth 50|Set-Content $path -Encoding UTF8 }

function Resolve-Window($window) {
    if(-not(Test-Path $OutputRoot)){New-Item -ItemType Directory -Path $OutputRoot -Force|Out-Null}
    $registryPath = Join-Path $OutputRoot 'BackupFailure_WindowRegistry.json'
    $default = [pscustomobject]@{ TimeZone='America/New_York'; WindowMode='DynatraceDaily18ET'; WindowDurationHours=24; WindowStartHourET=18; Windows=[pscustomobject]@{} }
    $reg = Read-Json $registryPath $default
    if(-not($reg.PSObject.Properties.Name -contains 'Windows')){ $reg | Add-Member -MemberType NoteProperty -Name Windows -Value ([pscustomobject]@{}) }
    $key = $window.WindowKey

    if($reg.Windows.PSObject.Properties.Name -contains $key){
        $m = $reg.Windows.$key
        if($IncidentNumber -and $IncidentNumber.Trim().ToUpper() -ne $m.IncidentNumber){ throw "Window $key is locked to $($m.IncidentNumber). Do not overwrite the locked incident." }
        $m.LastRunET = FmtEt (Get-NowEt)
        Write-Json $reg $registryPath
        return [pscustomobject]@{ RegistryPath=$registryPath; WindowKey=$key; Mapping=$m; IsNew=$false }
    }

    $inc = $IncidentNumber
    if(-not $inc){ Write-Host "New Dynatrace compute window detected: $($window.WindowLabel)" -ForegroundColor Yellow; $inc = Read-Host 'Enter incident number for this window' }
    if([string]::IsNullOrWhiteSpace($inc)){ throw 'Incident number is required for a new DT compute window.' }
    $inc = $inc.Trim().ToUpper()
    $folder = Join-Path $OutputRoot (SafeName $inc)
    if(-not(Test-Path $folder)){New-Item -ItemType Directory -Path $folder -Force|Out-Null}

    $m = [pscustomobject]@{
        IncidentNumber=$inc; WindowKey=$key; WindowLabel=$window.WindowLabel; WindowStartET=FmtEt $window.StartET; WindowEndET=FmtEt $window.EndET;
        SnStartUtc=$window.SnStartUtc; SnEndUtc=$window.SnEndUtc; WindowLocked=$true; WindowSource=$window.Source;
        FirstRunET=FmtEt (Get-NowEt); LastRunET=FmtEt (Get-NowEt); SnowSysId=''; SnowWorkNotesReadEnabled=$false; OutputFolder=$folder
    }
    $reg.Windows | Add-Member -MemberType NoteProperty -Name $key -Value $m
    Write-Json $reg $registryPath
    [pscustomobject]@{ RegistryPath=$registryPath; WindowKey=$key; Mapping=$m; IsNew=$true }
}

function ApiKey { if(-not(Test-Path $ApiKeyPath)){throw "API key file not found: $ApiKeyPath"}; $k=(Get-Content $ApiKeyPath -Raw).Trim(); if(-not $k){throw 'API key file is empty'}; $k }
function CohesityGet($path,$headers,$query) {
    $uri = $HeliosBaseUrl.TrimEnd('/') + $path
    if($query -and $query.Count){
        $pairs = foreach($k in $query.Keys){ if($null -ne $query[$k] -and [string]$query[$k] -ne ''){ '{0}={1}' -f [uri]::EscapeDataString([string]$k), [uri]::EscapeDataString([string]$query[$k]) } }
        if($pairs){ $uri += '?' + ($pairs -join '&') }
    }
    Invoke-RestMethod -Method GET -Uri $uri -Headers $headers -TimeoutSec 120
}
function NormStatus($s){ if(-not $s){'Unknown'}else{ $x=([string]$s -replace '^k',''); switch -Regex($x){'Succeeded|Success|Warning'{'Succeeded';break}'Fail|Error'{'Failed';break}'Cancel'{'Canceled';break}'Running|Started|Progress|Accepted'{'Running';break}default{$x}} } }
function NormEnv($s){ if(-not $s){'Unknown'}else{(([string]$s -replace '^k','') -replace 'Acropolis','Nutanix' -replace 'GenericNas','NAS')} }
function RunStatus($r){ $s=Prop $r @('status','backupRunStatus','runStatus'); if(-not $s){$s=Prop (Prop $r @('localBackupInfo')) @('status')}; NormStatus $s }
function RunType($r){ $t=Prop $r @('runType','backupRunType'); if(-not $t){$t=Prop (Prop $r @('localBackupInfo')) @('runType')}; if(-not $t){'Unknown'}else{([string]$t -replace '^k','')} }
function RunStart($r){ $u=Prop $r @('startTimeUsecs','runStartTimeUsecs'); if(-not $u){$u=Prop (Prop $r @('localBackupInfo')) @('startTimeUsecs')}; UsecToEt $u }
function RunEnd($r){ $u=Prop $r @('endTimeUsecs','endUsecs','runEndTimeUsecs'); if(-not $u){$u=Prop (Prop $r @('localBackupInfo')) @('endTimeUsecs')}; if(-not $u){$u=Prop $r @('startTimeUsecs','runStartTimeUsecs')}; UsecToEt $u }
function Msg($o){ foreach($f in @('errorMessage','message','errorMsg','failureMessage','warningMessage','reason')){ $v=Prop $o @($f); if($v){return [string]$v} }; '' }
function ObjName($o){ $x=Prop $o @('object','entity','source'); $n=Prop $o @('name','objectName','displayName'); if(-not $n){$n=Prop $x @('name','objectName','displayName')}; if($n){[string]$n}else{'UnknownObject'} }
function ObjId($o){ $x=Prop $o @('object','entity','source'); $id=Prop $o @('id','objectId','entityId','sourceId','uid'); if(-not $id){$id=Prop $x @('id','objectId','entityId','sourceId','uid')}; [string]$id }
function ObjType($o,$env){ $x=Prop $o @('object','entity','source'); $t=Prop $o @('type','objectType','entityType'); if(-not $t){$t=Prop $x @('type','objectType','entityType')}; if($t){([string]$t -replace '^k','')}else{$env} }
function HostName($o){ $h=Prop $o @('host','hostName','parentName','sourceName','registeredSourceName'); if($h){return [string]$h}; $x=Prop $o @('object','entity','source'); $h=Prop $x @('parentName','hostName','sourceName','registeredSourceName'); if($h){[string]$h}else{''} }
function ObjStatus($o,$runStatus){ $s=Prop $o @('status','runStatus','protectionStatus','backupStatus'); if(-not $s){$s=$runStatus}; NormStatus $s }
function ObjKey($clusterId,$env,$pgId,$pgName,$o){ $id=ObjId $o; if($id){"$clusterId|$env|$pgId|$id"}else{"$clusterId|$env|$pgName|$(HostName $o)|$(ObjName $o)"} }

function Get-Clusters($headers){
    $j=CohesityGet '/v2/mcm/cluster-mgmt/info' $headers @{}
    $out = foreach($c in (FirstArr $j @('clusters','clusterInfo','clusterInfos','items','data'))){ $id=Prop $c @('clusterId','id','uuid'); $name=Prop $c @('clusterName','name','displayName','hostname'); if($id -or $name){[pscustomobject]@{ClusterId=[string]$id;ClusterName=[string]$name}} }
    if($MaxClusters -gt 0){@($out|Select-Object -First $MaxClusters)}else{@($out)}
}
function Get-PGs($headers){ $j=CohesityGet '/v2/data-protect/protection-groups' $headers @{isDeleted='false';isActive='true';includeLastRunInfo='true'}; $p=FirstArr $j @('protectionGroups','protectionGroupInfos','items','data'); if($MaxProtectionGroupsPerCluster -gt 0){@($p|Select-Object -First $MaxProtectionGroupsPerCluster)}else{@($p)} }
function Get-Runs($headers,$pgId){ $e=[uri]::EscapeDataString([string]$pgId); $j=CohesityGet "/v2/data-protect/protection-groups/$e/runs" $headers @{numRuns=[string]$MaxRunsPerProtectionGroup;includeObjectDetails='true'}; FirstArr $j @('runs','protectionRuns','items','data') }

function New-Ev($inc,$time,$cid,$cl,$env,$pgid,$pg,$host,$oname,$otype,$oid,$okey,$rtype,$etype,$msg,$rs,$re){[pscustomobject]@{IncidentNumber=$inc;EventTimeET=FmtEt $time;ClusterId=$cid;Cluster=$cl;Environment=$env;ProtectionGroupId=$pgid;ProtectionGroup=$pg;Host=$host;ObjectName=$oname;ObjectType=$otype;ObjectId=$oid;ObjectKey=$okey;RunType=$rtype;EventType=$etype;Message=$msg;RunStartET=FmtEt $rs;RunEndET=FmtEt $re}}

function Collect($headers,$window,$inc){
    $events=@(); $evidence=@(); $warnings=@()
    foreach($cluster in (Get-Clusters $headers)){
        $cid=$cluster.ClusterId; $cl=if($cluster.ClusterName){$cluster.ClusterName}else{$cid}; $h=@{}; foreach($k in $headers.Keys){$h[$k]=$headers[$k]}; if($cid){$h['accessClusterId']=$cid}
        try{$pgs=Get-PGs $h}catch{$warnings+="Cluster $cl PG query failed: $($_.Exception.Message)"; continue}
        foreach($pg in $pgs){
            $pgid=[string](Prop $pg @('id','protectionGroupId','uid')); if(-not $pgid){$pgid=[string](Prop $pg @('name','protectionGroupName'))}
            $pgn=[string](Prop $pg @('name','protectionGroupName')); if(-not $pgn){$pgn=$pgid}
            $env=NormEnv (Prop $pg @('environment','env','protectionSourceEnvironment'))
            try{$runs=Get-Runs $h $pgid}catch{$warnings+="Cluster $cl PG $pgn run query failed: $($_.Exception.Message)"; continue}
            $oldest=$null
            foreach($r in $runs){
                $rs=RunStart $r; $re=RunEnd $r; $t=if($re){$re}else{$rs}; if(-not $t){continue}; if(-not $oldest -or $t -lt $oldest){$oldest=$t}
                if($t -lt $window.StartET -or $t -ge $window.EndET){continue}
                $rst=RunStatus $r; $rt=RunType $r; $objs=FirstArr $r @('objects','objectRuns','objectRunList','tasks','taskRuns')
                $evidence += [pscustomobject]@{IncidentNumber=$inc;Cluster=$cl;Environment=$env;ProtectionGroup=$pgn;RunType=$rt;RunStatus=$rst;RunStartET=FmtEt $rs;RunEndET=FmtEt $re;ObjectDetailCount=$objs.Count;Message=Msg $r}
                if($objs.Count -eq 0){ if($rst -in @('Failed','Canceled','Running')){ $etype=if($rst -eq 'Failed'){'Failed'}elseif($rst -eq 'Canceled'){'CancelledRun'}else{'RunningRun'}; $events+=New-Ev $inc $t $cid $cl $env $pgid $pgn '' $pgn 'ProtectionGroup' '' "$cid|$env|$pgid|PG_LEVEL|$rt" $rt $etype (Msg $r) $rs $re }; continue }
                foreach($o in $objs){
                    $ost=ObjStatus $o $rst; $etype=$null
                    if($ost -eq 'Running' -or $rst -eq 'Running'){$etype='RunningRun'}elseif($ost -eq 'Canceled' -or $rst -eq 'Canceled'){$etype='CancelledRun'}elseif($ost -eq 'Failed' -or $rst -eq 'Failed'){$etype='Failed'}elseif($ost -eq 'Succeeded' -or $rst -eq 'Succeeded'){$etype='Succeeded'}
                    if(-not $etype){continue}
                    $oid=ObjId $o; $ok=ObjKey $cid $env $pgid $pgn $o; $m=Msg $o; if(-not $m){$m=Msg $r}
                    $events+=New-Ev $inc $t $cid $cl $env $pgid $pgn (HostName $o) (ObjName $o) (ObjType $o $env) $oid $ok $rt $etype $m $rs $re
                }
            }
            if($oldest -and $oldest -gt $window.StartET){$warnings+="PG $pgn on $cl may be truncated; increase MaxRunsPerProtectionGroup."}
        }
    }
    [pscustomobject]@{Events=$events;RunEvidence=$evidence;Warnings=$warnings}
}

function Row($e,$section,$status,$ff,$lf,$rec,$cnt){[pscustomobject]@{Section=$section;Status=$status;IncidentNumber=$e.IncidentNumber;Cluster=$e.Cluster;Environment=$e.Environment;ProtectionGroup=$e.ProtectionGroup;Host=$e.Host;ObjectName=$e.ObjectName;ObjectType=$e.ObjectType;RunType=$e.RunType;FirstFailedET=$ff;LastFailedET=$lf;RecoveredET=$rec;ConsecutiveFailureCount=$cnt;Message=$e.Message;ObjectKey=$e.ObjectKey}}
function Build($events,$prev,$window,$inc,$evidence,$warnings){
    $prevFail=@{}; if($prev -and ($prev.PSObject.Properties.Name -contains 'Objects')){foreach($p in @($prev.Objects)){if($p.CurrentStatus -in @('StillFailing','ReFailed')){$prevFail[[string]$p.ObjectKey]=$p}}}
    $by=@{}; foreach($e in @($events|Sort-Object EventTimeET)){if(-not $by.ContainsKey($e.ObjectKey)){$by[$e.ObjectKey]=@()}; $by[$e.ObjectKey]+=$e}
    $cur=@();$rec=@();$newF=@();$newR=@();$con=@();$run=@();$can=@();$state=@()
    foreach($k in $by.Keys){$evs=@($by[$k]|Sort-Object EventTimeET);$fails=@($evs|Where-Object EventType -eq 'Failed');$succ=@($evs|Where-Object EventType -eq 'Succeeded');foreach($r in @($evs|Where-Object EventType -eq 'RunningRun')){$run+=Row $r 'Running Run' 'RunningAtLatestCheck' '' '' '' 0};foreach($c in @($evs|Where-Object EventType -eq 'CancelledRun')){$can+=Row $c 'Cancelled Run' 'CancelledInWindow' '' $c.EventTimeET '' 0};if($fails.Count -eq 0){continue};$ff=$fails|Select-Object -First 1;$lf=$fails|Select-Object -Last 1;$before=$succ|Where-Object{[datetime]$_.EventTimeET -lt [datetime]$lf.EventTimeET}|Select-Object -Last 1;$after=$succ|Where-Object{[datetime]$_.EventTimeET -gt [datetime]$lf.EventTimeET}|Select-Object -First 1;$cnt=if($before){@($fails|Where-Object{[datetime]$_.EventTimeET -gt [datetime]$before.EventTimeET}).Count}else{$fails.Count};if($after){$rec+=Row $lf 'Recovered In Window' 'RecoveredInWindow' $ff.EventTimeET $lf.EventTimeET $after.EventTimeET $cnt;if($prevFail.ContainsKey($k)){$newR+=Row $lf 'New Recovery' 'NewlyRecoveredThisCheck' $ff.EventTimeET $lf.EventTimeET $after.EventTimeET $cnt};$state+=[pscustomobject]@{ObjectKey=$k;Cluster=$lf.Cluster;Environment=$lf.Environment;ProtectionGroup=$lf.ProtectionGroup;Host=$lf.Host;ObjectName=$lf.ObjectName;ObjectType=$lf.ObjectType;RunType=$lf.RunType;CurrentStatus='RecoveredInWindow';FirstFailedET=$ff.EventTimeET;LastFailedET=$lf.EventTimeET;RecoveredET=$after.EventTimeET;ConsecutiveFailureCount=$cnt;LastMessage=$lf.Message}}else{$st=if($before){'ReFailed'}else{'StillFailing'};$cur+=Row $lf 'Current Still Failing' $st $ff.EventTimeET $lf.EventTimeET '' $cnt;if(-not $prevFail.ContainsKey($k)){$newF+=Row $lf 'New Failure' 'NewlyFailedThisCheck' $ff.EventTimeET $lf.EventTimeET '' $cnt};if($cnt -gt 1){$con+=Row $lf 'Consecutive Failure' 'ConsecutiveFailure' $ff.EventTimeET $lf.EventTimeET '' $cnt};$state+=[pscustomobject]@{ObjectKey=$k;Cluster=$lf.Cluster;Environment=$lf.Environment;ProtectionGroup=$lf.ProtectionGroup;Host=$lf.Host;ObjectName=$lf.ObjectName;ObjectType=$lf.ObjectType;RunType=$lf.RunType;CurrentStatus=$st;FirstFailedET=$ff.EventTimeET;LastFailedET=$lf.EventTimeET;RecoveredET='';ConsecutiveFailureCount=$cnt;LastMessage=$lf.Message}}}
    $failedKeys=@($events|Where-Object EventType -eq 'Failed'|Select-Object -ExpandProperty ObjectKey -Unique); $clusters=@($events|Where-Object Cluster|Select-Object -ExpandProperty Cluster -Unique); $envs=@($events|Where-Object Environment|Select-Object -ExpandProperty Environment -Unique); $pgs=@($events|Where-Object ProtectionGroup|Select-Object -ExpandProperty ProtectionGroup -Unique)
    $summary=@([pscustomobject]@{Metric='IncidentNumber';Value=$inc},[pscustomobject]@{Metric='WindowKey';Value=$window.WindowKey},[pscustomobject]@{Metric='WindowLabel';Value=$window.WindowLabel},[pscustomobject]@{Metric='WindowStartET';Value=FmtEt $window.StartET},[pscustomobject]@{Metric='WindowEndET';Value=FmtEt $window.EndET},[pscustomobject]@{Metric='SnStartUtc';Value=$window.SnStartUtc},[pscustomobject]@{Metric='SnEndUtc';Value=$window.SnEndUtc},[pscustomobject]@{Metric='GeneratedAtET';Value=FmtEt (Get-NowEt)},[pscustomobject]@{Metric='TotalUniqueObjectsFailedInWindow';Value=$failedKeys.Count},[pscustomobject]@{Metric='RecoveredInWindow';Value=$rec.Count},[pscustomobject]@{Metric='StillFailingAtLatestCheck';Value=$cur.Count},[pscustomobject]@{Metric='NewFailuresSincePreviousRun';Value=$newF.Count},[pscustomobject]@{Metric='NewRecoveriesSincePreviousRun';Value=$newR.Count},[pscustomobject]@{Metric='ConsecutiveRepeatedFailures';Value=$con.Count},[pscustomobject]@{Metric='RunningRunsSeen';Value=$run.Count},[pscustomobject]@{Metric='CancelledRunsSeen';Value=$can.Count},[pscustomobject]@{Metric='ImpactedClusters';Value=$clusters.Count},[pscustomobject]@{Metric='ImpactedEnvironments';Value=($envs -join '; ')},[pscustomobject]@{Metric='ImpactedProtectionGroups';Value=$pgs.Count},[pscustomobject]@{Metric='WarningCount';Value=$warnings.Count})
    [pscustomobject]@{Summary=$summary;CurrentFailing=$cur;Recovered=$rec;NewFailures=$newF;NewRecoveries=$newR;Consecutive=$con;CarryForward=$cur;EventHistory=@($events|Sort-Object EventTimeET);RunEvidence=$evidence;QuickView=@($cur+$rec+$newF+$newR+$con+$run+$can);ObjectState=$state;Warnings=$warnings}
}

function Export-Xlsx($path,[ordered]$sheets){
    if(Test-Path $path){Remove-Item $path -Force}
    if(Get-Command Export-Excel -ErrorAction SilentlyContinue){$first=$true;foreach($n in $sheets.Keys){$rows=@($sheets[$n]);if($rows.Count -eq 0){$rows=@([pscustomobject]@{Info='No rows'})};$p=@{Path=$path;WorksheetName=($n.Substring(0,[Math]::Min(31,$n.Length)));AutoSize=$true;FreezeTopRow=$true;BoldTopRow=$true};if(-not $first){$p.Append=$true};$rows|Export-Excel @p;$first=$false};return}
    $excel=$null;try{$excel=New-Object -ComObject Excel.Application;$excel.Visible=$false;$excel.DisplayAlerts=$false;$wb=$excel.Workbooks.Add();while($wb.Worksheets.Count -gt 1){$wb.Worksheets.Item(1).Delete()};$i=0;foreach($n in $sheets.Keys){$i++;$ws=if($i -eq 1){$wb.Worksheets.Item(1)}else{$wb.Worksheets.Add([Type]::Missing,$wb.Worksheets.Item($wb.Worksheets.Count))};$ws.Name=$n.Substring(0,[Math]::Min(31,$n.Length));$rows=@($sheets[$n]);if($rows.Count -eq 0){$rows=@([pscustomobject]@{Info='No rows'})};$heads=@($rows[0].PSObject.Properties.Name);for($c=0;$c -lt $heads.Count;$c++){$ws.Cells.Item(1,$c+1).Value2=$heads[$c];$ws.Cells.Item(1,$c+1).Font.Bold=$true};for($r=0;$r -lt $rows.Count;$r++){for($c=0;$c -lt $heads.Count;$c++){$ws.Cells.Item($r+2,$c+1).Value2=[string]$rows[$r].PSObject.Properties[$heads[$c]].Value}};$ws.Columns.AutoFit()|Out-Null};$wb.SaveAs($path,51);$wb.Close($true)}catch{throw "XLSX export failed. Install ImportExcel or run where Excel is installed. $($_.Exception.Message)"}finally{if($excel){$excel.Quit()|Out-Null}}
}

function WorkNotes($t,$w,$inc,$wb){$h=@{};foreach($r in $t.Summary){$h[$r.Metric]=$r.Value};@('Backup Failure Window Summary','',"Incident: $inc","Locked Compute Window: $($w.WindowLabel)","SNOW Compare UTC: $($w.SnStartUtc) to $($w.SnEndUtc)","Generated At: $(FmtEt (Get-NowEt)) ET",'Source: Cohesity Helios API / PowerShell Window Consolidator','', 'Summary:',"- Total unique objects failed in this window: $($h['TotalUniqueObjectsFailedInWindow'])","- Recovered within this window: $($h['RecoveredInWindow'])","- Still failing at latest check within this window: $($h['StillFailingAtLatestCheck'])","- New failures since previous check: $($h['NewFailuresSincePreviousRun'])","- New recoveries since previous check: $($h['NewRecoveriesSincePreviousRun'])","- Consecutive/repeated failures: $($h['ConsecutiveRepeatedFailures'])","- Running backup runs seen: $($h['RunningRunsSeen'])","- Cancelled backup runs seen: $($h['CancelledRunsSeen'])","- Impacted clusters: $($h['ImpactedClusters'])","- Impacted environments: $($h['ImpactedEnvironments'])","- Impacted protection groups: $($h['ImpactedProtectionGroups'])",'','Current Still Failing: See workbook tab 02_Current_Still_Failing','Recovered During Window: See workbook tab 03_Recovered_In_Window','Consecutive / Repeated Failures: See workbook tab 06_Consecutive_Failures','Carry Forward Baseline: See workbook tab 07_Carry_Forward_Baseline','','Note: Running runs are listed separately and are not treated as failed or recovered until they complete.','',"Attachment: $wb") -join [Environment]::NewLine}

try{
    $window=Get-DtWindow; $map=Resolve-Window $window; $inc=$map.Mapping.IncidentNumber; $folder=$map.Mapping.OutputFolder
    $statePath=Join-Path $folder ("{0}_State.json" -f (SafeName $inc)); $prev=Read-Json $statePath $null
    $headers=@{accept='application/json';apiKey=(ApiKey)}
    $coll=Collect $headers $window $inc; $tables=Build $coll.Events $prev $window $inc $coll.RunEvidence $coll.Warnings
    $runStatus=@([pscustomobject]@{Field='ScriptResult';Value='Success'},[pscustomobject]@{Field='IncidentNumber';Value=$inc},[pscustomobject]@{Field='WindowKey';Value=$window.WindowKey},[pscustomobject]@{Field='WindowLabel';Value=$window.WindowLabel},[pscustomobject]@{Field='SnStartUtc';Value=$window.SnStartUtc},[pscustomobject]@{Field='SnEndUtc';Value=$window.SnEndUtc},[pscustomobject]@{Field='ProductionApiMode';Value='GET-only'},[pscustomobject]@{Field='WarningCount';Value=$coll.Warnings.Count})
    $metadata=@([pscustomobject]@{Field='HeliosBaseUrl';Value=$HeliosBaseUrl},[pscustomobject]@{Field='ApiKeyPath';Value=$ApiKeyPath},[pscustomobject]@{Field='OutputRoot';Value=$OutputRoot},[pscustomobject]@{Field='RegistryPath';Value=$map.RegistryPath},[pscustomobject]@{Field='MaxRunsPerProtectionGroup';Value=$MaxRunsPerProtectionGroup})
    $warn=@($coll.Warnings|ForEach-Object{[pscustomobject]@{Warning=$_}});if($warn.Count -eq 0){$warn=@([pscustomobject]@{Warning='No warnings'})}
    $xlsx=Join-Path $folder ("{0}_BackupFailure_WindowSummary.xlsx" -f (SafeName $inc)); $txt=Join-Path $folder ("{0}_WorkNotes_Paste.txt" -f (SafeName $inc))
    $sheets=[ordered]@{'00_Run_Status'=$runStatus;'01_Summary'=$tables.Summary;'02_Current_Still_Failing'=$tables.CurrentFailing;'03_Recovered_In_Window'=$tables.Recovered;'04_New_Failures_Latest'=$tables.NewFailures;'05_New_Recoveries_Latest'=$tables.NewRecoveries;'06_Consecutive_Failures'=$tables.Consecutive;'07_Carry_Forward_Baseline'=$tables.CarryForward;'08_Event_History'=$tables.EventHistory;'09_Run_Evidence'=$tables.RunEvidence;'10_Metadata'=$metadata;'11_Warnings'=$warn}
    Export-Xlsx $xlsx $sheets; WorkNotes $tables $window $inc (Split-Path $xlsx -Leaf)|Set-Content $txt -Encoding UTF8
    Write-Json ([pscustomobject]@{IncidentNumber=$inc;WindowKey=$window.WindowKey;WindowLabel=$window.WindowLabel;SnStartUtc=$window.SnStartUtc;SnEndUtc=$window.SnEndUtc;WindowLocked=$true;WindowSource=$window.Source;LastRunET=FmtEt(Get-NowEt);Objects=$tables.ObjectState;Summary=$tables.Summary;WorkbookPath=$xlsx;WorkNotesPath=$txt}) $statePath
    $h=@{};foreach($r in $tables.Summary){$h[$r.Metric]=$r.Value};Write-Host "`nIncident: $inc" -ForegroundColor Cyan;Write-Host "Window  : $($window.WindowLabel)`n";Write-Host 'Summary:' -ForegroundColor Cyan;Write-Host "Total Failed In Window       : $($h['TotalUniqueObjectsFailedInWindow'])";Write-Host "Recovered In Window          : $($h['RecoveredInWindow'])";Write-Host "Still Failing Now            : $($h['StillFailingAtLatestCheck'])";Write-Host "New Failures Since Last Run  : $($h['NewFailuresSincePreviousRun'])";Write-Host "New Recoveries Since Last Run: $($h['NewRecoveriesSincePreviousRun'])";Write-Host "Consecutive Failures         : $($h['ConsecutiveRepeatedFailures'])";Write-Host "Running Runs Seen            : $($h['RunningRunsSeen'])";Write-Host "Cancelled Runs Seen          : $($h['CancelledRunsSeen'])`n";if($ShowGridView -and (Get-Command Out-GridView -ErrorAction SilentlyContinue)){if($MultipleGridViews){$tables.CurrentFailing|Out-GridView -Title "$inc - Current Still Failing";$tables.Recovered|Out-GridView -Title "$inc - Recovered In Window";$tables.Consecutive|Out-GridView -Title "$inc - Consecutive Failures"}else{$tables.QuickView|Out-GridView -Title "$inc - Backup Failure Window Quick View"}}else{$tables.QuickView|Select-Object Section,Cluster,Environment,ProtectionGroup,ObjectName,RunType,Status,LastFailedET,RecoveredET,ConsecutiveFailureCount|Format-Table -AutoSize};Write-Host 'Files Created:' -ForegroundColor Cyan;Write-Host $xlsx;Write-Host $txt;Write-Host $statePath;Write-Host '';Write-Host 'Next Step: Attach XLSX to incident and paste WorkNotes_Paste.txt into work_notes.' -ForegroundColor Yellow
}catch{Write-Host '';Write-Host 'SCRIPT RESULT: FAILED' -ForegroundColor Red;Write-Host $_.Exception.Message -ForegroundColor Red;throw}
