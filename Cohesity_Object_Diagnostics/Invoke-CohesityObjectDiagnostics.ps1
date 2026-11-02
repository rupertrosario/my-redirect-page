<#
.SYNOPSIS
  Cohesity object diagnostics - seven-day, GET-only Helios to offline HTML.
.DESCRIPTION
  Reuses the discovery sequence from the owner's dtsk_backup_status/cr_bkup_status.ps1:
  global object search -> candidate clusters -> cluster-scoped protected object search.
  Produces evidence-driven findings. Does NOT perform remediation or active restore tests.
  PowerShell 5.1 compatible. No modules, browser services, or external JS required.
.EXAMPLE
  .\Invoke-CohesityObjectDiagnostics.ps1 -ObjectName SQLPROD01
.EXAMPLE
  .\Invoke-CohesityObjectDiagnostics.ps1 -ObjectName 'SalesDB@SQLPROD01' -ClusterName PROD-01
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ObjectName,
    [string]$ClusterName = '',
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [int]$Days = 7,
    [int]$MaxRunsPerPG = 100,
    [int]$TimeoutSec = 90,
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\ObjectDiagnostics',
    [string]$HelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1',
    [string]$EncryptedFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
)
# Design conventions: Helios GET only, AES helper, accessClusterId, PowerShell 5.1.
# Object matching follows the existing DTSK backup validation discovery sequence.
# This script does not execute remediation or certify restore recoverability.
$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
if ($Days -lt 1 -or $Days -gt 7) { throw 'Days must be 1-7.' }
if ($MaxRunsPerPG -lt 1 -or $MaxRunsPerPG -gt 1000) { throw 'MaxRunsPerPG must be 1-1000.' }
$BaseUrl = $BaseUrl.TrimEnd('/')
$errors = New-Object 'System.Collections.Generic.List[string]'
$evidence = [ordered]@{}
function Get-PropertyValue($o, [string[]]$names, $default=$null) {
    if ($null -eq $o) { return $default }
    foreach ($name in $names) {
        $v=$o
        foreach ($part in ($name -split '\.')) {
            if ($null -eq $v) { break }
            if ($v -is [System.Collections.IDictionary]) { $v=$v[$part] }
            else { $prop=$v.PSObject.Properties[$part]; $v=if ($null -ne $prop) {$prop.Value} else {$null} }
        }
        if ($null -ne $v -and -not ($v -is [string] -and [string]::IsNullOrWhiteSpace($v))) { return $v }
    }
    return $default
}
function ConvertTo-ObjectArray($v) { if ($null -eq $v) { return @() }; return @($v) }
function Get-FirstNonBlank($v) { foreach($x in (ConvertTo-ObjectArray $v)) { if ($null -ne $x -and "$x".Trim()) {return "$x".Trim()} }; return '' }
function Normalize-ObjectName([string]$s) { return $s.Trim().TrimEnd('.').ToLowerInvariant() }
function Get-ShortName([string]$s) { if ($s -match '@') {$s=($s -split '@',2)[1]}; return ($s -split '\.')[0] }
function Test-ObjectNameMatch([string]$a,[string]$b) {
    if (-not $a -or -not $b) {return $false}
    $a=Normalize-ObjectName $a; $b=Normalize-ObjectName $b
    return ($a -eq $b -or $a -eq (Get-ShortName $b) -or (Get-ShortName $a) -eq (Get-ShortName $b))
}
function Convert-UsecsToUtc($usecs) {
    try { if (-not $usecs -or [double]$usecs -le 0) {return $null}; return [DateTimeOffset]::FromUnixTimeMilliseconds([long]([double]$usecs / 1000)).UtcDateTime } catch {return $null}
}
function Invoke-HeliosGetJson([string]$path,[hashtable]$headers,[string]$key) {
    $uri= if ($path.StartsWith('http')) {$path} else {"$BaseUrl$path"}
    try {
        $r=Invoke-WebRequest -Method Get -Uri $uri -Headers $headers -UseBasicParsing -TimeoutSec $TimeoutSec -ErrorAction Stop
        if (-not $r.Content) { throw 'Empty response' }
        $obj=$r.Content | ConvertFrom-Json
        if ($key) {$script:evidence[$key]=$obj}
        return $obj
    } catch {
        $script:errors.Add("$key : $($_.Exception.Message)") | Out-Null
        return $null
    }
}
function Get-SearchObjects($j) {return @(ConvertTo-ObjectArray (Get-PropertyValue $j @('objects','searchResults','results','entities','items','data') @()))}
function Expand-ProtectedObject($node,[string]$parent='', [string]$environment='', [string]$SourceHost='', [int]$depth=0) {
    if (-not $node -or $depth -gt 12) { return @() }
    $name=Get-FirstNonBlank @((Get-PropertyValue $node @('name','displayName','databaseName','dbName','dbUniqueName') ''))
    $env=Get-FirstNonBlank @((Get-PropertyValue $node @('environment','sourceInfo.environment') ''),$environment)
    $type=Get-FirstNonBlank @((Get-PropertyValue $node @('objectType','type','entityType') ''))
    $sqlHost=Get-FirstNonBlank @((Get-PropertyValue $node @('mssqlParams.hostInfo.name','sqlParams.hostInfo.name','oracleParams.hostInfo.name','hostInfo.name','sourceInfo.name','sourceName','hostName','serverName') ''),$SourceHost)
    $out=@([pscustomobject]@{ Name=$name; Host=$sqlHost; Parent=$parent; Environment=$env; Type=$type; Raw=$node })
    foreach ($child in (ConvertTo-ObjectArray (Get-PropertyValue $node @('childObjects') @()))) { $out += Expand-ProtectedObject $child $name $env $sqlHost ($depth+1) }
    return $out
}
function Get-ClusterIdFromObject($node) {return Get-FirstNonBlank @((Get-PropertyValue $node @('clusterId','clusterID','accessClusterId','sourceClusterId','cluster.id','cluster.clusterId','clusterInfo.id','clusterInfo.clusterId') ''))}
function Status($s) {
    $t=("$s").ToLowerInvariant()
    if ($t -match 'fail|error|cancel') {return 'FAIL'}
    if ($t -match 'warn|partial') {return 'WARN'}
    if ($t -match 'success|succeed|complete') {return 'PASS'}
    if ($t -match 'run|accept|queue|progress') {return 'RUNNING'}
    return 'UNKNOWN'
}
function Kind($s) {
    $t=("$s").ToLowerInvariant()
    if ($t -match 'log') {return 'Log'}
    if ($t -match 'full') {return 'Full'}
    if ($t -match 'increment|incre|differential|diff') {return 'Incre'}
    return 'Data'
}
function Get-LatestSnapshot($node) {
    $s=@(ConvertTo-ObjectArray (Get-PropertyValue $node @('latestSnapshotsInfo') @()))
    $s=@($s | Sort-Object { [double](Get-PropertyValue $_ @('protectionRunStartTimeUsecs','runStartTimeUsecs','startTimeUsecs','snapshotTimestampUsecs') 0) } -Descending)
    if ($s.Count) {return $s[0]};return $null
}
function Get-ConsecutiveFailures($runs) {
    $streak=0
    foreach ($r in @($runs | Sort-Object StartUtc -Descending)) {
        if ($r.Status -eq 'FAIL') {$streak++}
        elseif ($r.Status -eq 'PASS') {break}
        else {break} # UNKNOWN/WARN/RUNNING do not establish an unbroken failure streak
    }
    return $streak
}
if (-not (Test-Path -LiteralPath $HelperPath -PathType Leaf)) {throw "Missing AES helper: $HelperPath"}
if (-not (Test-Path -LiteralPath $EncryptedFile -PathType Leaf)) {throw "Missing encrypted key: $EncryptedFile"}
. $HelperPath
$apiKey=Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
if ([string]::IsNullOrWhiteSpace($apiKey)) {throw 'AES helper returned empty API key.'}
$common=@{accept='application/json';apiKey=$apiKey}
$clustersJson=Invoke-HeliosGetJson '/v2/mcm/cluster-mgmt/info' $common 'clusters'
$clusters=@(ConvertTo-ObjectArray (Get-PropertyValue $clustersJson @('cohesityClusters','clusters') @()))
if (-not $clusters.Count) {throw 'Helios returned no clusters. See API access/response.'}
$clusterMap=@{}
foreach($c in $clusters) {
    $id=Get-FirstNonBlank @((Get-PropertyValue $c @('clusterId','id') ''))
    if ($id) {$clusterMap[$id]=Get-FirstNonBlank @((Get-PropertyValue $c @('clusterName','name','displayName') ''),$id)}
}
$terms=New-Object 'System.Collections.Generic.List[string]'
$terms.Add($ObjectName)
$parts=$ObjectName -split '@',2
if ($parts.Count -eq 2) {$terms.Add($parts[0]);$terms.Add($parts[1]);$terms.Add((Get-ShortName $parts[1]))}
else {$terms.Add((Get-ShortName $ObjectName))}
$terms=@($terms | Where-Object {$_} | Sort-Object -Unique)
$globalObjects=@()
foreach($term in $terms) {
    $q=[uri]::EscapeDataString($term)
    $j=Invoke-HeliosGetJson "/v2/data-protect/search/objects?searchString=$q&includeTenants=true&count=100" $common "search_global_$([array]::IndexOf($terms,$term))"
    $globalObjects+=Get-SearchObjects $j
}
$candidateIds=@()
foreach($g in $globalObjects) {
    $candidateIds+=Get-ClusterIdFromObject $g
    foreach($opi in (ConvertTo-ObjectArray (Get-PropertyValue $g @('objectProtectionInfos') @()))) {
        $candidateIds+=Get-ClusterIdFromObject $opi
        foreach($pg in (ConvertTo-ObjectArray (Get-PropertyValue $opi @('protectionGroups') @()))) {$candidateIds+=Get-ClusterIdFromObject $pg}
    }
    foreach($pg in (ConvertTo-ObjectArray (Get-PropertyValue $g @('protectionGroups') @()))) {$candidateIds+=Get-ClusterIdFromObject $pg}
}
$candidateIds=@($candidateIds | Where-Object {$_ -and $clusterMap.ContainsKey("$_")} | Sort-Object -Unique)
if ($ClusterName) {
    $candidateIds=@($clusterMap.Keys | Where-Object { (Test-ObjectNameMatch $clusterMap[$_] $ClusterName) -or $_ -eq $ClusterName })
    if (-not $candidateIds.Count) {throw "Cluster not found: $ClusterName"}
} elseif (-not $candidateIds.Count) {
    $candidateIds=@($clusterMap.Keys) # same all-cluster fallback as dtsk, only if discovery found none
}
Write-Host "Searching $ObjectName across $($candidateIds.Count) candidate cluster(s)..." -ForegroundColor Cyan
$matchedObjects=@();$seen=@{}
foreach($cid in $candidateIds) {
    $headers=@{accept='application/json';apiKey=$apiKey;accessClusterId=$cid}
    foreach($term in $terms) {
        $q=[uri]::EscapeDataString($term)
        $j=Invoke-HeliosGetJson "/v2/data-protect/search/protected-objects?searchString=$q" $headers "protected_${cid}_$([array]::IndexOf($terms,$term))"
        foreach($root in (Get-SearchObjects $j)) {
            foreach($entry in (Expand-ProtectedObject $root)) {
                if (-not $entry.Name) {continue}
                $target=if ($parts.Count -eq 2) {$parts[0]} else {$ObjectName}
                $server=if ($parts.Count -eq 2) {$parts[1]} else {$ObjectName}
                if ($parts.Count -eq 2) {
                    # A database@server lookup must match both database and owning host.
                    if (-not ((Test-ObjectNameMatch $entry.Name $target) -and (Test-ObjectNameMatch $entry.Host $server))) { continue }
                }
                elseif (-not ((Test-ObjectNameMatch $entry.Name $target) -or (Test-ObjectNameMatch $entry.Host $server) -or (Test-ObjectNameMatch $entry.Parent $target))) {
                    continue
                }
                $raw=$entry.Raw
                $objId=Get-FirstNonBlank @((Get-PropertyValue $raw @('id','objectId','entityId') ''))
                $snap=Get-LatestSnapshot $raw
                $pgName=Get-FirstNonBlank @((Get-PropertyValue $snap @('protectionGroupName','protectionGroup.name','protectionGroupInfo.name') ''),(Get-PropertyValue $raw @('protectionGroupName','protectionGroup.name','protectionGroupInfo.name') ''))
                $pgId=Get-FirstNonBlank @((Get-PropertyValue $snap @('protectionGroupId','protectionGroup.id','protectionGroupInfo.id') ''),(Get-PropertyValue $raw @('protectionGroupId','protectionGroup.id','protectionGroupInfo.id') ''))
                $identity="$cid|$objId|$($entry.Name)|$($entry.Host)|$pgName"
                if ($seen.ContainsKey($identity)) {continue};$seen[$identity]=$true
                $matchedObjects+= [pscustomobject]@{ClusterId=$cid;Cluster=$clusterMap[$cid];Name=$entry.Name;Host=$entry.Host;Parent=$entry.Parent;Environment=$entry.Environment;Type=$entry.Type;ObjectId=$objId;PG=$pgName;PGId=$pgId;Raw=$raw}
            }
        }
    }
}
$cutoff=(Get-Date).ToUniversalTime().Date.AddDays(-($Days-1))
$now=(Get-Date).ToUniversalTime()
$allRecords=@();$profiles=@();$pgCache=@{};$sourceCache=@{}
foreach($m in $matchedObjects) {
    $headers=@{accept='application/json';apiKey=$apiKey;accessClusterId=$m.ClusterId}
    $pgId=$m.PGId
    $policy='Unknown'
    $domain='Unknown'
    if (-not $pgId -and $m.PG) {
        $pk="PGS_$($m.ClusterId)"
        if (-not $pgCache.ContainsKey($pk)) {
            $pgCache[$pk]=Invoke-HeliosGetJson '/v2/data-protect/protection-groups?isDeleted=false&includeTenants=true' $headers $pk
        }
        foreach($pg in (ConvertTo-ObjectArray (Get-PropertyValue $pgCache[$pk] @('protectionGroups') @()))) {
            if ((Get-PropertyValue $pg @('name') '') -eq $m.PG) {$pgId=Get-FirstNonBlank @((Get-PropertyValue $pg @('id') ''));break}
        }
    }
    # Registration is supplementary evidence; do not infer source health from protected-object search.
    $sourceName=Get-FirstNonBlank @($m.Host,$m.Name)
    $sourceState='UNKNOWN - not established'
    $refreshState='UNKNOWN - not established'
    $authState='UNKNOWN - not established'
    $refreshAt='Unknown'
    if (-not $sourceCache.ContainsKey($m.ClusterId)) {
        $sourceCache[$m.ClusterId]=Invoke-HeliosGetJson '/irisservices/api/v1/public/protectionSources/registrationInfo?includeApplicationsTreeInfo=false' $headers "SOURCES_$($m.ClusterId)"
    }
    $sourceRoots=@(ConvertTo-ObjectArray (Get-PropertyValue $sourceCache[$m.ClusterId] @('rootNodes') @()))
    foreach($src in $sourceRoots) {
        $root=Get-PropertyValue $src @('rootNode') $null
        $reg=Get-PropertyValue $src @('registrationInfo') $null
        $registeredName=Get-FirstNonBlank @((Get-PropertyValue $root @('name','displayName','physicalParams.hostName','sourceInfo.name') ''),(Get-PropertyValue $src @('name') ''))
        if (-not (Test-ObjectNameMatch $registeredName $sourceName)) {continue}
        $sourceState='Registered source matched'
        $refreshError=Get-FirstNonBlank @((Get-PropertyValue $reg @('refreshErrorMessage') ''))
        $refreshState=if ($refreshError) {"Refresh error: $refreshError"} else {'UNKNOWN - no explicit refresh result'}
        $authState=Get-FirstNonBlank @((Get-PropertyValue $reg @('authenticationStatus','authenticationErrorMessage') ''),'Unknown')
        $refreshTime=Convert-UsecsToUtc (Get-PropertyValue $reg @('refreshTimeUsecs') 0)
        if($refreshTime){$refreshAt=$refreshTime.ToString('o')}
        break
    }
    if ($pgId) {
        $pgKey="PG_$($m.ClusterId)_$pgId"
        if (-not $pgCache.ContainsKey($pgKey)) {$pgCache[$pgKey]=Invoke-HeliosGetJson "/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgId))" $headers $pgKey}
        $pgData=Get-PropertyValue $pgCache[$pgKey] @('protectionGroup') $pgCache[$pgKey]
        $policy=Get-FirstNonBlank @((Get-PropertyValue $pgData @('policy.name','protectionPolicy.name','policyName') ''),'Unknown')
        $domain=Get-FirstNonBlank @((Get-PropertyValue $pgData @('storageDomain.name','storageDomainName') ''),'Unknown')
        $rk="RUNS_$($m.ClusterId)_$pgId"
        if (-not $pgCache.ContainsKey($rk)) {
            $pgCache[$rk]=Invoke-HeliosGetJson "/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgId))/runs?numRuns=$MaxRunsPerPG&excludeNonRestorableRuns=false&includeObjectDetails=true" $headers $rk
        }
        foreach($run in (ConvertTo-ObjectArray (Get-PropertyValue $pgCache[$rk] @('runs') @()))) {
            $info=Get-PropertyValue $run @('localBackupInfo') $null
            if ($info -is [array]) {$info=if ($info.Count) {$info[0]} else {$null}}
            $runId=Get-FirstNonBlank @((Get-PropertyValue $run @('id') ''))
            $typeRaw=Get-FirstNonBlank @((Get-PropertyValue $info @('runType') ''),(Get-PropertyValue $run @('runType') ''))
            $runStatus=Status (Get-PropertyValue $info @('status') '')
            $start=Convert-UsecsToUtc (Get-PropertyValue $info @('startTimeUsecs') 0)
            if (-not $start -or $start -lt $cutoff -or $start -gt $now) {continue}
            foreach($ob in (ConvertTo-ObjectArray (Get-PropertyValue $run @('objects') @()))) {
                $object=Get-PropertyValue $ob @('object') $null
                $oid=Get-FirstNonBlank @((Get-PropertyValue $object @('id','objectId') ''))
                $oname=Get-FirstNonBlank @((Get-PropertyValue $object @('name','displayName') ''))
                $ohost=Get-FirstNonBlank @((Get-PropertyValue $object @('mssqlParams.hostInfo.name','hostName','sourceName') ''))
                if ($m.ObjectId -and $oid) {if ($oid -ne $m.ObjectId) {continue}}
                elseif (-not ((Test-ObjectNameMatch $oname $m.Name) -or ((Test-ObjectNameMatch $ohost $m.Host) -and (Test-ObjectNameMatch $oname $m.Name)))) {continue}
                $lsi=Get-PropertyValue $ob @('localSnapshotInfo') $null
                $snapInfo=Get-PropertyValue $lsi @('snapshotInfo') $null
                $attempts=@(ConvertTo-ObjectArray (Get-PropertyValue $lsi @('failedAttempts') @()))
                $msgs=@($attempts | ForEach-Object {First @((Get-PropertyValue $_ @('message','error') ''))} | Where-Object {$_})
                $statusRaw=Get-FirstNonBlank @((Get-PropertyValue $lsi @('status') ''),(Get-PropertyValue $snapInfo @('status') ''))
                $st=Status $statusRaw
                if ($attempts.Count -gt 0 -and $st -ne 'PASS') {$st='FAIL'}
                # Never use a PG-level PASS as evidence of object success.
                if ($st -eq 'UNKNOWN' -and $runStatus -eq 'FAIL') {$st='UNKNOWN'}
                $kind=Kind $typeRaw
                $allRecords += [pscustomobject]@{Cluster=$m.Cluster;Object=$m.Name;ObjectId=$m.ObjectId;PG=$m.PG;PGId=$pgId;RunId=$runId;StartUtc=$start.ToString('o');Date=$start.ToString('yyyy-MM-dd');Type=$kind;RunType=$typeRaw;Status=$st;RawStatus=$statusRaw;PGStatus=$runStatus;Message=($msgs -join ' | ');FailureAttempts=$attempts.Count}
            }
        }
    }
    $profiles+= [pscustomobject]@{Name=$m.Name;Host=$m.Host;Parent=$m.Parent;Cluster=$m.Cluster;ClusterId=$m.ClusterId;Environment=$m.Environment;Type=$m.Type;ObjectId=$m.ObjectId;ProtectionGroup=$m.PG;ProtectionGroupId=$pgId;Policy=$policy;StorageDomain=$domain;SourceRegistration=$sourceState;DiscoveryRefresh=$refreshState;SourceAuthentication=$authState;LastSourceRefreshUtc=$refreshAt}
}
$profiles=@($profiles | Sort-Object Cluster,ObjectId,Name -Unique)
$allRecords=@($allRecords | Sort-Object Cluster,ObjectId,RunId,Type,StartUtc -Unique)
# Do not combine independent database histories into one failure streak.
$identities=@($allRecords | ForEach-Object {"$($_.Cluster)|$($_.ObjectId)|$($_.Object)"} | Sort-Object -Unique)
$singleIdentity=($identities.Count -le 1)
$dataRuns=@($allRecords | Where-Object {$_.Type -ne 'Log'} | Sort-Object StartUtc -Descending)
$logRuns=@($allRecords | Where-Object {$_.Type -eq 'Log'} | Sort-Object StartUtc -Descending)
$dataFailures=if ($singleIdentity -and $dataRuns.Count) {FailStreak $dataRuns} else {$null}
$logFailures=if ($singleIdentity -and $logRuns.Count) {FailStreak $logRuns} else {$null}
$lastData=@($dataRuns | Where-Object {$_.Status -eq 'PASS'} | Select-Object -First 1)
$lastLog=@($logRuns | Where-Object {$_.Status -eq 'PASS'} | Select-Object -First 1)
$findings=@()
if (-not $singleIdentity) {$findings+= [pscustomobject]@{Severity='UNKNOWN';Area='Multiple objects';Finding='More than one distinct protected object matched this CI';Evidence='Run histories retained separately; combined failure streak suppressed';Action='Re-run with the exact database/object name and cluster, or assess each child independently.'}}
if (-not $profiles.Count) {$findings+= [pscustomobject]@{Severity='UNKNOWN';Area='Discovery';Finding='No matching protected object was verified';Evidence='Global + cluster-scoped searches completed (see API warnings)';Action='Check alternate hostname, source registration and cluster permissions.'}}
elseif (-not $allRecords.Count) {$findings+= [pscustomobject]@{Severity='UNKNOWN';Area='History';Finding='No confirmed object-level runs within the seven-day window';Evidence='Either unavailable, out of window, or truncated by PG run limit';Action='Validate matching object ID, PG ID, run pagination and historical runs.'}}
if ($null -ne $dataFailures -and $dataFailures -gt 0) {
    $msgs=@($dataRuns | Select-Object -First $dataFailures | Where-Object {$_.Message} | ForEach-Object {$_.Message} | Sort-Object -Unique)
    $findings+=[pscustomobject]@{Severity='FAIL';Area='Data backup';Finding="$dataFailures consecutive confirmed data-backup failures";Evidence=($msgs -join ' | ');Action='Correlate object-level messages and source state before retrying. Underlying cause not yet verified.'}
}
if ($null -ne $logFailures -and $logFailures -gt 0) {
    $msgs=@($logRuns | Select-Object -First $logFailures | Where-Object {$_.Message} | ForEach-Object {$_.Message} | Sort-Object -Unique)
    $findings+=[pscustomobject]@{Severity='FAIL';Area='SQL logs';Finding="$logFailures consecutive confirmed log-backup failures";Evidence=($msgs -join ' | ');Action='Investigate log-specific backup records; verify database recovery mode and chain externally.'}
}
if (@($dataRuns | Where-Object {$_.Status -eq 'UNKNOWN'}).Count) {$findings+=[pscustomobject]@{Severity='UNKNOWN';Area='Evidence quality';Finding='Some runs have no confirmed object-level status';Evidence='PG-level success intentionally not treated as object success';Action='Inspect raw run JSON and workload-specific status fields.'}}
if ($errors.Count) {$findings+=[pscustomobject]@{Severity='WARN';Area='API coverage';Finding="$($errors.Count) request(s) failed or returned no data";Evidence=($errors -join ' | ');Action='Check Helios permissions, version and API response schema.'}}
if (-not $findings.Count) {$findings+= [pscustomobject]@{Severity='UNKNOWN';Area='Diagnosis';Finding='No confirmed failure pattern in collected evidence';Evidence='Seven-day, object-level sample';Action='Review remaining unknowns and recovery-point health before declaring recoverable.'}}
$report=[ordered]@{Title='Cohesity Object Diagnostics';RequestedObject=$ObjectName;GeneratedUtc=(Get-Date).ToUniversalTime().ToString('o');WindowDays=$Days;WindowStartUtc=$cutoff.ToString('o');RunCapPerPG=$MaxRunsPerPG;Profiles=$profiles;Runs=$allRecords;Summary=[ordered]@{DataConsecutiveFailures=$dataFailures;LogConsecutiveFailures=$logFailures;LastSuccessfulDataBackup=if($lastData.Count){$lastData[0].StartUtc}else{$null};LastSuccessfulLogBackup=if($lastLog.Count){$lastLog[0].StartUtc}else{$null};LastSuccessfulRestore='UNKNOWN - restore history not queried';Replication='UNKNOWN - replication not queried'};Findings=$findings;Limitations=@('A 7-day run window cannot prove an older last successful backup.','Object-level statuses are not inferred from successful PG runs.','Source registration is checked only for an exact host/source match; VM and SQL child objects may require parent-source resolution. Restore and replication APIs are not queried in this version.','MaxRunsPerPG can truncate high-frequency log history; missing runs are not classified as skipped.');Errors=@($errors)}
if (-not (Test-Path -LiteralPath $OutputRoot)) {New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null}
$stamp=Get-Date -Format yyyyMMdd_HHmmss
$safe=$ObjectName -replace '[^a-zA-Z0-9_.-]','_'
$jsonPath=Join-Path $OutputRoot "Object_Diagnostics_${safe}_${stamp}.json"
$htmlPath=Join-Path $OutputRoot "Object_Diagnostics_${safe}_${stamp}.html"
$rawPath=Join-Path $OutputRoot "Object_Diagnostics_${safe}_${stamp}_raw.json"
[IO.File]::WriteAllText($jsonPath,($report | ConvertTo-Json -Depth 90),[Text.Encoding]::UTF8)
[IO.File]::WriteAllText($rawPath,($evidence | ConvertTo-Json -Depth 100),[Text.Encoding]::UTF8)
$payload=($report | ConvertTo-Json -Depth 90 -Compress).Replace('<','\u003c').Replace('>','\u003e').Replace('&','\u0026')
$template=@'
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Cohesity Object Diagnostics</title>
<style>
:root{font-family:Segoe UI,Arial,sans-serif;color:#e6edf7;background:#0e1523}*{box-sizing:border-box}body{max-width:1250px;margin:auto;padding:25px}h1{font-size:23px;margin:0 0 6px}h2{font-size:16px;margin:23px 0 10px}p{line-height:1.5}small,.muted{color:#9eacc0}.card{background:#172235;border:1px solid #354158;border-radius:12px;padding:16px;margin-top:12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(185px,1fr));gap:10px}.field{background:#111c2e;padding:12px;border-radius:8px;overflow-wrap:anywhere}.field small{display:block;margin-bottom:5px}.badge{display:inline-block;padding:3px 7px;border-radius:5px;font-size:12px;font-weight:700}.PASS{color:#69d9ad;background:#164337}.FAIL{color:#ffa3aa;background:#55242f}.WARN{color:#ffd58a;background:#4b3920}.UNKNOWN,.RUNNING{color:#bdc7d9;background:#323c4e}.week{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:7px}.day{border:1px solid #354158;border-radius:8px;padding:7px;min-height:83px}.day b{font-size:12px}.day .badge{display:block;margin-top:5px;text-align:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}a,button{color:#8bd2ff}button{background:#263b56;border:1px solid #527296;padding:8px 12px;border-radius:7px;cursor:pointer}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px;border-bottom:1px solid #354158;vertical-align:top;overflow-wrap:anywhere}th{color:#aebdd0}.scroll{overflow:auto}#sources{display:none}.link{background:none;border:0;text-decoration:underline;padding:0;font-size:22px;font-weight:700}.note{font-size:12px;color:#b0c0d4}@media(max-width:700px){body{padding:12px}.week{gap:2px}.day{padding:3px;min-height:74px}.day .badge{font-size:9px;padding:3px 1px}.day b{font-size:10px}}@media print{body{color:#111;background:white}.card,.field,.day{background:white;border-color:#888;color:#111}button{display:none}}
</style></head><body>
<header><h1>COHESITY OBJECT DIAGNOSTICS</h1><small id="meta"></small></header>
<main id="diagnostics">
<div class="card"><small>Object — click to see source and identity fields</small><div><button class="link" id="objectLink"></button></div><div id="topline" class="muted"></div></div>
<h2>Protection health</h2><div id="health" class="grid"></div>
<h2>7-day backup calendar <small>(read-only overview)</small></h2><div id="calendar" class="week"></div><p class="note">Full / Incre / Log indicate collected object-level run evidence. Missing activity is Unknown, not a confirmed missed backup. All times are UTC.</p>
<h2>Backup analysis</h2><div id="analysis" class="grid"></div>
<h2>Diagnostic findings and next action</h2><div id="findings"></div>
<h2>Collected backup evidence</h2><div class="card scroll"><table><thead><tr><th>UTC time</th><th>Object</th><th>Type</th><th>Status</th><th>Protection Group</th><th>Run evidence</th></tr></thead><tbody id="runs"></tbody></table></div>
<h2>Scope and unknowns</h2><div class="card" id="limits"></div>
</main>
<section id="sources"><button id="back">← Back to Diagnostics</button><h2>Source and object information</h2><p class="note">All matched identities returned by Helios. Unavailable fields are shown as Unknown.</p><div id="profileCards"></div></section>
<script>
const D=__REPORT__;
const $=id=>document.getElementById(id);
function esc(v){return String(v===null||v===undefined||v===''?'Unknown':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function field(label,value,cls=''){return `<div class="field"><small>${esc(label)}</small><strong class="${cls}">${esc(value)}</strong></div>`}
function badge(s,label){return `<span class="badge ${esc(s)}">${esc(label||s)}</span>`}
function show(source){$('diagnostics').style.display=source?'none':'block';$('sources').style.display=source?'block':'none';window.scrollTo(0,0)}
$('objectLink').onclick=()=>show(true);$('back').onclick=()=>show(false);
$('meta').textContent=`Generated ${D.GeneratedUtc} · GET-only Helios · ${D.WindowDays}-day window`;
$('objectLink').textContent=D.RequestedObject;
$('topline').textContent=D.Profiles.length?D.Profiles.map(p=>p.Cluster+' · '+p.Environment+' · '+p.ProtectionGroup).join(' | '):'No verified protected-object match';
const fails=D.Findings.some(f=>f.Severity==='FAIL');
$('health').innerHTML=[field('Protected object found',D.Profiles.length?'YES':'UNKNOWN',D.Profiles.length?'PASS':'UNKNOWN'),field('Source registration',D.Profiles.length?D.Profiles[0].SourceRegistration:'UNKNOWN','UNKNOWN'),field('Discovery refresh',D.Profiles.length?D.Profiles[0].DiscoveryRefresh:'UNKNOWN','UNKNOWN'),field('PG mapping',D.Profiles.some(p=>p.ProtectionGroupId)?'IDENTIFIED':'UNKNOWN',D.Profiles.some(p=>p.ProtectionGroupId)?'PASS':'UNKNOWN'),field('Backup health',fails?'ISSUES FOUND':'INCOMPLETE',fails?'FAIL':'UNKNOWN'),field('Replication','NOT QUERIED','UNKNOWN'),field('Last restore','NOT QUERIED','UNKNOWN')].join('');
const summary=D.Summary;const hasLog=D.Runs.some(r=>r.Type==='Log')||D.Profiles.some(p=>/sql|mssql/i.test(p.Environment+' '+p.Type));
$('analysis').innerHTML=[field('Consecutive data backup failures',summary.DataConsecutiveFailures===null?'Not established':summary.DataConsecutiveFailures),field('Consecutive log backup failures',summary.LogConsecutiveFailures===null?'Not established':summary.LogConsecutiveFailures),field('Last successful data backup (in window)',summary.LastSuccessfulDataBackup||'Not established'),field('Last successful log backup (in window)',summary.LastSuccessfulLogBackup||'Not established'),field('Last successful restore',summary.LastSuccessfulRestore),field('Replication',summary.Replication)].join('');
function agg(rows){if(!rows.length)return 'UNKNOWN';const s=rows.map(x=>x.Status);if(s.includes('FAIL'))return 'FAIL';if(s.includes('WARN'))return 'WARN';if(s.includes('UNKNOWN')||s.includes('RUNNING'))return 'UNKNOWN';return s.every(x=>x==='PASS')?'PASS':'UNKNOWN'}
let days=[];const now=new Date(D.GeneratedUtc);const base=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate()));for(let i=D.WindowDays-1;i>=0;i--){const d=new Date(base);d.setUTCDate(d.getUTCDate()-i);days.push(d.toISOString().slice(0,10))}
$('calendar').innerHTML=days.map(date=>{const rows=D.Runs.filter(r=>r.Date===date);const data=rows.filter(r=>r.Type!=='Log');const logs=rows.filter(r=>r.Type==='Log');const type=data.some(r=>r.Type==='Full')?'Full':data.some(r=>r.Type==='Incre')?'Incre':'Data';return `<div class="day"><b>${esc(date.slice(5))}</b>${badge(agg(data),data.length?type:'No data')}${hasLog?(logs.length?badge(agg(logs),'Log'):badge('UNKNOWN','Log —')):''}</div>`}).join('');
$('findings').innerHTML=D.Findings.map(f=>`<div class="card">${badge(f.Severity)} <b>${esc(f.Area)}: ${esc(f.Finding)}</b><p><small>Evidence:</small> ${esc(f.Evidence||'No definitive evidence returned')}</p><p><small>Next action:</small> ${esc(f.Action)}</p></div>`).join('');
$('runs').innerHTML=D.Runs.length?D.Runs.slice().sort((a,b)=>b.StartUtc.localeCompare(a.StartUtc)).map(r=>`<tr><td>${esc(r.StartUtc)}</td><td>${esc(r.Object)}</td><td>${esc(r.Type)} (${esc(r.RunType)})</td><td>${badge(r.Status)}</td><td>${esc(r.PG)}</td><td>${esc(r.Message||'No object message')}<br><small>${esc(r.RunId)}</small></td></tr>`).join(''):'<tr><td colspan="6">No confirmed object runs returned for this window.</td></tr>';
$('limits').innerHTML=D.Limitations.map(s=>`<p class="note">• ${esc(s)}</p>`).join('')+(D.Errors.length?`<p>${badge('WARN',D.Errors.length+' API collection warnings')} See JSON evidence.</p>`:'');
$('profileCards').innerHTML=D.Profiles.length?D.Profiles.map(p=>`<div class="card"><div class="grid">${Object.entries({'Object name':p.Name,'Source host':p.Host,'Parent object':p.Parent,'Cluster':p.Cluster,'Cluster ID':p.ClusterId,'Environment':p.Environment,'Object type':p.Type,'Object ID':p.ObjectId,'Protection Group':p.ProtectionGroup,'Protection Group ID':p.ProtectionGroupId,'Policy':p.Policy,'Storage domain':p.StorageDomain,'Registration':p.SourceRegistration,'Discovery refresh':p.DiscoveryRefresh,'Source authentication':p.SourceAuthentication,'Last source refresh':p.LastSourceRefreshUtc}).map(([k,v])=>field(k,v)).join('')}</div></div>`).join(''):'<div class="card">No protected-object profile identified.</div>';
</script></body></html>
'@
$html=$template.Replace('__REPORT__',$payload)
[IO.File]::WriteAllText($htmlPath,$html,[Text.Encoding]::UTF8)
Write-Host "`nCOHESITY OBJECT DIAGNOSTICS" -ForegroundColor Cyan
Write-Host "Object: $ObjectName | Profiles: $($profiles.Count) | Object runs: $($allRecords.Count)"
Write-Host "Data failure streak: $(if($null -eq $dataFailures){'UNKNOWN'}else{$dataFailures}) | Log failure streak: $(if($null -eq $logFailures){'UNKNOWN'}else{$logFailures})"
Write-Host "HTML: $htmlPath"
Write-Host "JSON: $jsonPath"
Write-Host "Raw evidence: $rawPath"
if($errors.Count){Write-Warning "$($errors.Count) API request(s) failed; inspect JSON and HTML limitations."}