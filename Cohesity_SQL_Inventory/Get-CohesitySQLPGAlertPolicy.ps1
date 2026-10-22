# Get-CohesitySQLPGAlertPolicy.ps1
# Cohesity Helios | GET only | PowerShell 5.1 | Active SQL Protection Groups
$ErrorActionPreference='Stop'
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
$baseUrl='https://helios.cohesity.com'
# Existing local AES helper paths from Cohesity scripts; change if your workstation differs.
$helperPath='X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$keyFile='X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
$outputDirectory=if($PSScriptRoot){$PSScriptRoot}else{(Get-Location).Path}
if(!(Test-Path $helperPath)){throw "Helper missing: $helperPath"}
if(!(Test-Path $keyFile)){throw "Encrypted key missing: $keyFile"}
. $helperPath
$apiKey=Get-CohesityApiKeyFromAes -EncryptedFile $keyFile
if([string]::IsNullOrWhiteSpace($apiKey)){throw 'API key is empty'}
function Get-Helios {
 param([string]$Uri,[string]$ClusterId)
 $h=@{accept='application/json';apiKey=$script:apiKey}
 if($ClusterId){$h.accessClusterId=$ClusterId}
 $arg=@{Uri=$Uri;Headers=$h;Method='Get';ErrorAction='Stop'}
 if($PSVersionTable.PSVersion.Major -lt 6){$arg.UseBasicParsing=$true}
 $r=Invoke-WebRequest @arg
 if(!$r.Content){throw "Empty API response: $Uri"}
 return ($r.Content|ConvertFrom-Json)
}
function Flatten-Policy {
 param($Value,[string]$Prefix='alertPolicy')
 $o=[ordered]@{}
 if($null -eq $Value){$o[$Prefix]='';return $o}
 if($Value -is [array]){
   for($i=0;$i -lt $Value.Count;$i++){
     $children=Flatten-Policy $Value[$i] "$Prefix[$i]"
     foreach($k in $children.Keys){$o[$k]=$children[$k]}
   }
   if($Value.Count -eq 0){$o[$Prefix]='(empty)'}
   return $o
 }
 if($Value -is [pscustomobject]){
   foreach($p in $Value.PSObject.Properties){
     $children=Flatten-Policy $p.Value "$Prefix.$($p.Name)"
     foreach($k in $children.Keys){$o[$k]=$children[$k]}
   }
   return $o
 }
 $o[$Prefix]=[string]$Value
 return $o
}
function Save-Xlsx {
 param([object[]]$Rows,[string]$Path)
 if(Get-Module -ListAvailable ImportExcel){
   Import-Module ImportExcel -ErrorAction Stop
   $Rows|Export-Excel -Path $Path -WorksheetName 'SQL PG Alerts' -AutoSize -BoldTopRow -FreezeTopRow -AutoFilter
   return
 }
 $excel=$null;$book=$null;$sheet=$null
 try{
   $excel=New-Object -ComObject Excel.Application -ErrorAction Stop
   $excel.Visible=$false;$excel.DisplayAlerts=$false
   $book=$excel.Workbooks.Add();$sheet=$book.Worksheets.Item(1)
   $sheet.Name='SQL PG Alerts'
   $cols=@($Rows[0].PSObject.Properties.Name)
   for($c=0;$c -lt $cols.Count;$c++){$sheet.Cells.Item(1,$c+1)=$cols[$c]}
   for($r=0;$r -lt $Rows.Count;$r++){
     for($c=0;$c -lt $cols.Count;$c++){$sheet.Cells.Item($r+2,$c+1)=[string]$Rows[$r].($cols[$c])}
   }
   $range=$sheet.UsedRange
   $range.EntireColumn.AutoFit()|Out-Null
   $sheet.Rows.Item(1).Font.Bold=$true
   $range.AutoFilter()|Out-Null
   $book.SaveAs($Path,51)
 }catch{throw "Cannot save XLSX: install ImportExcel or Microsoft Excel desktop. $($_.Exception.Message)"}
 finally{
   if($book){$book.Close($false)}
   if($excel){$excel.Quit()}
   foreach($obj in @($sheet,$book,$excel)){if($null -ne $obj){[void][Runtime.InteropServices.Marshal]::ReleaseComObject($obj)}}
 }
}
$inventory=Get-Helios "$baseUrl/v2/mcm/cluster-mgmt/info"
$items=@()
if($inventory.cohesityClusters){$items=@($inventory.cohesityClusters)}
elseif($inventory.clusters){$items=@($inventory.clusters)}
elseif($inventory.clusterInfos){$items=@($inventory.clusterInfos)}
elseif($inventory.mcmInfo -and $inventory.mcmInfo.clusterInfos){$items=@($inventory.mcmInfo.clusterInfos)}
elseif($inventory -is [array]){$items=@($inventory)}
$clusters=@(foreach($c in $items){
 $id=if($c.clusterId){$c.clusterId}else{$c.id}
 $name=if($c.clusterName){$c.clusterName}elseif($c.displayName){$c.displayName}else{$c.name}
 if($id){[pscustomobject]@{Name=[string]$name;Id=[string]$id}}
})
$clusters=@($clusters|Sort-Object Name,Id -Unique)
if(!$clusters.Count){throw 'No Helios clusters found'}
Write-Host '[0] All clusters'
for($i=0;$i -lt $clusters.Count;$i++){Write-Host "[$($i+1)] $($clusters[$i].Name)"}
$choice=Read-Host 'Select cluster number (0=all)'
[int]$index=-1
if(![int]::TryParse($choice,[ref]$index) -or $index -lt 0 -or $index -gt $clusters.Count){throw 'Invalid selection'}
$selected=if($index -eq 0){@($clusters)}else{@($clusters[$index-1])}
$raw=New-Object System.Collections.Generic.List[object]
$columns=New-Object System.Collections.Generic.List[string]
$errors=New-Object System.Collections.Generic.List[object]
foreach($cluster in $selected){
 Write-Host "Checking $($cluster.Name)..." -ForegroundColor Cyan
 try{
   $cookie=$null;$seen=@{}
   do{
     $uri="$baseUrl/v2/data-protect/protection-groups?environments=kSQL&isActive=true&isDeleted=false&isPaused=false&maxResultCount=200"
     if($cookie){$uri+='&paginationCookie='+[uri]::EscapeDataString($cookie)}
     $response=Get-Helios -Uri $uri -ClusterId $cluster.Id
     if($null -eq $response.protectionGroups){throw 'Missing protectionGroups in response'}
     foreach($pg in @($response.protectionGroups)){
       if($null -eq $pg -or $pg.environment -ne 'kSQL' -or $pg.isActive -ne $true -or $pg.isDeleted -eq $true -or $pg.isPaused -eq $true){continue}
       $policy=$pg.alertPolicy
       $fields=Flatten-Policy $policy
       foreach($key in $fields.Keys){if(!$columns.Contains($key)){$columns.Add($key)}}
       $targets=@()
       if($null -ne $policy -and $null -ne $policy.alertTargets){$targets=@($policy.alertTargets | Where-Object{$null -ne $_})}
       $statuses=@()
       if($null -ne $policy){
         if($null -ne $policy.alertPolicy){$statuses+=@($policy.alertPolicy)}
         if($null -ne $policy.backupRunStatus){$statuses+=@($policy.backupRunStatus)}
       }
       $failure=(@($statuses|ForEach-Object{[string]$_}) -join '; ') -match '(^|[^A-Za-z])kFailure([^A-Za-z]|$)'
       $addresses=@($targets|ForEach-Object{$_.emailAddress}|Where-Object{$_})
       $raw.Add([pscustomobject]@{
         Cluster=$cluster.Name;ClusterId=$cluster.Id
         ProtectionGroup=$(if($pg.name){$pg.name}else{$pg.protectionGroupName})
         ProtectionGroupId=$pg.id;FailureAlert=$(if($failure){'Yes'}else{'No'})
         AlertTargets=($addresses -join '; ')
         RecipientTypes=(@($targets|ForEach-Object{$_.recipientType}) -join '; ')
         Languages=(@($targets|ForEach-Object{$_.language}) -join '; ')
         Status=$(if($null -eq $policy){'Policy missing'}elseif(!$failure){'kFailure not configured'}elseif(!$addresses.Count){'No email recipients'}else{'Failure email configured'})
         Fields=$fields
       })
     }
     $cookie=[string]$response.paginationCookie
     if($cookie){if($seen.ContainsKey($cookie)){throw 'Repeated pagination cookie'};$seen[$cookie]=$true}
   }while($cookie)
 }catch{
   $errors.Add([pscustomobject]@{Cluster=$cluster.Name;Error=$_.Exception.Message})
   Write-Warning "Cluster $($cluster.Name): $($_.Exception.Message)"
 }
}
$rows=@(foreach($item in $raw){
 $record=[ordered]@{
  Cluster=$item.Cluster;ClusterId=$item.ClusterId;ProtectionGroup=$item.ProtectionGroup
  ProtectionGroupId=$item.ProtectionGroupId;FailureAlert=$item.FailureAlert
  AlertTargets=$item.AlertTargets;RecipientTypes=$item.RecipientTypes
  Languages=$item.Languages;Status=$item.Status
 }
 foreach($key in $columns){$record[$key]=if($item.Fields.Contains($key)){$item.Fields[$key]}else{''}}
 [pscustomobject]$record
})
Write-Host "`nACTIVE SQL PG ALERT CONFIGURATION" -ForegroundColor Cyan
if($rows.Count){
 $rows|Select-Object Cluster,ProtectionGroup,FailureAlert,AlertTargets,RecipientTypes,Languages,Status|Format-Table -AutoSize -Wrap|Out-Host
 $path=Join-Path $outputDirectory ("Cohesity_SQL_PG_AlertPolicy_{0}.xlsx" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
 Save-Xlsx -Rows $rows -Path $path
 Write-Host "Excel report: $path" -ForegroundColor Green
}else{Write-Warning 'No matching active SQL PGs; no spreadsheet created'}
Write-Host "SQL PGs: $($rows.Count); failed clusters: $($errors.Count)"
if($errors.Count){$errors|Format-Table -AutoSize -Wrap|Out-Host;Write-Warning 'Report incomplete: one or more clusters failed'}
