# Cohesity Helios - SQL Protection Group Alert Configuration Audit
# GET-only | PowerShell 5.1 compatible
#
# Scope:
#   - Active SQL PGs only (not deleted or paused)
#   - Entire alertPolicy section, including each alert target and recipient metadata
#   - Console summary + Excel workbook, no JSON files and no API writes
#
# APIs:
#   GET /v2/mcm/cluster-mgmt/info
#   GET /v2/data-protect/protection-groups?environments=kSQL...
#
# Excel export requires either ImportExcel module or Microsoft Excel desktop.

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Data\Cohesity\SQLInventory"
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# ------------------------------------------------------------
# Helios and local authentication configuration
# ------------------------------------------------------------
$baseUrl             = "https://helios.cohesity.com"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace([string]$apiKey)) {
    throw "AES API key helper returned an empty value."
}

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
# ------------------------------------------------------------
# Flatten nested alertPolicy properties into readable Excel columns
# ------------------------------------------------------------
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
# ------------------------------------------------------------
# Excel report (ImportExcel preferred, Excel COM fallback)
# ------------------------------------------------------------
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
# ------------------------------------------------------------
# Discover clusters and choose scope
# ------------------------------------------------------------
Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host " COHESITY SQL PG ALERT CONFIGURATION AUDIT" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan

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
Write-Host ""
Write-Host "AVAILABLE HELIOS CLUSTERS" -ForegroundColor Cyan
$clusterMenu = for ($i=0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{
        Index   = $i + 1
        Cluster = $clusters[$i].Name
    }
}
$clusterMenu | Format-Table -AutoSize | Out-Host
Write-Host "[0] All clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $choice = Read-Host "Select cluster (0=all, 1-$($clusters.Count)=single, X=exit)"
    if ($choice -match '^(x|q)
$columns=New-Object System.Collections.Generic.List[string]
$errors=New-Object System.Collections.Generic.List[object]
foreach($cluster in $selected){
 Write-Host ("Checking {0}..." -f $cluster.Name) -ForegroundColor Gray
 try{
   $cookie=$null;$seen=@{}
   do{
     $uri="$baseUrl/v2/data-protect/protection-groups?environments=kSQL&isActive=true&isDeleted=false&isPaused=false&includeTenants=true&useCachedData=false&maxResultCount=1000"
     if($cookie){$uri+='&paginationCookie='+[uri]::EscapeDataString($cookie)}
     $response=Get-Helios -Uri $uri -ClusterId $cluster.Id
     if ($null -eq $response) { throw "Empty protection group response" }
     if ($null -eq $response.protectionGroups) { throw "Missing protectionGroups in response" }
     foreach($pg in @($response.protectionGroups)){
       if($null -eq $pg -or $pg.environment -ne 'kSQL' -or $pg.isActive -ne $true -or $pg.isDeleted -eq $true -or $pg.isPaused -eq $true){continue}
       $policy=$pg.alertPolicy
       # An absent policy must remain visible as a configuration finding.
       $fields=Flatten-Policy $policy
       foreach($key in $fields.Keys){if(!$columns.Contains($key)){$columns.Add($key)}}
       $targets=@()
       if($null -ne $policy -and $null -ne $policy.alertTargets){$targets=@($policy.alertTargets | Where-Object{$null -ne $_})}
       $statuses=@()
       if ($null -ne $policy) {
         if ($null -ne $policy.alertPolicy) { $statuses += @($policy.alertPolicy) }
         if ($null -ne $policy.backupRunStatus) { $statuses += @($policy.backupRunStatus) }
       }
       $statusText=(@($statuses|ForEach-Object{[string]$_}) -join '; ')
       $failure=($statusText -match '(^|[^A-Za-z])kFailure([^A-Za-z]|$)')
       $addresses=@($targets|ForEach-Object{$_.emailAddress}|Where-Object{$_})
       $raw.Add([pscustomobject]@{
         Cluster=$cluster.Name;ClusterId=$cluster.Id
         ProtectionGroup=$(if($pg.name){$pg.name}else{$pg.protectionGroupName})
         ProtectionGroupId=$pg.id;FailureAlert=$(if($failure){'Yes'}else{'No'})
         AlertTargets=($addresses -join '; ')
         RecipientTypes=(@($targets|ForEach-Object{$_.recipientType}) -join '; ')
         Languages=(@($targets|ForEach-Object{$_.language}) -join '; ')
         Status=$(if($null -eq $policy){'ALERT POLICY MISSING'}elseif(!$failure){'FAILURE NOT CONFIGURED'}elseif(!$addresses.Count){'NO EMAIL RECIPIENTS'}else{'CONFIGURED'})
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
# ------------------------------------------------------------
# Display and export
# ------------------------------------------------------------
$rows = @($rows | Sort-Object Cluster, ProtectionGroup)
$configuredCount = @($rows | Where-Object { $_.Status -eq "CONFIGURED" }).Count
$reviewCount = $rows.Count - $configuredCount

Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host " SQL PG FAILURE ALERT CONFIGURATION" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan

if ($rows.Count -gt 0) {
    $rows |
        Select-Object Cluster, ProtectionGroup, FailureAlert,
                      AlertTargets, RecipientTypes, Languages, Status |
        Format-Table -AutoSize -Wrap |
        Out-Host
}
else {
    Write-Host "No active SQL protection groups were returned." -ForegroundColor Yellow
}

if ($errors.Count -gt 0) {
    Write-Host ""
    Write-Host "CLUSTER COLLECTION ISSUES" -ForegroundColor Yellow
    $errors | Format-Table -AutoSize -Wrap | Out-Host
}

$excelPath = $null
if ($rows.Count -gt 0) {
    if (-not (Test-Path $OutputDirectory -PathType Container)) {
        New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
    }
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $excelPath = Join-Path $OutputDirectory "Cohesity_SQL_PG_AlertPolicy_$timestamp.xlsx"
    Save-Xlsx -Rows $rows -Path $excelPath
}

Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host " AUDIT SUMMARY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host ("Clusters selected          : {0}" -f $selected.Count)
Write-Host ("Clusters failed            : {0}" -f $errors.Count)
Write-Host ("Active SQL PGs             : {0}" -f $rows.Count)
Write-Host ("Failure emails configured  : {0}" -f $configuredCount) -ForegroundColor Green
Write-Host ("Require configuration check: {0}" -f $reviewCount) -ForegroundColor Yellow

if ($excelPath) {
    Write-Host ("Excel output               : {0}" -f $excelPath) -ForegroundColor Green
}
if ($errors.Count -gt 0) {
    Write-Warning "Partial report: some cluster queries failed."
}
