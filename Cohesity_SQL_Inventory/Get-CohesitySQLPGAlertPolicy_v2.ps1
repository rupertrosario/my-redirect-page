# Cohesity Helios - SQL Protection Group Alert Configuration Audit V2
# GET-only | PowerShell 5.1 compatible | V2: PG creator + creation timestamp
#
# Scope:
#   - Active SQL PGs only (not deleted or paused)
#   - Entire alertPolicy section, including each alert target and recipient metadata
#   - Audit logs fetched before PG inventory, joined on cluster + entityName
#   - Console summary + Excel workbook, no JSON files and no API writes
#
# APIs:
#   GET /v2/mcm/cluster-mgmt/info
#   GET /v2/data-protect/protection-groups?environments=kSQL...
#   GET /v2/audit-logs?entityTypes=ProtectionGroup&actions=Create
#   Match by auditLogs[].entityName; creator=username; time=timestampUsecs
#
# Excel export requires either ImportExcel module or Microsoft Excel desktop.

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Data\Cohesity\SQLInventory",
    [int]$MaxAuditPages = 200
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
# Read creation audit history per selected cluster
# ------------------------------------------------------------
function Get-PropertyCandidate {
    param($Object, [string[]]$Names)
    if ($null -eq $Object) { return $null }
    foreach ($name in $Names) {
        $parts = $name -split '\\.'
        $current = $Object
        $valid = $true
        foreach ($part in $parts) {
            if ($null -eq $current) { $valid = $false; break }
            $prop = @($current.PSObject.Properties | Where-Object { $_.Name -ieq $part } | Select-Object -First 1)
            if ($prop.Count -eq 0) { $valid = $false; break }
            $current = $prop[0].Value
        }
        if ($valid -and $null -ne $current -and ([string]$current).Trim() -ne '') { return $current }
    }
    return $null
}

function Convert-AuditTime {
    param($Audit)
    $value = Get-PropertyCandidate $Audit @(
        'timestampUsecs','timeUsecs','createdTimeUsecs','eventTimeUsecs',
        'timestampMsecs','timestampMillis','timeMsecs',
        'timestamp','createdTime','eventTime','time'
    )
    if ($null -eq $value) { return '' }
    $text = [string]$value
    try {
        if ($text -match '^\d+$') {
            $number = [double]$text
            if ($number -ge 1e15) { return [DateTimeOffset]::FromUnixTimeMilliseconds([long][math]::Floor($number / 1000)).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }
            if ($number -ge 1e12) { return [DateTimeOffset]::FromUnixTimeMilliseconds([long]$number).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }
            if ($number -ge 1e9)  { return [DateTimeOffset]::FromUnixTimeSeconds([long]$number).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }
        }
        return ([datetime]::Parse($text)).ToString('yyyy-MM-dd HH:mm:ss')
    }
    catch { return '' }
}

function Get-CreateAuditRecords {
    param([string]$ClusterId)

    # Keep every record exactly as returned under auditLogs[].
    # Filtering is performed by the GET endpoint, not by this function.
    $records = New-Object System.Collections.Generic.List[object]
    $pageSize = 500
    $startIndex = 0
    $complete = $false

    for ($page = 0; $page -lt $MaxAuditPages; $page++) {
        $uri = "$baseUrl/v2/audit-logs?entityTypes=ProtectionGroup&actions=Create" +
               "&includeTenants=true&startIndex=$startIndex&count=$pageSize"
        $response = Get-Helios -Uri $uri -ClusterId $ClusterId
        if ($null -eq $response -or -not $response.PSObject.Properties['auditLogs']) {
            throw "GET /v2/audit-logs did not return auditLogs."
        }

        $batch = @($response.auditLogs | Where-Object { $null -ne $_ })
        Write-Host ("  Audit page {0}: API returned {1} records" -f ($page+1), $batch.Count) -ForegroundColor Gray

        foreach ($entry in $batch) { $records.Add($entry) }
        $startIndex += $batch.Count

        # Do not interpret response.count as the number retrieved or
        # as a total: finish only when the API returns a short page.
        if ($batch.Count -lt $pageSize) {
            $complete = $true
            break
        }
    }

    if (-not $complete) {
        throw "Audit retrieval stopped after $MaxAuditPages pages; results may be incomplete."
    }

    Write-Host ("  Total auditLogs[] entries retrieved: {0}" -f $records.Count) -ForegroundColor Cyan
    return @($records.ToArray())
}

function Get-CreationByName {
    param([object[]]$AuditRecords)

    # Key creation records by exact PG name, ignoring case.
    # Recreated PG names use the most recent creation event.
    $byName = @{}
    foreach ($audit in $AuditRecords) {
        # Only named, matching creation events are used for PG enrichment.
        if ([string]$audit.action -ine 'Create' -or
            [string]$audit.entityType -ine 'ProtectionGroup') { continue }
        $name = ([string]$audit.entityName).Trim()
        if (-not $name) { continue }
        $time = [long]$audit.timestampUsecs

        if (-not $byName.ContainsKey($name) -or $time -gt $byName[$name].TimeUsecs) {
            $byName[$name] = [pscustomobject]@{
                CreatedBy = [string]$audit.username
                CreatedOn = Convert-AuditTime $audit
                TimeUsecs = $time
            }
        }
    }
    return $byName
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
    if ($choice -match '^(x|q)$') { return }

    $index = -1
    if ([int]::TryParse($choice, [ref]$index) -and $index -ge 0 -and $index -le $clusters.Count) {
        break
    }
    Write-Host "Invalid selection. Please try again." -ForegroundColor Yellow
}

$selected = if ($index -eq 0) { @($clusters) } else { @($clusters[$index - 1]) }

Write-Host ""
Write-Host "COLLECTING ACTIVE SQL PROTECTION GROUPS" -ForegroundColor Cyan

$raw=New-Object System.Collections.Generic.List[object]
$columns=New-Object System.Collections.Generic.List[string]
$errors=New-Object System.Collections.Generic.List[object]
# ------------------------------------------------------------
# 1. Retrieve all available PG Create audit logs FIRST
# ------------------------------------------------------------
Write-Host ""
Write-Host "COLLECTING PROTECTION GROUP CREATION AUDITS" -ForegroundColor Cyan

$auditIndex = @{}
$auditReady = @{}
foreach ($cluster in $selected) {
    try {
        $auditRecords = @(Get-CreateAuditRecords -ClusterId $cluster.Id)
        Write-Host ""
        Write-Host ("AUDIT RECORDS RETRIEVED - {0}" -f $cluster.Name) -ForegroundColor Cyan
        if ($auditRecords.Count -gt 0) {
            $auditRecords |
                Select-Object @{Name='EntityName';Expression={$_.entityName}},
                              @{Name='Action';Expression={$_.action}},
                              @{Name='EntityType';Expression={$_.entityType}},
                              @{Name='Username';Expression={$_.username}},
                              @{Name='TimestampUsecs';Expression={$_.timestampUsecs}} |
                Format-Table -AutoSize -Wrap | Out-Host
        }
        else {
            Write-Host "No matching creation audit records returned." -ForegroundColor Yellow
        }
        Write-Host ""
        $auditIndex[$cluster.Id] = Get-CreationByName -AuditRecords $auditRecords
        $auditReady[$cluster.Id] = $true
        Write-Host ("{0}: {1} raw audit records, {2} distinct matching PG names" -f $cluster.Name, $auditRecords.Count, $auditIndex[$cluster.Id].Count) -ForegroundColor Gray
    }
    catch {
        $auditIndex[$cluster.Id] = @{}
        $auditReady[$cluster.Id] = $false
        $errors.Add([pscustomobject]@{
            Cluster = $cluster.Name
            Error = "Audit logs: $($_.Exception.Message)"
        })
        Write-Warning ("Audit retrieval failed for {0}: {1}" -f $cluster.Name, $_.Exception.Message)
    }
}

# ------------------------------------------------------------
# 2. Retrieve active SQL PGs and compare each name with audit index
# ------------------------------------------------------------
Write-Host ""
Write-Host "COLLECTING ACTIVE SQL PROTECTION GROUPS" -ForegroundColor Cyan

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
       $pgId=[string]$pg.id
       $createdBy='';$createdOn=''
       $pgName=if($pg.name){[string]$pg.name}else{[string]$pg.protectionGroupName}
       if ($auditReady[$cluster.Id] -and $pgName -and $auditIndex[$cluster.Id].ContainsKey($pgName.Trim())) {
           $createdBy=$auditIndex[$cluster.Id][$pgName.Trim()].CreatedBy
           $createdOn=$auditIndex[$cluster.Id][$pgName.Trim()].CreatedOn
       }
       $raw.Add([pscustomobject]@{
         Cluster=$cluster.Name;ClusterId=$cluster.Id
         ProtectionGroup=$pgName
         ProtectionGroupId=$pg.id;FailureAlert=$(if($failure){'Yes'}else{'No'})
         AlertTargets=($addresses -join '; ')
         RecipientTypes=(@($targets|ForEach-Object{$_.recipientType}) -join '; ')
         Languages=(@($targets|ForEach-Object{$_.language}) -join '; ')
         Status=$(if($null -eq $policy){'ALERT POLICY MISSING'}elseif(!$failure){'FAILURE NOT CONFIGURED'}elseif(!$addresses.Count){'NO EMAIL RECIPIENTS'}else{'CONFIGURED'})
         CreatedBy=$createdBy;CreatedOn=$createdOn
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
# ------------------------------------------------------------
$rows=@(foreach($item in $raw){
 $record=[ordered]@{
  Cluster=$item.Cluster;ClusterId=$item.ClusterId;ProtectionGroup=$item.ProtectionGroup
  ProtectionGroupId=$item.ProtectionGroupId;FailureAlert=$item.FailureAlert
  AlertTargets=$item.AlertTargets;RecipientTypes=$item.RecipientTypes
  Languages=$item.Languages;CreatedBy=$item.CreatedBy;CreatedOn=$item.CreatedOn
  Status=$item.Status
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
Write-Host " SQL PG FAILURE ALERT CONFIGURATION V2" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan

if ($rows.Count -gt 0) {
    $rows |
        Select-Object Cluster, ProtectionGroup, FailureAlert,
                      AlertTargets, RecipientTypes, Languages, CreatedBy, CreatedOn, Status |
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
    $excelPath = Join-Path $OutputDirectory "Cohesity_SQL_PG_AlertPolicy_v2_$timestamp.xlsx"
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
