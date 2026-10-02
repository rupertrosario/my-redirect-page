<#
.SYNOPSIS
Report Cohesity runs with an explicitly empty objects array (GET only).
.DESCRIPTION
Uses the existing AES helper and encrypted API key file. Prompts for one,
several, or all Helios clusters. Checks the latest NumRuns per PG, not all history.
Missing/null objects and failed requests are review items, never zero-object matches.
Times are Eastern Time, matching the existing backup scripts.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [string[]]$ClusterName = @(),
    [string]$ProtectionGroupName = '',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\ZeroObjectRuns',
    [string]$HelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1',
    [string]$EncryptedFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc',
    [ValidateRange(1,600)][int]$RequestTimeoutSec = 120
)
$ErrorActionPreference = 'Stop'
# Daily scan: latest 10 runs per protection group.
$NumRuns = 10
$BaseUrl = $BaseUrl.TrimEnd('/')

function Get-Prop($ObjectValue, [string]$Name, $DefaultValue = $null) {
    if ($null -eq $ObjectValue) { return $DefaultValue }
    $Property = $ObjectValue.PSObject.Properties[$Name]
    if ($Property) { return $Property.Value }
    return $DefaultValue
}
function As-Array($Value) {
    if ($null -ne $Value) { $Value }
}
function Get-ClusterDisplayName($Cluster) {
    foreach ($Field in @('name','clusterName','displayName')) {
        $Value = [string](Get-Prop $Cluster $Field '')
        if ($Value.Trim()) { return $Value.Trim() }
    }
    return "Unknown-$($Cluster.clusterId)"
}
function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    $Arguments = @{ Method='Get'; Uri=$Uri; Headers=$Headers; TimeoutSec=$RequestTimeoutSec }
    if ($PSVersionTable.PSVersion.Major -lt 6) { $Arguments.UseBasicParsing = $true }
    $Response = Invoke-WebRequest @Arguments
    if ([string]::IsNullOrWhiteSpace($Response.Content)) { throw 'Empty API response.' }
    return ($Response.Content | ConvertFrom-Json)
}
function Get-CohesityApiKeySafe {
    if (!(Test-Path -LiteralPath $HelperPath -PathType Leaf)) { throw "Missing AES helper: $HelperPath" }
    if (!(Test-Path -LiteralPath $EncryptedFile -PathType Leaf)) { throw "Missing encrypted API key: $EncryptedFile" }
    . $HelperPath
    $ApiKeyValue = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($ApiKeyValue)) { throw 'AES helper returned a blank API key.' }
    return $ApiKeyValue.Trim()
}
try { $EtZone = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
catch { $EtZone = [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
function Convert-UsecsToEtText($Usecs) {
    if ($null -eq $Usecs -or [int64]$Usecs -le 0) { return '' }
    $UtcDate = [DateTimeOffset]::FromUnixTimeMilliseconds([int64][math]::Floor([double]$Usecs / 1000)).UtcDateTime
    return ([TimeZoneInfo]::ConvertTimeFromUtc($UtcDate, $EtZone)).ToString('yyyy-MM-dd HH:mm:ss')
}
function Write-CsvRows($Rows, [string]$Path, [string[]]$Columns) {
    if (@($Rows).Count -gt 0) {
        $Rows | Select-Object $Columns | Export-Csv -LiteralPath $Path -NoTypeInformation -Encoding UTF8
    } else {
        ($Columns -join ',') | Set-Content -LiteralPath $Path -Encoding UTF8
    }
}

$ApiKey = Get-CohesityApiKeySafe
$BaseHeaders = @{ accept='application/json'; apiKey=$ApiKey }
$ClusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers $BaseHeaders
if (!$ClusterJson.PSObject.Properties['cohesityClusters']) { throw 'Cluster response missing cohesityClusters.' }
$Clusters = @(As-Array $ClusterJson.cohesityClusters | Sort-Object { Get-ClusterDisplayName $_ })
if ($Clusters.Count -eq 0) { throw 'No accessible clusters returned.' }

if ($ClusterName.Count -gt 0) {
    foreach ($RequestedName in $ClusterName) {
        if (@($Clusters | Where-Object { (Get-ClusterDisplayName $_) -eq $RequestedName }).Count -ne 1) {
            throw "Cluster name must match exactly one cluster: $RequestedName"
        }
    }
    $SelectedClusters = @($Clusters | Where-Object { (Get-ClusterDisplayName $_) -in $ClusterName })
} else {
    Write-Host ''
    for ($Index = 0; $Index -lt $Clusters.Count; $Index++) {
        Write-Host ('{0,3}. {1} [{2}]' -f ($Index + 1),(Get-ClusterDisplayName $Clusters[$Index]),$Clusters[$Index].clusterId)
    }
    do {
        $Selection = (Read-Host 'Select cluster number(s), e.g. 1,3 or A for all').Trim()
        $ValidSelection = $false
        if ($Selection -eq 'A') {
            $SelectedClusters = $Clusters
            $ValidSelection = $true
        } elseif ($Selection -match '^\d+(\s*,\s*\d+)*$') {
            $Numbers = @($Selection -split ',' | ForEach-Object { [double]$_.Trim() } | Sort-Object -Unique)
            if (@($Numbers | Where-Object { $_ -lt 1 -or $_ -gt $Clusters.Count }).Count -eq 0) {
                $SelectedClusters = @($Numbers | ForEach-Object { $Clusters[([int]$_ - 1)] })
                $ValidSelection = $true
            }
        }
        if (!$ValidSelection) { Write-Host 'Enter valid cluster numbers or A.' -ForegroundColor Yellow }
    } until ($ValidSelection)
}

$Rows = @()
$ReviewRows = @()
$CheckedPgs = 0
$CheckedRuns = 0
foreach ($Cluster in $SelectedClusters) {
    $DisplayName = Get-ClusterDisplayName $Cluster
    $ClusterId = [string]$Cluster.clusterId
    if (!$ClusterId) { throw "Cluster ID missing: $DisplayName" }
    $Headers = @{ accept='application/json'; apiKey=$ApiKey; accessClusterId=$ClusterId }
    Write-Host "Checking $DisplayName (latest $NumRuns runs per PG)..."
    try {
        $PgJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/data-protect/protection-groups?isDeleted=false" -Headers $Headers
        if (!$PgJson.PSObject.Properties['protectionGroups']) { throw 'PG response missing protectionGroups.' }
        $Pgs = @(As-Array $PgJson.protectionGroups)
        if ($ProtectionGroupName) { $Pgs = @($Pgs | Where-Object { $_.name -eq $ProtectionGroupName }) }
        if ($ProtectionGroupName -and $Pgs.Count -eq 0) { throw "PG not found: $ProtectionGroupName" }
    } catch {
        $ReviewRows += [pscustomobject]@{ Cluster=$DisplayName; ProtectionGroup=''; RunId=''; Reason=$_.Exception.Message }
        Write-Warning "PG lookup failed on $DisplayName; collection incomplete."
        continue
    }
    foreach ($Pg in $Pgs) {
        $CheckedPgs++
        try {
            if (!$Pg.id) { throw 'PG ID missing.' }
            $PgId = [uri]::EscapeDataString([string]$Pg.id)
            $Uri = "$BaseUrl/v2/data-protect/protection-groups/$PgId/runs?numRuns=$NumRuns&excludeNonRestorableRuns=false&includeObjectDetails=true"
            $RunsJson = Invoke-HeliosGetJson -Uri $Uri -Headers $Headers
            if (!$RunsJson.PSObject.Properties['runs']) { throw 'Run response missing runs.' }
            $Runs = @(As-Array $RunsJson.runs)
            foreach ($Run in $Runs) {
                $CheckedRuns++
                $ObjectsProperty = $Run.PSObject.Properties['objects']
                if (!$ObjectsProperty -or $null -eq $ObjectsProperty.Value -or !($ObjectsProperty.Value -is [array])) {
                    $ReviewRows += [pscustomobject]@{ Cluster=$DisplayName; ProtectionGroup=$Pg.name; RunId=$Run.id; Reason='objects missing, null, or not an array; zero objects not confirmed.' }
                    continue
                }
                if ($ObjectsProperty.Value.Count -ne 0) { continue }
                $Info = Get-Prop $Run 'localBackupInfo'
                $Rows += [pscustomobject]@{
                    Cluster=$DisplayName
                    Environment=$Pg.environment
                    ProtectionGroup=$Pg.name
                    ProtectionGroupId=$Pg.id
                    RunId=$Run.id
                    ObjectCount=0
                    RunType=(Get-Prop $Info 'runType' '')
                    RunStatus=(Get-Prop $Info 'status' '')
                    RunStartET=(Convert-UsecsToEtText (Get-Prop $Info 'startTimeUsecs'))
                    RunEndET=(Convert-UsecsToEtText (Get-Prop $Info 'endTimeUsecs'))
                    Message=(@(As-Array (Get-Prop $Info 'messages')) -join ' | ')
                    ClusterId=$ClusterId
                }
            }
        } catch {
            $ReviewRows += [pscustomobject]@{ Cluster=$DisplayName; ProtectionGroup=$Pg.name; RunId=''; Reason=$_.Exception.Message }
            Write-Warning "Run lookup failed: $DisplayName / $($Pg.name); collection incomplete."
        }
    }
}

New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null
$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss_fff'
$CsvPath = Join-Path $OutputRoot "Cohesity_ZeroObjectRuns_$Timestamp.csv"
$ReviewPath = Join-Path $OutputRoot "Cohesity_ZeroObjectRuns_Review_$Timestamp.csv"
$Columns = @('Cluster','Environment','ProtectionGroup','ProtectionGroupId','RunId','ObjectCount','RunType','RunStatus','RunStartET','RunEndET','Message','ClusterId')
Write-CsvRows -Rows @($Rows | Sort-Object Cluster,ProtectionGroup,RunStartET) -Path $CsvPath -Columns $Columns
Write-CsvRows -Rows $ReviewRows -Path $ReviewPath -Columns @('Cluster','ProtectionGroup','RunId','Reason')
$CollectionStatus = 'Complete'
if ($ReviewRows.Count -gt 0) { $CollectionStatus = 'Incomplete - review required' }
Write-Host "Collection: $CollectionStatus | PGs: $CheckedPgs | Runs: $CheckedRuns | Zero-object runs: $($Rows.Count) | Review items: $($ReviewRows.Count)"
Write-Host "CSV: $CsvPath"
Write-Host "Review CSV: $ReviewPath"
# No-run PGs produce no zero-object rows. Running/missed/canceled runs are retained
# with their real status; an empty run array does not prove an empty PG configuration.
