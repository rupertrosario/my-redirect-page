# Cohesity Helios - DR Ready Protection Group Configuration Export
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Active PGs only. Environments: NAS, SQL, Hyper-V, Nutanix AHV, Oracle, Physical.
# Reuses the same Helios API-key/AES helper and cluster-selection pattern as the
# existing inventory scripts in this repository.

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready"
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$root                = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath          = Join-Path $root "Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = Join-Path $root "Common\Secure\cohesity_apikey.enc"

$EnvironmentMap = @(
    [pscustomobject]@{ ApiName="kGenericNas"; DisplayName="NAS";         ParamNames=@("genericNasParams") },
    [pscustomobject]@{ ApiName="kSQL";        DisplayName="SQL";         ParamNames=@("mssqlParams") },
    [pscustomobject]@{ ApiName="kHyperV";     DisplayName="Hyper-V";     ParamNames=@("hypervParams","hyperVParams") },
    [pscustomobject]@{ ApiName="kAcropolis";  DisplayName="Nutanix AHV"; ParamNames=@("acropolisParams","nutanixParams","ahvParams") },
    [pscustomobject]@{ ApiName="kOracle";     DisplayName="Oracle";      ParamNames=@("oracleParams") },
    [pscustomobject]@{ ApiName="kPhysical";   DisplayName="Physical";    ParamNames=@("physicalParams") }
)

if (-not (Test-Path $helperPath -PathType Leaf)) { throw "API key helper not found: $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key file not found: $encryptedApiKeyPath" }
if (-not (Test-Path $OutputDirectory -PathType Container)) { New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null }

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES API key helper returned an empty API key." }

function Prop {
    param($Object,[string[]]$Names,$Default=$null)
    if ($null -eq $Object -or $Object -is [string]) { return $Default }
    foreach ($name in $Names) {
        foreach ($p in @($Object.PSObject.Properties)) {
            if ($p.Name -ieq $name) {
                if ($null -ne $p.Value) { return $p.Value }
                return $Default
            }
        }
    }
    return $Default
}

function First {
    param($Values)
    foreach ($v in @($Values)) {
        foreach ($x in @($v)) {
            if ($null -ne $x -and "$x".Trim() -ne "") { return "$x" }
        }
    }
    return ""
}

function Arr { param($Value) if ($null -eq $Value) { return @() }; return @($Value) }

function Headers {
    param([string]$ClusterId)
    $h = @{ accept="application/json"; apiKey=$apiKey }
    if ($ClusterId) { $h["accessClusterId"] = $ClusterId }
    return $h
}

function GetJson {
    param([string]$Uri,[hashtable]$Headers)
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    } else {
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function WriteJson {
    param([AllowNull()]$Value,[string]$Path)
    $Value | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function SafeName {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return "UNNAMED" }
    $safe = $Value -replace '[:\\/\*\?"<>\|]+','_'
    if ($safe.Length -gt 120) { $safe = $safe.Substring(0,120) }
    return $safe.Trim()
}

# Same recursive leaf expansion pattern used by the SQL inventory test.
function ExpandLeaf {
    param($Value,[string]$Path="")
    if ($null -eq $Value) { [pscustomobject]@{Field=$Path;Value="<null>"}; return }
    if ($Value -is [string] -or $Value -is [char] -or $Value -is [bool] -or
        $Value -is [byte] -or $Value -is [sbyte] -or $Value -is [int16] -or
        $Value -is [uint16] -or $Value -is [int32] -or $Value -is [uint32] -or
        $Value -is [int64] -or $Value -is [uint64] -or $Value -is [single] -or
        $Value -is [double] -or $Value -is [decimal] -or $Value -is [datetime] -or
        $Value -is [guid]) {
        [pscustomobject]@{Field=$Path;Value=[string]$Value}; return
    }
    if ($Value -is [System.Collections.IDictionary]) {
        if (@($Value.Keys).Count -eq 0) { [pscustomobject]@{Field=$Path;Value="{}"}; return }
        foreach ($key in $Value.Keys) {
            $child = if ($Path) { "$Path.$key" } else { [string]$key }
            ExpandLeaf $Value[$key] $child
        }
        return
    }
    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @($Value)
        if ($items.Count -eq 0) { [pscustomobject]@{Field=$Path;Value="[]"}; return }
        for ($i=0; $i -lt $items.Count; $i++) { ExpandLeaf $items[$i] "$Path[$i]" }
        return
    }
    $props = @($Value.PSObject.Properties)
    if ($props.Count -eq 0) { [pscustomobject]@{Field=$Path;Value=[string]$Value}; return }
    foreach ($p in $props) {
        $child = if ($Path) { "$Path.$($p.Name)" } else { $p.Name }
        ExpandLeaf $p.Value $child
    }
}

function GetEnvBlock {
    param($PG,[string[]]$Names)
    foreach ($name in $Names) {
        $v = Prop $PG @($name) $null
        if ($null -ne $v) { return [pscustomobject]@{Name=$name;Value=$v} }
    }
    return $null
}

function GetActivePGs {
    param([string]$Environment,[hashtable]$Headers)
    $all=@(); $cookie=""
    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=$Environment&isActive=true&isDeleted=false&includeLastRunInfo=false&pruneSourceIds=false&pruneExcludedSourceIds=false&maxResultCount=1000"
        if ($cookie) { $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))" }
        $j = GetJson $uri $Headers
        $all += @(Arr (Prop $j @("protectionGroups") @()) | Where-Object { $_ })
        $cookie = First @((Prop $j @("paginationCookie") ""))
        $truncated = Prop $j @("isResponseTruncated") $false
        if ($truncated -ne $true -and -not $cookie) { break }
    } while ($cookie)
    return @($all)
}

function GetSourceRegistrations {
    param([hashtable]$Headers)
    try {
        $j = GetJson "$baseUrl/v2/data-protect/sources/registrations?includeSourceCredentials=false&includeExternalMetadata=true" $Headers
    } catch {
        $j = GetJson "$baseUrl/v2/data-protect/sources/registrations?includeSourceCredentials=false" $Headers
    }
    $regs = Prop $j @("registrations") $null
    if ($null -ne $regs) { return @(Arr $regs) }
    return @(Arr $j)
}

# Build ID -> readable source/object metadata from the returned source-registration trees.
function IndexSourceNode {
    param($Node,[hashtable]$Index,[string]$RootId,[string]$RootName,[string]$Path="")
    if ($null -eq $Node -or $Node -is [string] -or $Node -is [ValueType]) { return }
    if ($Node -is [System.Collections.IEnumerable] -and $Node -isnot [System.Collections.IDictionary]) {
        $i=0; foreach ($item in @($Node)) { IndexSourceNode $item $Index $RootId $RootName "$Path[$i]"; $i++ }; return
    }
    $id = First @((Prop $Node @("id") $null),(Prop $Node @("objectId") $null),(Prop $Node @("sourceId") $null),(Prop $Node @("entityId") $null))
    if ($id) {
        $Index[$id] = [pscustomobject]@{
            Id=$id
            Name=(First @((Prop $Node @("name","objectName","sourceName","hostName","displayName") $null)))
            Environment=(First @((Prop $Node @("environment") $null)))
            ObjectType=(First @((Prop $Node @("objectType","type") $null)))
            RootRegistrationId=$RootId
            RootRegistrationName=$RootName
            RegistrationPath=$Path
        }
    }
    foreach ($p in @($Node.PSObject.Properties)) {
        if ($null -eq $p.Value -or $p.Value -is [string] -or $p.Value -is [ValueType]) { continue }
        $child = if ($Path) { "$Path.$($p.Name)" } else { $p.Name }
        IndexSourceNode $p.Value $Index $RootId $RootName $child
    }
}

function BuildSourceIndex {
    param($Registrations)
    $index=@{}
    foreach ($r in @(Arr $Registrations | Where-Object { $_ })) {
        $si = Prop $r @("sourceInfo") $null
        $rootId = First @((Prop $r @("id","sourceId") $null),(Prop $si @("id","sourceId","entityId") $null))
        $rootName = First @((Prop $r @("name","sourceName") $null),(Prop $si @("name","sourceName","displayName") $null),$rootId)
        IndexSourceNode $r $index $rootId $rootName "registration"
    }
    return $index
}

# Source/object IDs used by the environment-specific PG configuration.
function GetIdReferences {
    param($EnvironmentParams)
    $rows=@()
    if ($null -eq $EnvironmentParams) { return @() }
    foreach ($leaf in @(ExpandLeaf $EnvironmentParams "")) {
        $field=[string]$leaf.Field; $value=[string]$leaf.Value
        $leafName=(($field -split '\.')[-1]) -replace '\[\d+\]$',''
        if ($leafName -match '(?i)(^id$|Id$|Ids$)' -and $value -match '^\d+$') {
            $rows += [pscustomobject]@{FieldPath=$field;Id=$value}
        }
    }
    return @($rows | Sort-Object FieldPath,Id -Unique)
}

# -------------------- Helios cluster menu --------------------
$clusterJson = GetJson "$baseUrl/v2/mcm/cluster-mgmt/info" (Headers)
$clusters = @(Arr (Prop $clusterJson @("cohesityClusters") @()) | ForEach-Object {
    [pscustomobject]@{
        ClusterName = First @((Prop $_ @("clusterName","displayName","name") $null))
        ClusterId   = First @((Prop $_ @("clusterId","id") $null))
    }
} | Where-Object { $_.ClusterId } | Sort-Object ClusterName)
if ($clusters.Count -eq 0) { throw "No clusters returned from Helios." }

$menu = for ($i=0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{Index=$i+1;ClusterName=$clusters[$i].ClusterName;ClusterId=$clusters[$i].ClusterId}
}
Write-Host "`nAvailable Helios Clusters:" -ForegroundColor Cyan
$menu | Format-Table -AutoSize
Write-Host "[0] All clusters`n[X] Exit" -ForegroundColor Yellow
while ($true) {
    $selection = Read-Host "Select cluster"
    if ($selection -match '^(x|X|q|Q)$') { return }
    $n=0
    if ([int]::TryParse($selection,[ref]$n) -and $n -ge 0 -and $n -le $menu.Count) {
        if ($n -eq 0) { $selectedClusters=@($menu) } else { $selectedClusters=@($menu | Where-Object {$_.Index -eq $n}) }
        break
    }
    Write-Host "Invalid selection." -ForegroundColor Red
}

# -------------------- Collection --------------------
foreach ($cluster in $selectedClusters) {
    $h = Headers $cluster.ClusterId
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $clusterDir = Join-Path $OutputDirectory ("{0}_{1}" -f (SafeName $cluster.ClusterName),$stamp)
    $pgRoot = Join-Path $clusterDir "PGs"
    New-Item $pgRoot -ItemType Directory -Force | Out-Null

    Write-Host "`nCollecting $($cluster.ClusterName) ..." -ForegroundColor Cyan

    $errors=@(); $summary=@(); $allPgFields=@(); $envFields=@(); $sourceRefs=@(); $sourceRegFields=@()

    try { $sourceRegs=@(GetSourceRegistrations $h) }
    catch { $sourceRegs=@(); $errors += [pscustomobject]@{Environment="ALL";ProtectionGroup="";Stage="SourceRegistrations";Error=$_.Exception.Message} }

    WriteJson @($sourceRegs) (Join-Path $clusterDir "Source_Registrations_All.json")
    $sourceIndex = BuildSourceIndex $sourceRegs

    foreach ($r in $sourceRegs) {
        $si=Prop $r @("sourceInfo") $null
        $rid=First @((Prop $r @("id","sourceId") $null),(Prop $si @("id","sourceId","entityId") $null))
        $rname=First @((Prop $r @("name","sourceName") $null),(Prop $si @("name","sourceName","displayName") $null),$rid)
        foreach ($leaf in @(ExpandLeaf $r "")) {
            $sourceRegFields += [pscustomobject]@{Cluster=$cluster.ClusterName;RegistrationId=$rid;RegistrationName=$rname;Field=$leaf.Field;Value=$leaf.Value}
        }
    }

    foreach ($env in $EnvironmentMap) {
        Write-Host "  $($env.DisplayName)" -ForegroundColor Yellow
        try { $pgs=@(GetActivePGs $env.ApiName $h) }
        catch { $errors += [pscustomobject]@{Environment=$env.DisplayName;ProtectionGroup="";Stage="ListActivePGs";Error=$_.Exception.Message}; continue }

        foreach ($stub in $pgs) {
            $pgId=First @((Prop $stub @("id","protectionGroupId") $null))
            $pgName=First @((Prop $stub @("name","protectionGroupName") $null),$pgId)
            if (-not $pgId) { $errors += [pscustomobject]@{Environment=$env.DisplayName;ProtectionGroup=$pgName;Stage="PGId";Error="Missing PG id"}; continue }

            try {
                $encoded=[uri]::EscapeDataString($pgId)
                $pg=GetJson "$baseUrl/v2/data-protect/protection-groups/$encoded?includeLastRunInfo=false&pruneSourceIds=false" $h
            } catch { $errors += [pscustomobject]@{Environment=$env.DisplayName;ProtectionGroup=$pgName;Stage="PGDetail";Error=$_.Exception.Message}; continue }

            if ($null -eq $pg) { continue }
            $isActive=Prop $pg @("isActive") $null; $isDeleted=Prop $pg @("isDeleted") $false
            if ($isActive -eq $false -or $isDeleted -eq $true) { continue }

            $block=GetEnvBlock $pg $env.ParamNames
            $blockName=if ($block) {$block.Name} else {"NOT_FOUND"}
            $blockValue=if ($block) {$block.Value} else {$null}

            $policyId=First @((Prop $pg @("policyId") $null))
            $policy=$null
            if ($policyId) {
                try { $policy=GetJson "$baseUrl/v2/data-protect/policies/$([uri]::EscapeDataString($policyId))" $h }
                catch { $errors += [pscustomobject]@{Environment=$env.DisplayName;ProtectionGroup=$pgName;Stage="Policy";Error=$_.Exception.Message} }
            }

            $folder=Join-Path $pgRoot ("{0}__{1}" -f (SafeName $pgName),(SafeName $pgId))
            New-Item $folder -ItemType Directory -Force | Out-Null
            WriteJson $pg (Join-Path $folder "ProtectionGroup.json")
            WriteJson $blockValue (Join-Path $folder "EnvironmentParams.json")
            WriteJson $policy (Join-Path $folder "Policy.json")

            foreach ($leaf in @(ExpandLeaf $pg "")) {
                $allPgFields += [pscustomobject]@{Cluster=$cluster.ClusterName;Environment=$env.DisplayName;ProtectionGroup=$pgName;ProtectionGroupId=$pgId;Field=$leaf.Field;Value=$leaf.Value}
            }
            foreach ($leaf in @(ExpandLeaf $blockValue "")) {
                $envFields += [pscustomobject]@{Cluster=$cluster.ClusterName;Environment=$env.DisplayName;ProtectionGroup=$pgName;ProtectionGroupId=$pgId;ParameterBlock=$blockName;Field=$leaf.Field;Value=$leaf.Value}
            }

            $pgRefs=@(); $sourceObjects=@()
            foreach ($ref in @(GetIdReferences $blockValue)) {
                $resolved=$null
                if ($sourceIndex.ContainsKey([string]$ref.Id)) { $resolved=$sourceIndex[[string]$ref.Id] }

                # Fallback: ask Cohesity directly for this source/object ID.
                if ($null -eq $resolved) {
                    try {
                        $sourceObject=GetJson "$baseUrl/v2/data-protect/sources/$($ref.Id)" $h
                        if ($sourceObject) {
                            $sourceObjects += [pscustomobject]@{ReferenceField=$ref.FieldPath;ReferenceId=$ref.Id;Source=$sourceObject}
                            $resolved=[pscustomobject]@{
                                Id=[string]$ref.Id
                                Name=First @((Prop $sourceObject @("name","objectName","sourceName","hostName","displayName") $null))
                                Environment=First @((Prop $sourceObject @("environment") $null))
                                ObjectType=First @((Prop $sourceObject @("objectType","type") $null))
                                RootRegistrationId=""
                                RootRegistrationName=""
                                RegistrationPath=""
                            }
                        }
                    } catch { }
                }

                $row=[pscustomobject]@{
                    Cluster=$cluster.ClusterName
                    Environment=$env.DisplayName
                    ProtectionGroup=$pgName
                    ProtectionGroupId=$pgId
                    ParameterBlock=$blockName
                    ReferenceField=$ref.FieldPath
                    SourceObjectId=$ref.Id
                    Resolved=($null -ne $resolved)
                    ResolvedName=if ($resolved) {$resolved.Name} else {""}
                    ResolvedEnvironment=if ($resolved) {$resolved.Environment} else {""}
                    ResolvedObjectType=if ($resolved) {$resolved.ObjectType} else {""}
                    RootRegistrationId=if ($resolved) {$resolved.RootRegistrationId} else {""}
                    RootRegistrationName=if ($resolved) {$resolved.RootRegistrationName} else {""}
                }
                $pgRefs += $row; $sourceRefs += $row
            }
            WriteJson @($pgRefs) (Join-Path $folder "SourceReferences.json")
            WriteJson @($sourceObjects) (Join-Path $folder "SourceObjects_Fallback.json")

            $summary += [pscustomobject]@{
                Cluster=$cluster.ClusterName
                ClusterId=$cluster.ClusterId
                Environment=$env.DisplayName
                EnvironmentApiName=$env.ApiName
                ProtectionGroup=$pgName
                ProtectionGroupId=$pgId
                IsActive=$isActive
                IsDeleted=$isDeleted
                IsPaused=Prop $pg @("isPaused") $null
                PolicyId=$policyId
                ParameterBlock=$blockName
                ParameterLeafCount=if ($blockValue) {@(ExpandLeaf $blockValue "").Count} else {0}
                SourceIdReferenceCount=@($pgRefs).Count
                ResolvedSourceRefCount=@($pgRefs | Where-Object {$_.Resolved}).Count
                OutputFolder=(Split-Path $folder -Leaf)
            }

            WriteJson ([ordered]@{
                ExportVersion="1.0"
                ExportedAt=(Get-Date).ToString("o")
                ReadOnly=$true
                Cluster=$cluster.ClusterName
                ClusterId=$cluster.ClusterId
                Environment=$env.DisplayName
                EnvironmentApiName=$env.ApiName
                ProtectionGroup=$pgName
                ProtectionGroupId=$pgId
                ActiveOnly=$true
                ParameterBlock=$blockName
                PolicyId=$policyId
                Files=@("ProtectionGroup.json","EnvironmentParams.json","Policy.json","SourceReferences.json","SourceObjects_Fallback.json")
                Notes=@(
                    "ProtectionGroup.json is the complete detailed GET response and is not POST-sanitized.",
                    "Source_Registrations_All.json at cluster level preserves source registration configuration; credentials are not requested.",
                    "Target-cluster DR recreation must remap source/object, policy and storage-domain IDs."
                )
            }) (Join-Path $folder "Manifest.json")
        }
    }

    $summary | Sort-Object Environment,ProtectionGroup | Export-Csv (Join-Path $clusterDir "Active_ProtectionGroups_Summary.csv") -NoTypeInformation -Encoding UTF8
    $allPgFields | Sort-Object Environment,ProtectionGroup,Field | Export-Csv (Join-Path $clusterDir "ProtectionGroup_All_Parameters.csv") -NoTypeInformation -Encoding UTF8
    $envFields | Sort-Object Environment,ProtectionGroup,Field | Export-Csv (Join-Path $clusterDir "Environment_Configured_Parameters.csv") -NoTypeInformation -Encoding UTF8
    $sourceRefs | Sort-Object Environment,ProtectionGroup,ReferenceField | Export-Csv (Join-Path $clusterDir "PG_Source_References.csv") -NoTypeInformation -Encoding UTF8
    $sourceRegFields | Sort-Object RegistrationName,Field | Export-Csv (Join-Path $clusterDir "Source_Registration_All_Parameters.csv") -NoTypeInformation -Encoding UTF8
    $errors | Export-Csv (Join-Path $clusterDir "Collection_Errors.csv") -NoTypeInformation -Encoding UTF8

    WriteJson ([ordered]@{
        ExportVersion="1.0"
        ExportedAt=(Get-Date).ToString("o")
        Script="Get-CohesityDRReadyPGConfig.ps1"
        HeliosBaseUrl=$baseUrl
        Cluster=$cluster.ClusterName
        ClusterId=$cluster.ClusterId
        ActiveOnly=$true
        DeletedIncluded=$false
        Environments=@($EnvironmentMap | ForEach-Object {$_.DisplayName})
        ProtectionGroupCount=@($summary).Count
        CountsByEnvironment=@($summary | Group-Object Environment | ForEach-Object {[pscustomobject]@{Environment=$_.Name;Count=$_.Count}})
        SourceRegistrationCount=@($sourceRegs).Count
        CollectionErrorCount=@($errors).Count
        OutputDirectory=$clusterDir
        Safety="GET-only. No POST, PUT, PATCH, DELETE, pause, resume, activate or deactivate calls."
    }) (Join-Path $clusterDir "Run_Metadata.json")

    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green
    Write-Host "Active PGs exported: $(@($summary).Count)" -ForegroundColor Green
    if (@($errors).Count -gt 0) { Write-Host "Warnings/errors: $(@($errors).Count) - see Collection_Errors.csv" -ForegroundColor Yellow }
}
