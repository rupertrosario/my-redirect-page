# === COHESITY ACTIVE PHYSICAL PG INVENTORY ===
# Branch: Cohesity_Automations
# Folder: inventory
# Cohesity API scope: GET-only
# Output folder: X:\PowerShell\Cohesity_API_Scripts\inventory

$ErrorActionPreference = 'Stop'

# ---------- CONFIG ----------
$HeliosUrl  = 'https://helios.cohesity.com'
$ApiKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt'
$OutputDir  = 'X:\PowerShell\Cohesity_API_Scripts\inventory'

# ---------- HELPERS ----------
function Invoke-CohesityGet {
    param(
        [string]$Uri,
        [hashtable]$Headers
    )

    $Response = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
    return ($Response.Content | ConvertFrom-Json)
}

function Convert-UsecsToET {
    param($Usecs)

    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) {
        return ''
    }

    try {
        $Dto = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $Tz  = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($Dto, $Tz)).ToString('yyyy-MM-dd HH:mm:ss')
    }
    catch {
        return ''
    }
}

function Join-Text {
    param($Values)

    if ($null -eq $Values) {
        return ''
    }

    return (@($Values) |
        Where-Object { $null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_) } |
        ForEach-Object { [string]$_ } |
        Select-Object -Unique) -join '; '
}

function Get-Value {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return $null
    }

    foreach ($Name in $Names) {
        if ($Object.PSObject.Properties.Name -contains $Name) {
            $Value = $Object.$Name
            if ($null -ne $Value -and -not [string]::IsNullOrWhiteSpace([string]$Value)) {
                return $Value
            }
        }
    }

    return $null
}

function Get-ClusterItemsFromResponse {
    param($Json)

    $Items = @()

    if ($Json -is [System.Array]) {
        $Items += @($Json)
    }
    else {
        foreach ($PropertyName in @('clusters', 'clusterInfo', 'clusterInfos', 'clusterInfoList', 'items', 'data')) {
            if ($Json.PSObject.Properties.Name -contains $PropertyName) {
                if ($null -ne $Json.$PropertyName) {
                    $Items += @($Json.$PropertyName)
                }
            }
        }

        if ($Items.Count -eq 0) {
            $Items += @($Json)
        }
    }

    return @($Items | Where-Object { $_ })
}

function Get-ClusterIndex {
    param(
        [string]$HeliosUrl,
        [string]$ApiKey
    )

    $Headers = @{
        apiKey = $ApiKey
        accept = 'application/json'
    }

    Write-Host 'Getting cluster index from Helios...' -ForegroundColor Yellow
    $Json = Invoke-CohesityGet -Uri "$HeliosUrl/v2/mcm/cluster-mgmt/info" -Headers $Headers

    $ClusterMap = @{}

    foreach ($Item in (Get-ClusterItemsFromResponse $Json)) {
        $ClusterId = Get-Value $Item @('clusterId', 'id', 'accessClusterId')
        $ClusterName = Get-Value $Item @('clusterName', 'name', 'displayName')

        if ([string]::IsNullOrWhiteSpace([string]$ClusterId) -and ($Item.PSObject.Properties.Name -contains 'clusterIdentifier')) {
            $ClusterId = Get-Value $Item.clusterIdentifier @('clusterId', 'id')
            if ([string]::IsNullOrWhiteSpace([string]$ClusterName)) {
                $ClusterName = Get-Value $Item.clusterIdentifier @('clusterName', 'name')
            }
        }

        if (-not [string]::IsNullOrWhiteSpace([string]$ClusterId)) {
            if ([string]::IsNullOrWhiteSpace([string]$ClusterName)) {
                $ClusterName = $ClusterId
            }

            if (-not $ClusterMap.ContainsKey([string]$ClusterId)) {
                $ClusterMap[[string]$ClusterId] = [PSCustomObject]@{
                    ClusterId   = [string]$ClusterId
                    ClusterName = [string]$ClusterName
                }
            }
        }
    }

    $Clusters = @($ClusterMap.Values | Sort-Object ClusterName)

    if ($Clusters.Count -eq 0) {
        throw 'No clusters returned from /v2/mcm/cluster-mgmt/info.'
    }

    return $Clusters
}

function Select-Clusters {
    param($Clusters)

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'

    for ($i = 0; $i -lt $Clusters.Count; $i++) {
        Write-Host ('[{0}] {1}' -f ($i + 1), $Clusters[$i].ClusterName)
    }

    Write-Host 'Examples: 1 or 1,3,5 or 2-4' -ForegroundColor DarkGray
    $Choice = (Read-Host 'Enter selection').Trim()

    if ([string]::IsNullOrWhiteSpace($Choice) -or $Choice -eq '0') {
        return @($Clusters)
    }

    $SelectedNumbers = @()

    foreach ($Part in ($Choice -split ',')) {
        $Part = $Part.Trim()

        if ($Part -match '^(\d+)\s*-\s*(\d+)$') {
            $Start = [int]$Matches[1]
            $End   = [int]$Matches[2]

            if ($Start -lt 1 -or $End -gt $Clusters.Count -or $Start -gt $End) {
                throw "Invalid cluster range: $Part"
            }

            $SelectedNumbers += $Start..$End
        }
        elseif ($Part -match '^\d+$') {
            $Number = [int]$Part

            if ($Number -lt 1 -or $Number -gt $Clusters.Count) {
                throw "Invalid cluster number: $Part"
            }

            $SelectedNumbers += $Number
        }
        else {
            throw "Invalid selection: $Part"
        }
    }

    $SelectedNumbers = $SelectedNumbers | Sort-Object -Unique
    return @($SelectedNumbers | ForEach-Object { $Clusters[$_ - 1] })
}

function Get-PolicyName {
    param(
        $PolicyId,
        [hashtable]$Headers,
        [hashtable]$PolicyCache
    )

    if ([string]::IsNullOrWhiteSpace([string]$PolicyId)) {
        return ''
    }

    if ($PolicyCache.ContainsKey([string]$PolicyId)) {
        return $PolicyCache[[string]$PolicyId]
    }

    try {
        $Json = Invoke-CohesityGet -Uri "$HeliosUrl/v2/data-protect/policies?ids=$PolicyId" -Headers $Headers
        $PolicyName = ($Json.policies | Where-Object { $_.id -eq $PolicyId } | Select-Object -ExpandProperty name -First 1)

        if ([string]::IsNullOrWhiteSpace([string]$PolicyName)) {
            $PolicyName = $PolicyId
        }

        $PolicyCache[[string]$PolicyId] = $PolicyName
        return $PolicyName
    }
    catch {
        $PolicyCache[[string]$PolicyId] = $PolicyId
        return $PolicyId
    }
}

function Get-PhysicalProtectionGroups {
    param(
        [string]$HeliosUrl,
        [hashtable]$Headers
    )

    $AllGroups = @()
    $Cookie = $null

    do {
        $Uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"

        if (-not [string]::IsNullOrWhiteSpace([string]$Cookie)) {
            $Uri = "$Uri&paginationCookie=$([uri]::EscapeDataString([string]$Cookie))"
        }

        $Json = Invoke-CohesityGet -Uri $Uri -Headers $Headers

        if ($Json.protectionGroups) {
            $AllGroups += @($Json.protectionGroups | Where-Object { $_ })
        }

        $Cookie = $Json.paginationCookie
    }
    while (-not [string]::IsNullOrWhiteSpace([string]$Cookie))

    return @($AllGroups)
}

# ---------- MAIN ----------
if (-not (Test-Path $ApiKeyFile)) {
    throw "API key file not found: $ApiKeyFile"
}

$ApiKey = (Get-Content $ApiKeyFile -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "API key file is empty: $ApiKeyFile"
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

$ClusterIndex = Get-ClusterIndex -HeliosUrl $HeliosUrl -ApiKey $ApiKey
$SelectedClusters = Select-Clusters -Clusters $ClusterIndex

$SummaryRows = @()
$DetailRows = @()
$PolicyCache = @{}

foreach ($Cluster in $SelectedClusters) {
    $Headers = @{
        apiKey          = $ApiKey
        accessClusterId = $Cluster.ClusterId
        accept          = 'application/json'
    }

    Write-Host "Collecting active Physical PGs from $($Cluster.ClusterName)..." -ForegroundColor Yellow
    $ProtectionGroups = Get-PhysicalProtectionGroups -HeliosUrl $HeliosUrl -Headers $Headers

    foreach ($Pg in ($ProtectionGroups | Where-Object { $_ })) {
        $PhysicalParams = $Pg.physicalParams
        if ($null -eq $PhysicalParams) {
            continue
        }

        $ProtectionType = $PhysicalParams.protectionType
        $FileParams = $PhysicalParams.fileProtectionTypeParams
        $VolumeParams = $PhysicalParams.volumeProtectionTypeParams

        if ($ProtectionType -eq 'kVolume') {
            $Objects = @($VolumeParams.objects | Where-Object { $_ })
            $GlobalExcludePaths = ''
            $JobExcludedVssWriters = Join-Text $VolumeParams.excludedVssWriters
        }
        else {
            $Objects = @($FileParams.objects | Where-Object { $_ })
            $GlobalExcludePaths = Join-Text $FileParams.globalExcludePaths
            $JobExcludedVssWriters = Join-Text $FileParams.excludedVssWriters
        }

        $LastRun = $Pg.lastRun
        $LocalInfo = $LastRun.localBackupInfo
        if ($null -eq $LocalInfo) {
            $LocalInfo = $LastRun.localSnapshotInfo
        }

        $LastRunStatus = $LocalInfo.status
        if ([string]::IsNullOrWhiteSpace([string]$LastRunStatus)) {
            $LastRunStatus = $LastRun.status
        }

        $EndTimeUsecs = $LocalInfo.endTimeUsecs
        if ($null -eq $EndTimeUsecs) {
            $EndTimeUsecs = $LastRun.endTimeUsecs
        }

        $PolicyName = $Pg.policyName
        if ([string]::IsNullOrWhiteSpace([string]$PolicyName)) {
            $PolicyName = Get-PolicyName -PolicyId $Pg.policyId -Headers $Headers -PolicyCache $PolicyCache
        }

        $SummaryRows += [PSCustomObject]@{
            Cluster               = $Cluster.ClusterName
            PGName                = $Pg.name
            PolicyName            = $PolicyName
            ProtectionType        = $ProtectionType
            PGObjectCount         = @($Objects).Count
            GlobalExcludePaths    = $GlobalExcludePaths
            JobExcludedVssWriters = $JobExcludedVssWriters
            IsActive              = $Pg.isActive
            IsPaused              = $Pg.isPaused
            LastRunStatus         = $LastRunStatus
            LastRunEndET          = Convert-UsecsToET $EndTimeUsecs
        }

        foreach ($Obj in $Objects) {
            $ObjectName = $Obj.name
            if ([string]::IsNullOrWhiteSpace([string]$ObjectName)) { $ObjectName = $Obj.sourceName }
            if ([string]::IsNullOrWhiteSpace([string]$ObjectName)) { $ObjectName = $Obj.hostName }
            if ([string]::IsNullOrWhiteSpace([string]$ObjectName)) { $ObjectName = $Obj.id }

            $ObjectId = $Obj.id
            if ([string]::IsNullOrWhiteSpace([string]$ObjectId)) { $ObjectId = $Obj.sourceId }

            $ObjectExcludedVssWriters = Join-Text $Obj.excludedVssWriters

            if ($ProtectionType -eq 'kVolume') {
                $DetailRows += [PSCustomObject]@{
                    Cluster                  = $Cluster.ClusterName
                    PGName                   = $Pg.name
                    PolicyName               = $PolicyName
                    ProtectionType           = $ProtectionType
                    ObjectName               = $ObjectName
                    ObjectId                 = $ObjectId
                    IncludedPath             = Join-Text $Obj.volumeGuids
                    ObjectExcludedPaths      = ''
                    SkipNestedVolumes        = ''
                    GlobalExcludePaths       = $GlobalExcludePaths
                    ObjectExcludedVssWriters = $ObjectExcludedVssWriters
                    JobExcludedVssWriters    = $JobExcludedVssWriters
                }
            }
            else {
                foreach ($FilePath in ($Obj.filePaths | Where-Object { $_ })) {
                    $DetailRows += [PSCustomObject]@{
                        Cluster                  = $Cluster.ClusterName
                        PGName                   = $Pg.name
                        PolicyName               = $PolicyName
                        ProtectionType           = $ProtectionType
                        ObjectName               = $ObjectName
                        ObjectId                 = $ObjectId
                        IncludedPath             = $FilePath.includedPath
                        ObjectExcludedPaths      = Join-Text $FilePath.excludedPaths
                        SkipNestedVolumes        = $FilePath.skipNestedVolumes
                        GlobalExcludePaths       = $GlobalExcludePaths
                        ObjectExcludedVssWriters = $ObjectExcludedVssWriters
                        JobExcludedVssWriters    = $JobExcludedVssWriters
                    }
                }
            }
        }
    }
}

$SummaryRows = $SummaryRows | Sort-Object Cluster, PGName
$DetailRows = $DetailRows | Sort-Object Cluster, PGName, ObjectName, IncludedPath

$Stamp = Get-Date -Format 'yyyy-MM-dd_HHmm'
$SummaryCsv = Join-Path $OutputDir "Physical_PG_Summary_$Stamp.csv"
$DetailCsv = Join-Path $OutputDir "Physical_PG_Object_Detail_$Stamp.csv"

$SummaryRows | Export-Csv -Path $SummaryCsv -NoTypeInformation
$DetailRows | Export-Csv -Path $DetailCsv -NoTypeInformation

Write-Host ''
Write-Host "Output folder: $OutputDir" -ForegroundColor Green
Write-Host "Summary rows : $(@($SummaryRows).Count)" -ForegroundColor Green
Write-Host "Detail rows  : $(@($DetailRows).Count)" -ForegroundColor Green
Write-Host "Summary CSV  : $SummaryCsv" -ForegroundColor Green
Write-Host "Detail CSV   : $DetailCsv" -ForegroundColor Green

$SummaryRows | Format-Table Cluster, PGName, PolicyName, ProtectionType, PGObjectCount, GlobalExcludePaths, IsPaused, LastRunStatus, LastRunEndET -AutoSize

try {
    $SummaryRows | Out-GridView -Title 'Cohesity Physical PG Summary'
}
catch {
    Write-Warning 'Out-GridView is unavailable in this PowerShell host. CSV files were still created.'
}
