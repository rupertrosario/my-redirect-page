# === COHESITY ACTIVE PHYSICAL PG INVENTORY ===
# Branch: Cohesity_Automations
# Folder: inventory
# Cohesity API scope: GET-only

$ErrorActionPreference = 'Stop'

# ---------- CONFIG ----------
$HeliosUrl  = 'https://helios.cohesity.com'
$ApiKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt'
$OutputDir  = $PSScriptRoot

# Put clusters here in this exact format: "clusterId clusterName"
# Example:
# $ClustersAvailable = @(
#     "1234567890123456789 ClusterName01"
#     "9876543210987654321 ClusterName02"
# )
$ClustersAvailable = @(
)

# ---------- HELPERS ----------
function Convert-UsecsToET {
    param($Usecs)

    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) {
        return ''
    }

    try {
        $DateTimeOffset = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $TimeZone = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($DateTimeOffset, $TimeZone)).ToString('yyyy-MM-dd HH:mm:ss')
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
        Select-Object -Unique) -join '; '
}

function Get-SelectedClusters {
    param($ClustersAvailable)

    if ($null -eq $ClustersAvailable -or @($ClustersAvailable).Count -eq 0) {
        throw 'No clusters configured. Add cluster entries to $ClustersAvailable at the top of the script.'
    }

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'

    for ($i = 0; $i -lt @($ClustersAvailable).Count; $i++) {
        $ClusterId, $ClusterName = $ClustersAvailable[$i] -split ' ', 2
        Write-Host ('[{0}] {1}' -f ($i + 1), $ClusterName)
    }

    Write-Host 'Examples: 1 or 1,3,5 or 2-4' -ForegroundColor DarkGray
    $Choice = (Read-Host 'Enter selection').Trim()

    if ($Choice -eq '0') {
        return @($ClustersAvailable)
    }

    $SelectedNumbers = @()

    foreach ($Part in ($Choice -split ',')) {
        $Part = $Part.Trim()

        if ($Part -match '^(\d+)\s*-\s*(\d+)$') {
            $Start = [int]$Matches[1]
            $End   = [int]$Matches[2]

            if ($Start -lt 1 -or $End -gt @($ClustersAvailable).Count -or $Start -gt $End) {
                throw "Invalid cluster range: $Part"
            }

            $SelectedNumbers += $Start..$End
        }
        elseif ($Part -match '^\d+$') {
            $Number = [int]$Part

            if ($Number -lt 1 -or $Number -gt @($ClustersAvailable).Count) {
                throw "Invalid cluster number: $Part"
            }

            $SelectedNumbers += $Number
        }
        else {
            throw "Invalid selection: $Part"
        }
    }

    $SelectedNumbers = $SelectedNumbers | Sort-Object -Unique
    return @($SelectedNumbers | ForEach-Object { $ClustersAvailable[$_ - 1] })
}

function Get-PolicyName {
    param($PolicyId, $Headers, $PolicyCache)

    if ([string]::IsNullOrWhiteSpace([string]$PolicyId)) {
        return ''
    }

    if ($PolicyCache.ContainsKey($PolicyId)) {
        return $PolicyCache[$PolicyId]
    }

    try {
        $PolicyUri = "$HeliosUrl/v2/data-protect/policies?ids=$PolicyId"
        $PolicyResponse = Invoke-WebRequest -Method Get -Uri $PolicyUri -Headers $Headers
        $PolicyJson = $PolicyResponse.Content | ConvertFrom-Json
        $PolicyName = ($PolicyJson.policies | Where-Object { $_.id -eq $PolicyId } | Select-Object -ExpandProperty name -First 1)

        if ([string]::IsNullOrWhiteSpace([string]$PolicyName)) {
            $PolicyName = $PolicyId
        }

        $PolicyCache[$PolicyId] = $PolicyName
        return $PolicyName
    }
    catch {
        $PolicyCache[$PolicyId] = $PolicyId
        return $PolicyId
    }
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

$SelectedClusters = Get-SelectedClusters $ClustersAvailable
$SummaryRows = @()
$PolicyCache = @{}

foreach ($Cluster in $SelectedClusters) {
    $ClusterId, $ClusterName = $Cluster -split ' ', 2

    $Headers = @{
        apiKey          = $ApiKey
        accessClusterId = $ClusterId
        accept          = 'application/json'
    }

    Write-Host "Collecting active Physical PGs from $ClusterName ..." -ForegroundColor Yellow

    $Uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
    $Response = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers
    $Json = $Response.Content | ConvertFrom-Json

    foreach ($Pg in ($Json.protectionGroups | Where-Object { $_ })) {
        $PhysicalParams = $Pg.physicalParams
        $ProtectionType = $PhysicalParams.protectionType

        $FileParams = $PhysicalParams.fileProtectionTypeParams
        $VolumeParams = $PhysicalParams.volumeProtectionTypeParams

        if ($ProtectionType -eq 'kVolume') {
            $Objects = @($VolumeParams.objects | Where-Object { $_ })
            $GlobalExcludePaths = ''
        }
        else {
            $Objects = @($FileParams.objects | Where-Object { $_ })
            $GlobalExcludePaths = Join-Text $FileParams.globalExcludePaths
        }

        $LastRun = $Pg.lastRun
        $LocalBackupInfo = $LastRun.localBackupInfo
        if ($null -eq $LocalBackupInfo) {
            $LocalBackupInfo = $LastRun.localSnapshotInfo
        }

        $LastRunStatus = $LocalBackupInfo.status
        if ([string]::IsNullOrWhiteSpace([string]$LastRunStatus)) {
            $LastRunStatus = $LastRun.status
        }

        $EndTimeUsecs = $LocalBackupInfo.endTimeUsecs
        if ($null -eq $EndTimeUsecs) {
            $EndTimeUsecs = $LastRun.endTimeUsecs
        }

        $PolicyName = $Pg.policyName
        if ([string]::IsNullOrWhiteSpace([string]$PolicyName)) {
            $PolicyName = Get-PolicyName -PolicyId $Pg.policyId -Headers $Headers -PolicyCache $PolicyCache
        }

        $SummaryRows += [PSCustomObject]@{
            Cluster            = $ClusterName
            PGName             = $Pg.name
            PolicyName         = $PolicyName
            ProtectionType     = $ProtectionType
            PGObjectCount      = @($Objects).Count
            GlobalExcludePaths = $GlobalExcludePaths
            IsActive           = $Pg.isActive
            IsPaused           = $Pg.isPaused
            LastRunStatus      = $LastRunStatus
            LastRunEndET       = Convert-UsecsToET $EndTimeUsecs
        }
    }
}

$SummaryRows = $SummaryRows | Sort-Object Cluster, PGName
$Stamp = Get-Date -Format 'yyyy-MM-dd_HHmm'
$SummaryCsv = Join-Path $OutputDir "Physical_PG_Summary_$Stamp.csv"
$SummaryRows | Export-Csv -Path $SummaryCsv -NoTypeInformation

Write-Host ''
Write-Host "Summary rows: $(@($SummaryRows).Count)" -ForegroundColor Green
Write-Host "Summary CSV : $SummaryCsv" -ForegroundColor Green

$SummaryRows | Format-Table Cluster, PGName, PolicyName, ProtectionType, PGObjectCount, GlobalExcludePaths, IsPaused, LastRunStatus, LastRunEndET -AutoSize
$SummaryRows | Out-GridView -Title 'Cohesity Physical PG Summary'
