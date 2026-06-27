# ==========================================================
# CR Backup Status Report
# GET ONLY | FS + SQL + ORACLE + HYPERV + NUTANIX + VM
#
# Endpoints:
# 1) GET /v2/mcm/cluster-mgmt/info
# 2) GET /v2/data-protect/search/objects?searchString=<CI>&includeTenants=true&count=100
# 3) GET /v2/data-protect/search/protected-objects?searchString=<CI_OR_SHORTNAME>
#
# Output:
# ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup, LastBackupTime
#
# Rules:
# - SQL SourceName    = objects.mssqlParams.hostInfo.name first
# - Oracle SourceName = objects.oracleParams.hostInfo.name ONLY
# - Oracle rows without oracleParams.hostInfo.name are skipped
# - Oracle container rows like Oracle Servers/kRACDatabase are not displayed
# - Nutanix/AHV is classified as Nutanix
# - Hyper-V is classified as HyperV
# - Shortname + FQDN are merged into one CI identity
# - DB@server CI values are split for DB and server-level search
# - DB/CN fallback is only for SQL and Oracle
# - FS is not classified as DB only because name/FQDN contains DB
# - No BackupLocation
# - No replication columns
# - No SyntheticReason
# - No Debug CSV
# ==========================================================

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# -------------------------
# Input
# -------------------------
$ChangeID   = Read-Host "Enter Change Number"
$ExecutedBy = (whoami).Trim()

# -------------------------
# Paths
# -------------------------
$BaseDir   = "X:\PowerShell\Data\Cohesity\BackupValidation"
$InputCsv  = Join-Path $BaseDir "$ChangeID.csv"
$InputTxt  = Join-Path $BaseDir "$ChangeID.txt"
$TimeStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$OutputTxt = Join-Path $BaseDir "${ChangeID}_CRBackupStatus_$TimeStamp.txt"
$OutputCsv = Join-Path $BaseDir "${ChangeID}_CRBackupStatus_$TimeStamp.csv"

if (-not (Test-Path -Path $BaseDir -PathType Container)) {
    New-Item -Path $BaseDir -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path $InputCsv)) {
    throw "CSV not found: $InputCsv"
}

# -------------------------
# API
# -------------------------
$BaseUrl    = "https://helios.cohesity.com"
$ApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

if (-not (Test-Path $ApiKeyPath)) {
    throw "API key file not found: $ApiKeyPath"
}

$ApiKey = (Get-Content -Path $ApiKeyPath -Raw).Trim()

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "API key file is empty: $ApiKeyPath"
}

# -------------------------
# Settings
# -------------------------
$GlobalSearchCount                     = 100
$FallbackWhenNoGlobalObject            = $true
$FallbackWhenDbMissingForDbNamedServer = $true

# Only SQL/Oracle fallback uses this pattern.
$DbServerNamePattern = '(?i)db|cn'

$ShowProgress     = $true
$ReportTimeZoneId = "Eastern Standard Time"

# ==========================================================
# Basic Helpers
# ==========================================================
function Invoke-HeliosJson {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][hashtable]$Headers,
        [int]$TimeoutSec = 90,
        [switch]$Quiet
    )

    try {
        $resp = Invoke-WebRequest `
            -UseBasicParsing `
            -Method Get `
            -Uri $Uri `
            -Headers $Headers `
            -TimeoutSec $TimeoutSec `
            -ErrorAction Stop

        if ([string]::IsNullOrWhiteSpace($resp.Content)) {
            return $null
        }

        return ($resp.Content | ConvertFrom-Json)
    }
    catch {
        if (-not $Quiet) {
            Write-Host "GET failed:" -ForegroundColor DarkRed
            Write-Host $Uri -ForegroundColor DarkRed
            Write-Host $_.Exception.Message -ForegroundColor DarkRed
        }

        return $null
    }
}

function As-Array {
    param($Value)

    if ($null -eq $Value) {
        return @()
    }

    if ($Value -is [array]) {
        return @($Value)
    }

    return @($Value)
}

function First-NonBlank {
    param($Values)

    foreach ($v in (As-Array -Value $Values)) {
        if ($null -eq $v) {
            continue
        }

        if ($v -is [array]) {
            foreach ($x in $v) {
                $t = First-NonBlank -Values $x
                if (-not [string]::IsNullOrWhiteSpace($t)) {
                    return "$t".Trim()
                }
            }
            continue
        }

        if ($v -is [pscustomobject]) {
            foreach ($p in @(
                "display_value",
                "displayName",
                "name",
                "value",
                "id",
                "uid",
                "_id",
                "clusterId",
                "clusterID",
                "sourceClusterId",
                "sourceClusterID",
                "accessClusterId",
                "protectionGroupId",
                "groupId",
                "jobId"
            )) {
                if ($v.PSObject.Properties.Name -contains $p) {
                    $t = First-NonBlank -Values $v.$p
                    if (-not [string]::IsNullOrWhiteSpace($t)) {
                        return "$t".Trim()
                    }
                }
            }

            continue
        }

        $s = "$v".Trim()
        if (-not [string]::IsNullOrWhiteSpace($s)) {
            return $s
        }
    }

    return ""
}

function Normalize-Name {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }

    return ($Value.Trim().Trim('"').Trim("'").ToLowerInvariant())
}

function Get-ShortName {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }

    $v = $Value.Trim()

    if ($v.Contains(".")) {
        return $v.Split(".")[0]
    }

    return $v
}

function Test-TextMatchesName {
    param(
        [string]$Text,
        [string]$Name
    )

    $t = Normalize-Name $Text
    $n = Normalize-Name $Name

    if ([string]::IsNullOrWhiteSpace($t) -or [string]::IsNullOrWhiteSpace($n)) {
        return $false
    }

    $short = Normalize-Name (Get-ShortName -Value $Name)

    return (
        $t -eq $n -or
        $t -eq $short -or
        $t.Contains($n) -or
        (-not [string]::IsNullOrWhiteSpace($short) -and $t.Contains($short))
    )
}

function Test-OracleContainerName {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return $false
    }

    return (
        $Name -match '^Oracle\s+Servers($|/)' -or
        $Name -match '(^|/)(kRACDatabase|kNonRACDatabase|kOracleDatabase)$'
    )
}

function Is-BadCiName {
    param([string]$Ci)

    if ([string]::IsNullOrWhiteSpace($Ci)) {
        return $true
    }

    if ($Ci.Trim().ToUpperInvariant() -eq "N/A") {
        return $true
    }

    if ($Ci -match '^https?://') {
        return $true
    }

    if ($Ci -match '^[0-9a-fA-F]{32}$') {
        return $true
    }

    return $false
}

function Convert-UsecsToEtString {
    param($Usecs)

    try {
        if ($null -eq $Usecs) {
            return ""
        }

        $n = [int64]$Usecs
        if ($n -le 0) {
            return ""
        }

        $utc = ([DateTimeOffset]::FromUnixTimeMilliseconds([int64]($n / 1000))).UtcDateTime
        $tz  = [System.TimeZoneInfo]::FindSystemTimeZoneById($ReportTimeZoneId)
        $et  = [System.TimeZoneInfo]::ConvertTimeFromUtc($utc, $tz)

        return $et.ToString("yyyy-MM-dd HH:mm:ss")
    }
    catch {
        return ""
    }
}

function Is-ValidBackupTime {
    param([string]$TimeText)

    return ($TimeText -notin @(
        "",
        $null,
        "NoBackup",
        "NoBackupTime",
        "NoObject",
        "NoFSBackupFound",
        "NoDbBackupFound"
    ))
}

function Get-DbAtServerParts {
    param([string]$Ci)

    if ([string]::IsNullOrWhiteSpace($Ci)) {
        return [pscustomobject]@{
            IsDbAtServer = $false
            DbName       = ""
            ServerName   = ""
        }
    }

    $v = "$Ci".Trim()

    if ($v -notmatch "@") {
        return [pscustomobject]@{
            IsDbAtServer = $false
            DbName       = ""
            ServerName   = ""
        }
    }

    $parts = $v -split "@", 2
    $dbName = "$($parts[0])".Trim()
    $server = "$($parts[1])".Trim()

    if ([string]::IsNullOrWhiteSpace($dbName) -or
        [string]::IsNullOrWhiteSpace($server)) {
        return [pscustomobject]@{
            IsDbAtServer = $false
            DbName       = ""
            ServerName   = ""
        }
    }

    return [pscustomobject]@{
        IsDbAtServer = $true
        DbName       = $dbName
        ServerName   = $server
    }
}

function Test-CiLooksLikeDbServer {
    param([string]$Ci)

    if ([string]::IsNullOrWhiteSpace($Ci)) {
        return $false
    }

    $parts = Get-DbAtServerParts -Ci $Ci

    if ($parts.IsDbAtServer) {
        return $true
    }

    return ($Ci -match $DbServerNamePattern)
}

function Get-CiIdentityKey {
    param([string]$Ci)

    if ([string]::IsNullOrWhiteSpace($Ci)) {
        return ""
    }

    $parts = Get-DbAtServerParts -Ci $Ci

    if ($parts.IsDbAtServer) {
        return (Normalize-Name (Get-ShortName -Value $parts.ServerName))
    }

    return (Normalize-Name (Get-ShortName -Value $Ci))
}

function Get-PreferredCiDisplayName {
    param([string[]]$Aliases)

    $valid = @(
        $Aliases |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        ForEach-Object { "$_".Trim() } |
        Sort-Object -Unique
    )

    if (-not $valid -or $valid.Count -eq 0) {
        return ""
    }

    $serverNames = @()

    foreach ($v in $valid) {
        $parts = Get-DbAtServerParts -Ci $v

        if ($parts.IsDbAtServer) {
            $serverNames += $parts.ServerName
        }
        else {
            $serverNames += $v
        }
    }

    $serverNames = @(
        $serverNames |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Sort-Object -Unique
    )

    $fqdn = @($serverNames | Where-Object { $_ -match "\." } | Select-Object -First 1)

    if ($fqdn.Count -gt 0) {
        return $fqdn[0]
    }

    return $serverNames[0]
}

function Get-SearchTermsForCiAliases {
    param([string[]]$Aliases)

    $terms = @()

    foreach ($a in @($Aliases)) {
        if ([string]::IsNullOrWhiteSpace($a)) {
            continue
        }

        $alias = "$a".Trim()
        $parts = Get-DbAtServerParts -Ci $alias

        if ($parts.IsDbAtServer) {
            # Preserve exact CR CI value and also search by server and DB names.
            $terms += $alias
            $terms += $parts.ServerName
            $terms += Get-ShortName -Value $parts.ServerName
            $terms += $parts.DbName
        }
        else {
            $terms += $alias
            $terms += Get-ShortName -Value $alias
        }
    }

    return @(
        $terms |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Sort-Object -Unique
    )
}

# ==========================================================
# Cluster Helpers
# ==========================================================
function Add-ClusterMapEntry {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Map,
        $Id,
        [string]$ClusterName
    )

    $txt = First-NonBlank -Values @($Id)

    if (-not [string]::IsNullOrWhiteSpace($txt)) {
        $Map["$txt"] = $ClusterName
    }
}

function Get-ClusterNameFromMap {
    param(
        [string]$ClusterId,
        [hashtable]$ClusterMap
    )

    if ([string]::IsNullOrWhiteSpace($ClusterId)) {
        return "-"
    }

    if ($ClusterMap.ContainsKey("$ClusterId")) {
        return "$($ClusterMap["$ClusterId"])"
    }

    return "$ClusterId"
}

function Add-CandidateCluster {
    param(
        [System.Collections.ArrayList]$List,
        [hashtable]$Seen,
        [string]$ClusterId,
        [string]$ClusterName,
        [string]$SearchMode
    )

    if ([string]::IsNullOrWhiteSpace($ClusterId)) {
        return
    }

    if ($Seen.ContainsKey("$ClusterId")) {
        return
    }

    if ([string]::IsNullOrWhiteSpace($ClusterName)) {
        $ClusterName = $ClusterId
    }

    $Seen["$ClusterId"] = $true

    [void]$List.Add([pscustomobject]@{
        clusterId   = "$ClusterId"
        clusterName = "$ClusterName"
        searchMode  = "$SearchMode"
    })
}

function Get-ClusterIdFromSearchNode {
    param($Node)

    if ($null -eq $Node) {
        return ""
    }

    return First-NonBlank -Values @(
        $Node.clusterId,
        $Node.clusterID,
        $Node.accessClusterId,
        $Node.sourceClusterId,
        $Node.sourceClusterID,
        $Node.cluster.id,
        $Node.cluster.clusterId,
        $Node.cluster.clusterID,
        $Node.clusterInfo.id,
        $Node.clusterInfo.clusterId,
        $Node.clusterInfo.clusterID
    )
}

# ==========================================================
# Snapshot / PG Helpers
# ==========================================================
function Get-SnapshotRunType {
    param($Snapshot)

    return First-NonBlank -Values @(
        $Snapshot.runType,
        $Snapshot.backupRunType
    )
}

function Test-RegularSnapshot {
    param($Snapshot)

    $rt = Get-SnapshotRunType -Snapshot $Snapshot

    if ([string]::IsNullOrWhiteSpace($rt)) {
        return $true
    }

    return ($rt -notmatch 'log|archive')
}

function Get-SnapshotUsecs {
    param($Snapshot)

    $values = @()

    if ($null -eq $Snapshot) {
        return 0
    }

    $values += $Snapshot.protectionRunStartTimeUsecs
    $values += $Snapshot.runStartTimeUsecs
    $values += $Snapshot.startTimeUsecs
    $values += $Snapshot.snapshotTimestampUsecs
    $values += $Snapshot.endTimeUsecs

    foreach ($l in (As-Array -Value $Snapshot.localSnapshotInfo)) {
        $values += $l.snapshotInfo.endTimeUsecs
        $values += $l.snapshotInfo.snapshotTimestampUsecs
        $values += $l.snapshotInfo.startTimeUsecs
    }

    foreach ($a in (As-Array -Value $Snapshot.archivalSnapshotsInfo)) {
        $values += $a.snapshotInfo.endTimeUsecs
        $values += $a.snapshotInfo.snapshotTimestampUsecs
        $values += $a.snapshotInfo.startTimeUsecs
    }

    $max = 0

    foreach ($v in $values) {
        if ($null -eq $v) {
            continue
        }

        try {
            $n = [int64]$v
            if ($n -gt $max) {
                $max = $n
            }
        }
        catch {
        }
    }

    return $max
}

function Get-BestSnapshot {
    param($Object)

    $snapshots = @(As-Array -Value $Object.latestSnapshotsInfo)

    if (-not $snapshots -or $snapshots.Count -eq 0) {
        return $null
    }

    $regular = @($snapshots | Where-Object { Test-RegularSnapshot -Snapshot $_ })

    if ($regular.Count -gt 0) {
        return @($regular | Sort-Object { Get-SnapshotUsecs -Snapshot $_ } -Descending | Select-Object -First 1)[0]
    }

    return @($snapshots | Sort-Object { Get-SnapshotUsecs -Snapshot $_ } -Descending | Select-Object -First 1)[0]
}

function Get-ProtectionGroupName {
    param(
        $Object,
        $Snapshot
    )

    $pg = First-NonBlank -Values @(
        $Snapshot.protectionGroupName,
        $Snapshot.protectionGroup.name,
        $Snapshot.protectionGroupInfo.name,
        $Object.protectionGroupName,
        $Object.protectionGroup.name,
        $Object.protectionGroupInfo.name
    )

    if ([string]::IsNullOrWhiteSpace($pg)) {
        return "-"
    }

    return $pg
}

# ==========================================================
# Search Object Helpers
# ==========================================================
function Get-GlobalObjects {
    param($Json)

    if ($null -eq $Json) {
        return @()
    }

    if ($Json -is [array]) {
        return @($Json)
    }

    foreach ($p in @("objects", "searchResults", "results", "entities", "items", "data")) {
        if ($Json.PSObject.Properties.Name -contains $p) {
            $v = $Json.$p
            if ($v -is [array]) {
                return @($v)
            }
        }
    }

    return @()
}

function Get-ProtectedObjects {
    param($Json)

    if ($null -eq $Json) {
        return @()
    }

    if ($Json -is [array]) {
        return @($Json)
    }

    if ($Json.PSObject.Properties.Name -contains "objects") {
        return @(As-Array -Value $Json.objects)
    }

    return @()
}

function Get-ObjectNameFromNode {
    param($Object)

    return First-NonBlank -Values @(
        $Object.name,
        $Object.displayName,
        $Object.databaseName,
        $Object.dbName,
        $Object.dbUniqueName
    )
}

function Get-ObjectTypeFromNode {
    param($Object)

    return First-NonBlank -Values @(
        $Object.objectType,
        $Object.type,
        $Object.entityType
    )
}

function Get-EnvironmentFromNode {
    param(
        $Object,
        [string]$ParentEnvironment
    )

    return First-NonBlank -Values @(
        $Object.environment,
        $Object.sourceInfo.environment,
        $ParentEnvironment
    )
}

function Get-SqlHostNameFromNode {
    param($Object)

    return First-NonBlank -Values @(
        $Object.mssqlParams.hostInfo.name,
        $Object.mssqlParams.hostInfo.displayName,
        $Object.mssqlParams.hostInfo.entity.name,
        $Object.mssqlParams.hostInfo.entity.displayName,
        $Object.sqlParams.hostInfo.name,
        $Object.sqlParams.hostInfo.displayName,
        $Object.sqlParams.hostInfo.entity.name,
        $Object.sqlParams.hostInfo.entity.displayName
    )
}

function Get-OracleHostNameFromNode {
    param($Object)

    return First-NonBlank -Values @(
        $Object.oracleParams.hostInfo.name,
        $Object.oracleParams.hostInfo.displayName,
        $Object.oracleParams.hostInfo.entity.name,
        $Object.oracleParams.hostInfo.entity.displayName
    )
}

function Get-GenericSourceNameFromNode {
    param($Object)

    return First-NonBlank -Values @(
        $Object.hostInfo.name,
        $Object.hostInfo.displayName,
        $Object.hostInfo.entity.name,
        $Object.hostInfo.entity.displayName,
        $Object.sourceInfo.name,
        $Object.sourceInfo.displayName,
        $Object.sourceInfo.entity.name,
        $Object.sourceInfo.entity.displayName,
        $Object.sourceName,
        $Object.hostName,
        $Object.serverName
    )
}

function Get-VmBackupTypeFromText {
    param(
        [string]$Environment,
        [string]$ObjectType,
        [string]$ObjectName,
        [string]$SourceName,
        [string]$SourceInfoName
    )

    $text = "$Environment $ObjectType $ObjectName $SourceName $SourceInfoName"

    if ($text -match 'kAcropolis|Acropolis|Nutanix|AHV') {
        return "Nutanix"
    }

    if ($text -match 'kHyperV|HyperV|Hyper-V') {
        return "HyperV"
    }

    if ($text -match 'kVMware|VMware|kVirtualMachine|VirtualMachine') {
        return "VM"
    }

    return ""
}

function Resolve-SourceName {
    param(
        [string]$BackupType,
        [string]$ObjectName,
        [string]$ParentName,
        [string]$SqlHostName,
        [string]$OracleHostName,
        [string]$GenericSourceName,
        [string]$ParentSourceName
    )

    if ($BackupType -eq "Oracle") {
        return $OracleHostName
    }

    if ($BackupType -eq "SQL" -and -not [string]::IsNullOrWhiteSpace($SqlHostName)) {
        return $SqlHostName
    }

    if (-not [string]::IsNullOrWhiteSpace($GenericSourceName) -and -not (Test-OracleContainerName -Name $GenericSourceName)) {
        return $GenericSourceName
    }

    if (-not [string]::IsNullOrWhiteSpace($ParentSourceName) -and -not (Test-OracleContainerName -Name $ParentSourceName)) {
        return $ParentSourceName
    }

    if (-not [string]::IsNullOrWhiteSpace($ObjectName) -and $ObjectName.Contains("/")) {
        $prefix = $ObjectName.Split("/", 2)[0].Trim()
        if (-not (Test-OracleContainerName -Name $prefix)) {
            return $prefix
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($ParentName) -and -not (Test-OracleContainerName -Name $ParentName)) {
        return $ParentName
    }

    return $ObjectName
}

function Get-PreBackupType {
    param(
        [string]$Environment,
        [string]$ObjectType,
        [string]$ObjectName,
        [string]$SqlHostName,
        [string]$OracleHostName,
        [string]$GenericSourceName
    )

    if (-not [string]::IsNullOrWhiteSpace($SqlHostName)) {
        return "SQL"
    }

    if (-not [string]::IsNullOrWhiteSpace($OracleHostName)) {
        return "Oracle"
    }

    $vmType = Get-VmBackupTypeFromText -Environment $Environment -ObjectType $ObjectType -ObjectName $ObjectName -SourceName $GenericSourceName -SourceInfoName ""

    if (-not [string]::IsNullOrWhiteSpace($vmType)) {
        return $vmType
    }

    if ("$Environment $ObjectType" -match 'kOracle') {
        return "Oracle"
    }

    if ("$Environment $ObjectType" -match 'kSQL') {
        return "SQL"
    }

    return "FS"
}

function Test-ValidDbName {
    param(
        [string]$Name,
        [string]$ServerName
    )

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return $false
    }

    if (Test-OracleContainerName -Name $Name) {
        return $false
    }

    $bad = @(
        "database",
        "databases",
        "db",
        "name",
        "mssql",
        "sql",
        "oracle",
        "source",
        "server",
        "object",
        "objects",
        "Oracle Servers",
        "kRACDatabase",
        "kNonRACDatabase",
        "kOracleDatabase"
    )

    if ((Normalize-Name $Name) -in @($bad | ForEach-Object { Normalize-Name $_ })) {
        return $false
    }

    if (Test-TextMatchesName -Text $Name -Name $ServerName) {
        return $false
    }

    return $true
}

function Find-DatabaseNames {
    param(
        $Value,
        [string]$ServerName,
        [int]$Depth = 0
    )

    if ($Depth -gt 8 -or $null -eq $Value) {
        return @()
    }

    $out = @()

    if ($Value -is [array]) {
        foreach ($item in $Value) {
            $out += Find-DatabaseNames -Value $item -ServerName $ServerName -Depth ($Depth + 1)
        }
        return @($out | Sort-Object -Unique)
    }

    if ($Value -isnot [pscustomobject]) {
        return @()
    }

    $dbName = First-NonBlank -Values @(
        $Value.databaseName,
        $Value.dbName,
        $Value.name,
        $Value.displayName
    )

    if (Test-ValidDbName -Name $dbName -ServerName $ServerName) {
        $out += $dbName
    }

    foreach ($prop in $Value.PSObject.Properties) {
        if ($prop.Name -match 'mssql|sql|oracle|database|databases|db|params|objects|children|instances|list|info') {
            $out += Find-DatabaseNames -Value $prop.Value -ServerName $ServerName -Depth ($Depth + 1)
        }
    }

    return @($out | Sort-Object -Unique)
}

function Get-ParamDbRows {
    param(
        $Object,
        [string]$SourceName,
        [string]$ParentName,
        [string]$Environment,
        [int]$Depth
    )

    $rows = @()

    if ([string]::IsNullOrWhiteSpace($SourceName) -or (Test-OracleContainerName -Name $SourceName)) {
        return @()
    }

    $sqlHostName    = Get-SqlHostNameFromNode -Object $Object
    $oracleHostName = Get-OracleHostNameFromNode -Object $Object

    if ([string]::IsNullOrWhiteSpace($sqlHostName) -and [string]::IsNullOrWhiteSpace($oracleHostName)) {
        return @()
    }

    $containers = @(
        $Object.mssqlParams,
        $Object.sqlParams,
        $Object.mssql,
        $Object.oracleParams
    )

    $dbNames = @()

    foreach ($container in $containers) {
        $dbNames += Find-DatabaseNames -Value $container -ServerName $SourceName
    }

    $dbNames = @(
        $dbNames |
        Where-Object { Test-ValidDbName -Name $_ -ServerName $SourceName } |
        Sort-Object -Unique
    )

    foreach ($db in $dbNames) {
        $fullName = "$db".Trim()

        if ([string]::IsNullOrWhiteSpace($fullName)) {
            continue
        }

        if (-not $fullName.Contains("/")) {
            $fullName = "$SourceName/$fullName"
        }

        if (Test-OracleContainerName -Name $fullName) {
            continue
        }

        $rows += [pscustomobject]@{
            Object            = $Object
            ObjectName        = $fullName
            ParentName        = $ParentName
            ParentEnvironment = $Environment
            Environment       = $Environment
            ObjectType        = "kDatabase"
            SqlHostName       = $sqlHostName
            OracleHostName    = $oracleHostName
            GenericSourceName = Get-GenericSourceNameFromNode -Object $Object
            SourceName        = $SourceName
            SourceInfoName    = $SourceName
            Depth             = $Depth + 1
        }
    }

    return @($rows)
}

function Get-FlatProtectedObjects {
    param(
        $Object,
        [string]$ParentName = "",
        [string]$ParentEnvironment = "",
        [string]$ParentSourceName = "",
        [int]$Depth = 0
    )

    $rows = @()

    if ($null -eq $Object) {
        return @()
    }

    $objectName        = Get-ObjectNameFromNode -Object $Object
    $env               = Get-EnvironmentFromNode -Object $Object -ParentEnvironment $ParentEnvironment
    $objectType        = Get-ObjectTypeFromNode -Object $Object
    $sqlHostName       = Get-SqlHostNameFromNode -Object $Object
    $oracleHostName    = Get-OracleHostNameFromNode -Object $Object
    $genericSourceName = Get-GenericSourceNameFromNode -Object $Object

    $preType = Get-PreBackupType `
        -Environment $env `
        -ObjectType $objectType `
        -ObjectName $objectName `
        -SqlHostName $sqlHostName `
        -OracleHostName $oracleHostName `
        -GenericSourceName $genericSourceName

    $sourceName = Resolve-SourceName `
        -BackupType $preType `
        -ObjectName $objectName `
        -ParentName $ParentName `
        -SqlHostName $sqlHostName `
        -OracleHostName $oracleHostName `
        -GenericSourceName $genericSourceName `
        -ParentSourceName $ParentSourceName

    $rows += [pscustomobject]@{
        Object            = $Object
        ObjectName        = $objectName
        ParentName        = $ParentName
        ParentEnvironment = $ParentEnvironment
        Environment       = $env
        ObjectType        = $objectType
        SqlHostName       = $sqlHostName
        OracleHostName    = $oracleHostName
        GenericSourceName = $genericSourceName
        SourceName        = $sourceName
        SourceInfoName    = $sourceName
        Depth             = $Depth
    }

    $rows += Get-ParamDbRows `
        -Object $Object `
        -SourceName $sourceName `
        -ParentName $objectName `
        -Environment $env `
        -Depth $Depth

    foreach ($child in (As-Array -Value $Object.childObjects)) {
        $rows += Get-FlatProtectedObjects `
            -Object $child `
            -ParentName $objectName `
            -ParentEnvironment $env `
            -ParentSourceName $sourceName `
            -Depth ($Depth + 1)
    }

    return @($rows)
}

function Test-NonDisplayObject {
    param($FlatObject)

    return (
        (Test-OracleContainerName -Name $FlatObject.ObjectName) -or
        (Test-OracleContainerName -Name $FlatObject.SourceName)
    )
}

function Get-BackupTypeFromFlatObject {
    param($FlatObject)

    if (Test-NonDisplayObject -FlatObject $FlatObject) {
        return "Container"
    }

    if (-not [string]::IsNullOrWhiteSpace($FlatObject.SqlHostName)) {
        return "SQL"
    }

    if (-not [string]::IsNullOrWhiteSpace($FlatObject.OracleHostName)) {
        return "Oracle"
    }

    $vmType = Get-VmBackupTypeFromText `
        -Environment $FlatObject.Environment `
        -ObjectType $FlatObject.ObjectType `
        -ObjectName $FlatObject.ObjectName `
        -SourceName $FlatObject.SourceName `
        -SourceInfoName $FlatObject.SourceInfoName

    if (-not [string]::IsNullOrWhiteSpace($vmType)) {
        return $vmType
    }

    $envTypeText = "$($FlatObject.Environment) $($FlatObject.ObjectType)"

    if ($envTypeText -match 'kOracle') {
        return "Oracle"
    }

    if ($envTypeText -match 'kSQL') {
        return "SQL"
    }

    return "FS"
}

function Test-ObjectMatchesCiFlat {
    param(
        $FlatObject,
        [string]$Ci
    )

    foreach ($v in @(
        $FlatObject.SourceName,
        $FlatObject.SourceInfoName,
        $FlatObject.ObjectName,
        $FlatObject.ParentName
    )) {
        if (-not [string]::IsNullOrWhiteSpace($v) -and -not (Test-OracleContainerName -Name $v)) {
            if (Test-TextMatchesName -Text $v -Name $Ci) {
                return $true
            }
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($FlatObject.ObjectName) -and $FlatObject.ObjectName.Contains("/")) {
        $prefix = $FlatObject.ObjectName.Split("/", 2)[0].Trim()
        if (-not (Test-OracleContainerName -Name $prefix)) {
            if (Test-TextMatchesName -Text $prefix -Name $Ci) {
                return $true
            }
        }
    }

    return $false
}

function Test-ObjectMatchesCiAliasesFlat {
    param(
        $FlatObject,
        [string[]]$CiAliases
    )

    foreach ($alias in @($CiAliases)) {
        if (-not [string]::IsNullOrWhiteSpace($alias)) {
            if (Test-ObjectMatchesCiFlat -FlatObject $FlatObject -Ci $alias) {
                return $true
            }
        }
    }

    return $false
}

function Test-DbLikeFlat {
    param($FlatObject)

    if (Test-NonDisplayObject -FlatObject $FlatObject) {
        return $false
    }

    return ((Get-BackupTypeFromFlatObject -FlatObject $FlatObject) -in @("SQL", "Oracle"))
}

function Test-VmLikeFlat {
    param($FlatObject)

    return ((Get-BackupTypeFromFlatObject -FlatObject $FlatObject) -in @("HyperV", "Nutanix", "VM"))
}

function Get-FlatObjectKey {
    param($FlatObject)

    return @(
        $FlatObject.ObjectName,
        $FlatObject.SourceName,
        $FlatObject.ObjectType
    ) -join "|"
}

function Get-DedupedFlatObjects {
    param($FlatObjects)

    $seen = @{}
    $out  = @()

    foreach ($f in @($FlatObjects)) {
        $key = (Get-FlatObjectKey -FlatObject $f).ToLowerInvariant()
        if ($seen.ContainsKey($key)) {
            continue
        }

        $seen[$key] = $true
        $out += $f
    }

    return @($out)
}

function Convert-FlatObjectToBackupRow {
    param(
        $FlatObject,
        [string]$Ci,
        [string]$ClusterName,
        [string]$ClusterId
    )

    $obj = $FlatObject.Object
    $snap = Get-BestSnapshot -Object $obj
    $backupType = Get-BackupTypeFromFlatObject -FlatObject $FlatObject

    if ($backupType -eq "Container") {
        return $null
    }

    if ($backupType -eq "Oracle" -and [string]::IsNullOrWhiteSpace($FlatObject.OracleHostName)) {
        return $null
    }

    $objectNameOut = $FlatObject.ObjectName
    if ([string]::IsNullOrWhiteSpace($objectNameOut)) {
        $objectNameOut = "-"
    }

    $sourceNameOut = Resolve-SourceName `
        -BackupType $backupType `
        -ObjectName $FlatObject.ObjectName `
        -ParentName $FlatObject.ParentName `
        -SqlHostName $FlatObject.SqlHostName `
        -OracleHostName $FlatObject.OracleHostName `
        -GenericSourceName $FlatObject.GenericSourceName `
        -ParentSourceName $FlatObject.SourceName

    if ($backupType -eq "Oracle" -and [string]::IsNullOrWhiteSpace($sourceNameOut)) {
        return $null
    }

    $usecs = 0
    $lastBackupTime = "NoBackup"
    $pg = "-"

    if ($null -ne $snap) {
        $usecs = Get-SnapshotUsecs -Snapshot $snap
        $lastBackupTime = Convert-UsecsToEtString -Usecs $usecs

        if ([string]::IsNullOrWhiteSpace($lastBackupTime)) {
            $lastBackupTime = "NoBackupTime"
        }

        $pg = Get-ProtectionGroupName -Object $obj -Snapshot $snap
    }
    else {
        $pg = Get-ProtectionGroupName -Object $obj -Snapshot $null
    }

    return [pscustomobject]@{
        ServerName      = $Ci
        BackupType      = $backupType
        ObjectName      = $objectNameOut
        SourceName      = $sourceNameOut
        ClusterName     = $ClusterName
        ProtectionGroup = $pg
        LastBackupTime  = $lastBackupTime
        LastBackupUsecs = $usecs
    }
}

# ==========================================================
# Search Functions
# ==========================================================
function Search-GlobalObjects {
    param(
        [string[]]$SearchTerms,
        [hashtable]$Headers
    )

    $all = @()

    foreach ($term in @($SearchTerms)) {
        if ([string]::IsNullOrWhiteSpace($term)) {
            continue
        }

        $uri = "$BaseUrl/v2/data-protect/search/objects?searchString=$([uri]::EscapeDataString($term))&includeTenants=true&count=$GlobalSearchCount"
        $json = Invoke-HeliosJson -Uri $uri -Headers $Headers -Quiet

        $all += Get-GlobalObjects -Json $json
    }

    return @($all)
}

function Get-CandidateClustersFromGlobalObjects {
    param(
        $GlobalObjects,
        $Clusters,
        [hashtable]$ClusterMap
    )

    $list = New-Object System.Collections.ArrayList
    $seen = @{}

    foreach ($obj in @($GlobalObjects)) {
        Add-CandidateCluster `
            -List $list `
            -Seen $seen `
            -ClusterId (Get-ClusterIdFromSearchNode -Node $obj) `
            -ClusterName (Get-ClusterNameFromMap -ClusterId (Get-ClusterIdFromSearchNode -Node $obj) -ClusterMap $ClusterMap) `
            -SearchMode "GlobalObject"

        foreach ($opi in (As-Array -Value $obj.objectProtectionInfos)) {
            $cid = Get-ClusterIdFromSearchNode -Node $opi

            Add-CandidateCluster `
                -List $list `
                -Seen $seen `
                -ClusterId $cid `
                -ClusterName (Get-ClusterNameFromMap -ClusterId $cid -ClusterMap $ClusterMap) `
                -SearchMode "ObjectProtectionInfo"

            foreach ($pg in (As-Array -Value $opi.protectionGroups)) {
                $pgCid = Get-ClusterIdFromSearchNode -Node $pg

                Add-CandidateCluster `
                    -List $list `
                    -Seen $seen `
                    -ClusterId $pgCid `
                    -ClusterName (Get-ClusterNameFromMap -ClusterId $pgCid -ClusterMap $ClusterMap) `
                    -SearchMode "ProtectionGroup"
            }
        }

        foreach ($pg2 in (As-Array -Value $obj.protectionGroups)) {
            $pg2Cid = Get-ClusterIdFromSearchNode -Node $pg2

            Add-CandidateCluster `
                -List $list `
                -Seen $seen `
                -ClusterId $pg2Cid `
                -ClusterName (Get-ClusterNameFromMap -ClusterId $pg2Cid -ClusterMap $ClusterMap) `
                -SearchMode "ProtectionGroup"
        }
    }

    return @($list)
}

function Search-ProtectedObjectsOnClusters {
    param(
        [string]$Ci,
        [string[]]$CiAliases,
        [string[]]$SearchTerms,
        $CandidateClusters,
        [hashtable]$SearchedClusterTermKeys
    )

    $rows = @()

    foreach ($clu in @($CandidateClusters)) {
        foreach ($term in @($SearchTerms)) {
            if ([string]::IsNullOrWhiteSpace($term)) {
                continue
            }

            $searchKey = "$($clu.clusterId)|$term"
            if ($SearchedClusterTermKeys.ContainsKey($searchKey)) {
                continue
            }
            $SearchedClusterTermKeys[$searchKey] = $true

            $headers = @{
                "accept"          = "application/json"
                "apiKey"          = $ApiKey
                "accessClusterId" = "$($clu.clusterId)"
            }

            $uri = "$BaseUrl/v2/data-protect/search/protected-objects?searchString=$([uri]::EscapeDataString($term))"
            $json = Invoke-HeliosJson -Uri $uri -Headers $headers -Quiet
            $objects = Get-ProtectedObjects -Json $json

            if (-not $objects -or $objects.Count -eq 0) {
                continue
            }

            $flat = @()
            foreach ($obj in @($objects)) {
                $flat += Get-FlatProtectedObjects -Object $obj
            }

            $flat = @($flat | Where-Object { -not (Test-NonDisplayObject -FlatObject $_) })

            if (-not $flat -or $flat.Count -eq 0) {
                continue
            }

            $matching = @($flat | Where-Object { Test-ObjectMatchesCiAliasesFlat -FlatObject $_ -CiAliases $CiAliases })
            $dbLike   = @($flat | Where-Object { Test-DbLikeFlat -FlatObject $_ })
            $vmLike   = @($flat | Where-Object { Test-VmLikeFlat -FlatObject $_ })

            $objectsToCheck = Get-DedupedFlatObjects -FlatObjects @($matching + $dbLike + $vmLike)

            if (-not $objectsToCheck -or $objectsToCheck.Count -eq 0) {
                $objectsToCheck = $flat
            }

            foreach ($f in @($objectsToCheck)) {
                $r = Convert-FlatObjectToBackupRow `
                    -FlatObject $f `
                    -Ci $Ci `
                    -ClusterName $clu.clusterName `
                    -ClusterId $clu.clusterId

                if ($null -eq $r) {
                    continue
                }

                if ($r.BackupType -in @("FS", "VM", "HyperV", "Nutanix", "SQL", "Oracle")) {
                    $rows += $r
                }
            }
        }
    }

    return @($rows)
}

# ==========================================================
# Result Helpers
# ==========================================================
function Get-BackupTypeRank {
    param([string]$BackupType)

    switch ($BackupType) {
        "FS"       { return 10 }
        "VM"       { return 20 }
        "HyperV"   { return 30 }
        "Nutanix"  { return 40 }
        "SQL"      { return 50 }
        "Oracle"   { return 60 }
        default     { return 999 }
    }
}

function Test-HasValidFs {
    param($Rows)

    foreach ($r in @($Rows)) {
        if ($r.BackupType -eq "FS" -and (Is-ValidBackupTime -TimeText $r.LastBackupTime)) {
            return $true
        }
    }

    return $false
}

function Test-HasValidSqlOrOracle {
    param($Rows)

    foreach ($r in @($Rows)) {
        if ($r.BackupType -in @("SQL", "Oracle") -and (Is-ValidBackupTime -TimeText $r.LastBackupTime)) {
            return $true
        }
    }

    return $false
}

# ==========================================================
# Read Input CSV and Group CI Aliases
# ==========================================================
$RawCsv = Import-Csv -Path $InputCsv

if (-not $RawCsv -or $RawCsv.Count -eq 0) {
    throw "CSV has no rows: $InputCsv"
}

$CiColumn = @("ServerName", "Server", "CI", "ci_item", "Name") |
    Where-Object { $RawCsv[0].PSObject.Properties.Name -contains $_ } |
    Select-Object -First 1

if ([string]::IsNullOrWhiteSpace($CiColumn)) {
    throw "CSV must contain one of these columns: ServerName, Server, CI, ci_item, Name"
}

$CiAliasMap = @{}

foreach ($row in $RawCsv) {
    $ci = "$($row.$CiColumn)".Trim()

    if (Is-BadCiName -Ci $ci) {
        continue
    }

    $key = Get-CiIdentityKey -Ci $ci

    if ([string]::IsNullOrWhiteSpace($key)) {
        continue
    }

    if (-not $CiAliasMap.ContainsKey($key)) {
        $CiAliasMap[$key] = New-Object System.Collections.ArrayList
    }

    $aliasesToAdd = @()
    $aliasesToAdd += $ci

    $parts = Get-DbAtServerParts -Ci $ci

    if ($parts.IsDbAtServer) {
        $aliasesToAdd += $parts.ServerName
        $aliasesToAdd += Get-ShortName -Value $parts.ServerName
        $aliasesToAdd += $parts.DbName

        $shortDbAtServer = Get-ShortName -Value $ci
        if (-not [string]::IsNullOrWhiteSpace($shortDbAtServer)) {
            $aliasesToAdd += $shortDbAtServer
        }
    }
    else {
        $short = Get-ShortName -Value $ci
        if (-not [string]::IsNullOrWhiteSpace($short)) {
            $aliasesToAdd += $short
        }
    }

    foreach ($alias in @($aliasesToAdd | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)) {
        if (-not @($CiAliasMap[$key]) -contains $alias) {
            [void]$CiAliasMap[$key].Add($alias)
        }
    }
}

$CiGroups = @()
foreach ($key in $CiAliasMap.Keys) {
    $aliases = @($CiAliasMap[$key] | Sort-Object -Unique)
    $display = Get-PreferredCiDisplayName -Aliases $aliases

    if (-not [string]::IsNullOrWhiteSpace($display)) {
        $CiGroups += [pscustomobject]@{
            DisplayName = $display
            Aliases     = $aliases
        }
    }
}

$CiGroups = @($CiGroups | Sort-Object DisplayName)

if (-not $CiGroups -or $CiGroups.Count -eq 0) {
    throw "No valid CI values found in CSV column: $CiColumn"
}

$CleanCiList = @($CiGroups | ForEach-Object { $_.DisplayName })
$CleanCiList | Set-Content -Path $InputTxt -Encoding UTF8

# ==========================================================
# Get Clusters
# ==========================================================
$CommonHeaders = @{
    "accept" = "application/json"
    "apiKey" = $ApiKey
}

$ClusterInfoUri = "$BaseUrl/v2/mcm/cluster-mgmt/info"
$ClusterJson = Invoke-HeliosJson -Uri $ClusterInfoUri -Headers $CommonHeaders

if ($null -eq $ClusterJson) {
    throw "Unable to fetch cluster information from Helios."
}

$Clusters = @()
if ($ClusterJson.cohesityClusters) {
    $Clusters = @(As-Array -Value $ClusterJson.cohesityClusters)
}
elseif ($ClusterJson.clusters) {
    $Clusters = @(As-Array -Value $ClusterJson.clusters)
}
elseif ($ClusterJson -is [array]) {
    $Clusters = @($ClusterJson)
}

if (-not $Clusters -or $Clusters.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$ClusterMap = @{}
foreach ($c in $Clusters) {
    $clusterName = First-NonBlank -Values @($c.clusterName, $c.name)

    foreach ($id in @(
        $c.clusterId,
        $c.clusterID,
        $c.id,
        $c.uid,
        $c._id
    )) {
        Add-ClusterMapEntry -Map $ClusterMap -Id $id -ClusterName $clusterName
    }
}

# ==========================================================
# Main Processing
# ==========================================================
$Results = @()
$Counter = 0
$SearchedClusterTermKeys = @{}

foreach ($group in $CiGroups) {
    $Counter++

    $Ci = $group.DisplayName
    $CiAliases = @($group.Aliases)
    $SearchTerms = Get-SearchTermsForCiAliases -Aliases $CiAliases

    if ($ShowProgress) {
        Write-Host "[$Counter/$($CiGroups.Count)] Checking CI: $Ci" -ForegroundColor Cyan
    }

    $GlobalObjects = Search-GlobalObjects -SearchTerms $SearchTerms -Headers $CommonHeaders
    $CandidateClusters = Get-CandidateClustersFromGlobalObjects `
        -GlobalObjects $GlobalObjects `
        -Clusters $Clusters `
        -ClusterMap $ClusterMap

    if (-not $CandidateClusters -or $CandidateClusters.Count -eq 0) {
        if ($FallbackWhenNoGlobalObject) {
            $CandidateClustersList = New-Object System.Collections.ArrayList
            $seenFallback = @{}

            foreach ($clu in @($Clusters)) {
                Add-CandidateCluster `
                    -List $CandidateClustersList `
                    -Seen $seenFallback `
                    -ClusterId "$($clu.clusterId)" `
                    -ClusterName "$($clu.clusterName)" `
                    -SearchMode "AllClusterFallback"
            }

            $CandidateClusters = @($CandidateClustersList)
        }
    }

    if (-not $CandidateClusters -or $CandidateClusters.Count -eq 0) {
        $Results += [pscustomobject]@{
            ServerName      = $Ci
            BackupType      = "Unknown"
            ObjectName      = "-"
            SourceName      = $Ci
            ClusterName     = "-"
            ProtectionGroup = "-"
            LastBackupTime  = "NoObject"
            LastBackupUsecs = 0
        }

        continue
    }

    $CiRows = @(Search-ProtectedObjectsOnClusters `
        -Ci $Ci `
        -CiAliases $CiAliases `
        -SearchTerms $SearchTerms `
        -CandidateClusters $CandidateClusters `
        -SearchedClusterTermKeys $SearchedClusterTermKeys)

    $HasDbNamedServer = @($CiAliases | Where-Object { Test-CiLooksLikeDbServer -Ci $_ }).Count -gt 0
    $HasDbRows = @($CiRows | Where-Object { $_.BackupType -in @("SQL", "Oracle") }).Count -gt 0

    if ($FallbackWhenDbMissingForDbNamedServer -and $HasDbNamedServer -and -not $HasDbRows) {
        $AllClusterFallback = New-Object System.Collections.ArrayList
        $seenAll = @{}

        foreach ($clu in @($Clusters)) {
            Add-CandidateCluster `
                -List $AllClusterFallback `
                -Seen $seenAll `
                -ClusterId "$($clu.clusterId)" `
                -ClusterName "$($clu.clusterName)" `
                -SearchMode "SqlOracleFallbackNameMatched"
        }

        if ($AllClusterFallback.Count -gt 0) {
            if ($ShowProgress) {
                Write-Host "  SQL/Oracle fallback because CI name contains DB/CN: $($AllClusterFallback.Count) clusters" -ForegroundColor DarkGray
            }

            $DbFallbackRows = Search-ProtectedObjectsOnClusters `
                -Ci $Ci `
                -CiAliases $CiAliases `
                -SearchTerms $SearchTerms `
                -CandidateClusters @($AllClusterFallback) `
                -SearchedClusterTermKeys $SearchedClusterTermKeys

            $CiRows += @($DbFallbackRows | Where-Object {
                $_.BackupType -in @("SQL", "Oracle")
            })
        }
    }

    if (-not $CiRows -or $CiRows.Count -eq 0) {
        $Results += [pscustomobject]@{
            ServerName      = $Ci
            BackupType      = "Unknown"
            ObjectName      = "-"
            SourceName      = $Ci
            ClusterName     = "-"
            ProtectionGroup = "-"
            LastBackupTime  = "NoObject"
            LastBackupUsecs = 0
        }

        continue
    }

    $DedupedRows = $CiRows |
        Where-Object { $_.BackupType -ne "Container" } |
        Group-Object ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup |
        ForEach-Object {
            $_.Group | Sort-Object LastBackupUsecs -Descending | Select-Object -First 1
        }

    foreach ($Row in $DedupedRows) {
        $Results += [pscustomobject]@{
            ServerName      = $Row.ServerName
            BackupType      = $Row.BackupType
            ObjectName      = $Row.ObjectName
            SourceName      = $Row.SourceName
            ClusterName     = $Row.ClusterName
            ProtectionGroup = $Row.ProtectionGroup
            LastBackupTime  = $Row.LastBackupTime
            LastBackupUsecs = $Row.LastBackupUsecs
        }
    }

    $HasSqlOrOracleBackup = Test-HasValidSqlOrOracle -Rows $DedupedRows
    $HasFSBackup = Test-HasValidFs -Rows $DedupedRows

    if ($HasSqlOrOracleBackup -and -not $HasFSBackup) {
        $Results += [pscustomobject]@{
            ServerName      = $Ci
            BackupType      = "FS"
            ObjectName      = $Ci
            SourceName      = $Ci
            ClusterName     = "-"
            ProtectionGroup = "-"
            LastBackupTime  = "NoFSBackupFound"
            LastBackupUsecs = 0
        }
    }
}

# ==========================================================
# Summary / Output
# ==========================================================
$TotalInputCIs = $CiGroups.Count

$SortedResults = $Results |
    Sort-Object ServerName, @{ Expression = { Get-BackupTypeRank -BackupType $_.BackupType } }, ObjectName, ClusterName, ProtectionGroup

$DetailsText = (
    $SortedResults |
    Format-Table ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup, LastBackupTime -AutoSize |
    Out-String -Width 420
).TrimEnd()

# -------------------------
# Report Note
# -------------------------
$ReportNote = @(
    "Note:"
    "- NAS backups are excluded from this server decommission validation."
    "- No Backup Found means no in-scope Cohesity backup object was found for the CI."
    "- DB Only / No Server Backup means a SQL/Oracle backup was found, but no FS, VM, Hyper-V, or Nutanix/AHV backup was found for the server."
    "- Servers with names containing db or cn may require DB-level backup review if only FS/VM backup is found."
)

$SortedResults |
    Select-Object ServerName, BackupType, ObjectName, SourceName, ClusterName, ProtectionGroup, LastBackupTime |
    Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

$Report = @()
$Report += "=========================================================="
$Report += "CR Backup Status Report"
$Report += "=========================================================="
$Report += "Change Number     : $ChangeID"
$Report += "Executed By       : $ExecutedBy"
$Report += "Run Date          : $(Get-Date -Format 'dd-MMM-yyyy HH:mm:ss')"
$Report += "Total Input CIs   : $TotalInputCIs"
$Report += ""
$Report += "----------------------------------------------------------"
$Report += "Details"
$Report += "----------------------------------------------------------"
$Report += ""
$Report += $DetailsText
$Report += ""
$Report += "----------------------------------------------------------"
$Report += "Note"
$Report += "----------------------------------------------------------"
$Report += ""
$Report += $ReportNote
$Report += ""
$Report += "----------------------------------------------------------"
$Report += "Report Location"
$Report += "----------------------------------------------------------"
$Report += "TXT : $OutputTxt"
$Report += "CSV : $OutputCsv"
$Report += "=========================================================="

$Report | Set-Content -Path $OutputTxt -Encoding UTF8

Write-Host ""
Write-Host "=========================================================="
Write-Host "CR Backup Status Report"
Write-Host "=========================================================="
Write-Host "Change Number     : $ChangeID"
Write-Host "Executed By       : $ExecutedBy"
Write-Host "Run Date          : $(Get-Date -Format 'dd-MMM-yyyy HH:mm:ss')"
Write-Host "Total Input CIs   : $TotalInputCIs"
Write-Host ""
Write-Host "----------------------------------------------------------"
Write-Host "Details"
Write-Host "----------------------------------------------------------"
Write-Host ""
Write-Host $DetailsText
Write-Host ""
Write-Host "----------------------------------------------------------"
Write-Host "Note"
Write-Host "----------------------------------------------------------"
Write-Host ""
foreach ($line in $ReportNote) {
    Write-Host $line
}
Write-Host ""
Write-Host "----------------------------------------------------------"
Write-Host "Report Location"
Write-Host "----------------------------------------------------------"
Write-Host "TXT : $OutputTxt"
Write-Host "CSV : $OutputCsv"
Write-Host "=========================================================="