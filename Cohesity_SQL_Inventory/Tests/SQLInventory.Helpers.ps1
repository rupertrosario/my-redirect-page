# Cohesity SQL inventory helper functions. GET-only.

function Invoke-CohesityGet {
    param([string]$Uri, [hashtable]$Headers)
    $params = @{ Uri = $Uri; Headers = $Headers; Method = "Get"; ErrorAction = "Stop" }
    if ($PSVersionTable.PSVersion.Major -lt 6) { $params.UseBasicParsing = $true }
    $response = Invoke-WebRequest @params
    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function Get-PropertyValue {
    param($Object, [string[]]$Names, $Default = $null)
    if ($null -eq $Object) { return $Default }
    foreach ($name in $Names) {
        $p = @($Object.PSObject.Properties) | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
        if ($null -ne $p -and $null -ne $p.Value) { return $p.Value }
    }
    return $Default
}

function Get-NestedValue {
    param($Object, [string[]]$Paths, $Default = $null)
    foreach ($path in $Paths) {
        $current = $Object
        $found = $true
        foreach ($segment in ($path -split '\.')) {
            if ($null -eq $current) { $found = $false; break }
            $p = @($current.PSObject.Properties) | Where-Object { $_.Name -ieq $segment } | Select-Object -First 1
            if ($null -eq $p) { $found = $false; break }
            $current = $p.Value
        }
        if ($found -and $null -ne $current) { return $current }
    }
    return $Default
}

function Get-ClusterNameValue {
    param($Cluster)
    return [string](Get-PropertyValue $Cluster @("name", "clusterName", "displayName") "Unknown")
}

function Get-ClusterIdValue {
    param($Cluster)
    $id = Get-PropertyValue $Cluster @("clusterId", "id") $null
    if ($null -eq $id) { return $null }
    return [string]$id
}

function Convert-ToDisplayValue {
    param($Value)
    if ($null -eq $Value) { return "" }
    if ($Value -is [bool]) { if ($Value) { return "True" } else { return "False" } }
    return [string]$Value
}

function Convert-UsecsToLocalText {
    param($Usecs)
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return "" }
    try {
        $milliseconds = [int64]([decimal]$Usecs / 1000)
        return [DateTimeOffset]::FromUnixTimeMilliseconds($milliseconds).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss zzz")
    }
    catch { return "" }
}

function Expand-LeafValue {
    param($Value, [string]$Path)
    if ($null -eq $Value) { return }
    if ($Value -is [string] -or $Value -is [char] -or $Value -is [bool] -or
        $Value -is [byte] -or $Value -is [sbyte] -or $Value -is [int16] -or
        $Value -is [uint16] -or $Value -is [int32] -or $Value -is [uint32] -or
        $Value -is [int64] -or $Value -is [uint64] -or $Value -is [single] -or
        $Value -is [double] -or $Value -is [decimal] -or $Value -is [datetime] -or
        $Value -is [guid]) {
        [pscustomobject]@{ Field = $Path; Value = Convert-ToDisplayValue $Value }
        return
    }
    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { [string]$key } else { "$Path.$key" }
            Expand-LeafValue -Value $Value[$key] -Path $childPath
        }
        return
    }
    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @($Value)
        for ($i = 0; $i -lt $items.Count; $i++) { Expand-LeafValue -Value $items[$i] -Path "$Path[$i]" }
        return
    }
    foreach ($p in @($Value.PSObject.Properties)) {
        $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { $p.Name } else { "$Path.$($p.Name)" }
        Expand-LeafValue -Value $p.Value -Path $childPath
    }
}

function Convert-SectionsToText {
    param($Object, [string[]]$SectionNames, [string]$Prefix = "")
    if ($null -eq $Object) { return "" }
    $pairs = New-Object System.Collections.ArrayList
    foreach ($sectionName in $SectionNames) {
        $section = Get-PropertyValue $Object @($sectionName) $null
        if ($null -eq $section) { continue }
        foreach ($leaf in @(Expand-LeafValue -Value $section -Path $sectionName)) {
            if ($leaf.Field -match '(?i)(password|secret|token|credential|apiKey)') { continue }
            if ([string]::IsNullOrWhiteSpace([string]$leaf.Value)) { continue }
            [void]$pairs.Add("$Prefix$($leaf.Field)=$($leaf.Value)")
        }
    }
    return (@($pairs | Select-Object -Unique) -join "; ")
}

function Convert-PolicyToText {
    param($Policy)
    if ($null -eq $Policy) { return "" }
    $pairs = New-Object System.Collections.ArrayList
    foreach ($leaf in @(Expand-LeafValue -Value $Policy -Path "")) {
        if ($leaf.Field -match '(?i)(password|secret|token|credential|apiKey)') { continue }
        if ($leaf.Field -notmatch '(?i)(schedule|retention|frequency|periodicity|backup|snapshot|increment|full|log|sla)') { continue }
        if ([string]::IsNullOrWhiteSpace([string]$leaf.Value)) { continue }
        [void]$pairs.Add("$($leaf.Field)=$($leaf.Value)")
    }
    return (@($pairs | Select-Object -Unique) -join "; ")
}

function Get-ActiveSqlParameters {
    param($MssqlParams)
    if ($null -eq $MssqlParams) { return $null }
    $protectionType = [string](Get-PropertyValue $MssqlParams @("protectionType") "")
    $names = @()
    if ($protectionType -match '(?i)file') { $names += "fileProtectionTypeParams" }
    elseif ($protectionType -match '(?i)native') { $names += "nativeProtectionTypeParams" }
    elseif ($protectionType -match '(?i)volume') { $names += "volumeProtectionTypeParams" }
    $names += @("fileProtectionTypeParams", "nativeProtectionTypeParams", "volumeProtectionTypeParams")
    foreach ($name in @($names | Select-Object -Unique)) {
        $value = Get-PropertyValue $MssqlParams @($name) $null
        if ($null -ne $value) { return $value }
    }
    return $null
}

function Get-ExclusionRules {
    param($MssqlParams)
    $rules = New-Object System.Collections.ArrayList
    foreach ($sectionName in @("fileProtectionTypeParams", "nativeProtectionTypeParams", "volumeProtectionTypeParams")) {
        $section = Get-PropertyValue $MssqlParams @($sectionName) $null
        if ($null -eq $section) { continue }
        foreach ($filter in @(Get-PropertyValue $section @("excludeFilters") @())) {
            $text = [string](Get-PropertyValue $filter @("filterString", "databaseName", "name") "")
            if ([string]::IsNullOrWhiteSpace($text)) { continue }
            [void]$rules.Add([pscustomobject]@{
                FilterString = $text.Trim()
                IsRegex = [bool](Get-PropertyValue $filter @("isRegularExpression") $false)
            })
        }
    }
    return @($rules)
}

function Test-ExclusionMatches {
    param($Rule, [string[]]$Candidates)
    foreach ($candidate in @($Candidates | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
        if ($Rule.IsRegex) {
            try { if ($candidate -match $Rule.FilterString) { return $true } } catch { }
        }
        elseif ($Rule.FilterString.Contains("*") -or $Rule.FilterString.Contains("?")) {
            if ($candidate -like $Rule.FilterString) { return $true }
        }
        elseif ($candidate -ieq $Rule.FilterString) { return $true }
    }
    return $false
}

function Test-IsSqlDatabaseObject {
    param($ObjectSummary)
    if ($null -eq $ObjectSummary) { return $false }
    $type = [string](Get-PropertyValue $ObjectSummary @("objectType") "")
    $name = [string](Get-PropertyValue $ObjectSummary @("name") "")
    if ($type -match '(?i)(AAGRoot|RootContainer|Instance|Host|Server)$') { return $false }
    if ($type -match '(?i)(Database|SQLDB|MSSQLDatabase)') { return $true }
    return ($name -match '[\\/]')
}

function Get-RunInformation {
    param($Run)
    $info = @(Get-PropertyValue $Run @("localBackupInfo") @()) | Select-Object -First 1
    $runType = [string](Get-PropertyValue $info @("runType") (Get-PropertyValue $Run @("runType") ""))
    $status = [string](Get-PropertyValue $info @("status") (Get-PropertyValue $Run @("status") ""))
    $startUsecs = Get-PropertyValue $info @("startTimeUsecs") (Get-PropertyValue $Run @("startTimeUsecs") $null)
    $endUsecs = Get-PropertyValue $info @("endTimeUsecs") (Get-PropertyValue $Run @("endTimeUsecs") $null)
    [pscustomobject]@{
        RunType = $runType; Status = $status; StartUsecs = $startUsecs; EndUsecs = $endUsecs
        StartText = Convert-UsecsToLocalText $startUsecs; EndText = Convert-UsecsToLocalText $endUsecs
    }
}

function Get-BackupCategory {
    param([string]$RunType)
    if ($RunType -match '(?i)log') { return "Log" }
    if ($RunType -match '(?i)full') { return "Full" }
    if ($RunType -match '(?i)(increment|regular)') { return "Incremental" }
    return "Other"
}

function Get-ObjectBackupStatus {
    param($RunObject, [string]$RunStatus)
    $status = [string](Get-NestedValue $RunObject @(
        "localSnapshotInfo.snapshotInfo.status", "localSnapshotInfo.status",
        "originalBackupInfo.snapshotInfo.status", "originalBackupInfo.status"
    ) "")
    if ([string]::IsNullOrWhiteSpace($status)) { return $RunStatus }
    return $status
}

function Get-ObjectFailureMessage {
    param($RunObject)
    foreach ($name in @("localSnapshotInfo", "originalBackupInfo")) {
        $container = Get-PropertyValue $RunObject @($name) $null
        foreach ($attempt in @(Get-PropertyValue $container @("failedAttempts") @()) | Select-Object -Last 1) {
            $message = [string](Get-PropertyValue $attempt @("message", "errorMessage") "")
            if (-not [string]::IsNullOrWhiteSpace($message)) {
                return (($message -replace '[\r\n]+', ' ') -replace '\s+', ' ').Trim()
            }
        }
    }
    return ""
}

function Get-CohesityObjectById {
    param([string]$ObjectId, [hashtable]$Headers, [hashtable]$Cache,
          [System.Collections.ArrayList]$Issues, [string]$Cluster, [string]$ProtectionGroup)
    if ([string]::IsNullOrWhiteSpace($ObjectId)) { return $null }
    if ($Cache.ContainsKey($ObjectId)) { return $Cache[$ObjectId] }
    try {
        $detail = Invoke-CohesityGet -Uri "$baseUrl/v2/data-protect/objects/$([uri]::EscapeDataString($ObjectId))" -Headers $Headers
        $Cache[$ObjectId] = $detail
        return $detail
    }
    catch {
        $Cache[$ObjectId] = $null
        [void]$Issues.Add([pscustomobject]@{ Cluster=$Cluster; ProtectionGroup=$ProtectionGroup; Stage="Object lookup"; Issue="Object '$ObjectId': $($_.Exception.Message)" })
        return $null
    }
}

function Get-CohesityPolicyById {
    param([string]$PolicyId, [hashtable]$Headers, [hashtable]$Cache,
          [System.Collections.ArrayList]$Issues, [string]$Cluster, [string]$ProtectionGroup)
    if ([string]::IsNullOrWhiteSpace($PolicyId)) { return $null }
    if ($Cache.ContainsKey($PolicyId)) { return $Cache[$PolicyId] }
    try {
        $response = Invoke-CohesityGet -Uri "$baseUrl/v2/data-protect/policies?ids=$([uri]::EscapeDataString($PolicyId))" -Headers $Headers
        $policy = @($response.policies) | Where-Object { [string](Get-PropertyValue $_ @("id") "") -eq $PolicyId } | Select-Object -First 1
        if ($null -eq $policy) { $policy = @($response.policies) | Select-Object -First 1 }
        $Cache[$PolicyId] = $policy
        return $policy
    }
    catch {
        $Cache[$PolicyId] = $null
        [void]$Issues.Add([pscustomobject]@{ Cluster=$Cluster; ProtectionGroup=$ProtectionGroup; Stage="Policy lookup"; Issue="Policy '$PolicyId': $($_.Exception.Message)" })
        return $null
    }
}

function Get-SafeFileName {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return "Unknown" }
    return ($Value -replace '[\\/:*?"<>|]', '_')
}

