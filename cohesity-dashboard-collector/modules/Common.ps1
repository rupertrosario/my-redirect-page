Set-StrictMode -Version Latest

function Get-PropertyValue {
    param([object]$Object, [string[]]$Names, $Default = $null)
    if ($null -eq $Object) { return $Default }
    foreach ($name in $Names) {
        if ($Object -is [System.Collections.IDictionary] -and $Object.Contains($name)) { return $Object[$name] }
        $property = $Object.PSObject.Properties[$name]
        if ($null -ne $property -and $null -ne $property.Value) { return $property.Value }
    }
    return $Default
}

function Get-NestedValue {
    param([object]$Object, [string]$Path, $Default = $null)
    $current = $Object
    foreach ($part in ($Path -split '\.')) {
        $current = Get-PropertyValue $current @($part) $null
        if ($null -eq $current) { return $Default }
    }
    return $current
}

function Get-Collection {
    param([object]$Response, [string[]]$Names = @())
    if ($null -eq $Response) { return @() }
    foreach ($name in $Names) {
        $value = Get-PropertyValue $Response @($name) $null
        if ($null -ne $value) { return @($value) }
    }
    if ($Response -is [System.Collections.IEnumerable] -and $Response -isnot [string]) { return @($Response) }
    return @($Response)
}

function Invoke-HeliosGet {
    param(
        [string]$BaseUrl, [string]$Path, [hashtable]$Headers,
        [int]$TimeoutSec = 90, [bool]$VerifyTls = $true
    )
    $uri = if ($Path -match '^https?://') { $Path } else { '{0}{1}' -f $BaseUrl.TrimEnd('/'), $Path }
    $params = @{ Uri=$uri; Method='Get'; Headers=$Headers; TimeoutSec=$TimeoutSec; ErrorAction='Stop' }
    if ($PSVersionTable.PSVersion.Major -lt 6) { $params.UseBasicParsing = $true }
    elseif (-not $VerifyTls) { $params.SkipCertificateCheck = $true }
    $response = Invoke-WebRequest @params
    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function Get-VersionStatus {
    param([string]$Current, [string]$Target)
    try { if ([version]$Current -lt [version]$Target) { return 'BelowBaseline' } }
    catch { return 'Unknown' }
    return 'Current'
}

function Convert-UsecsToUtc {
    param($Usecs)
    try {
        $value = [int64]$Usecs
        if ($value -le 0) { return '' }
        return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]($value / 1000)).UtcDateTime.ToString('o')
    } catch { return '' }
}

function Test-SuccessStatus { param([string]$Status) $Status -match '^(k)?(Success|Succeeded|Successful|Completed|SucceededWithWarning)$' }
function Test-FailedStatus { param([string]$Status) $Status -match '^(k)?(Failed|Failure|Error)$' }
function Test-CancelledStatus { param([string]$Status) $Status -match '^(k)?(Canceled|Cancelled|Canceling)$' }

function Convert-RunType {
    param([string]$RunType)
    if ($RunType -match 'log') { return 'Log' }
    if ($RunType -match 'full') { return 'Full' }
    if ($RunType -match 'increment|regular') { return 'Incremental' }
    if ([string]::IsNullOrWhiteSpace($RunType)) { return 'Unknown' }
    return ($RunType -replace '^k','')
}

function Get-LocationText {
    param($Location)
    if ($null -eq $Location) { return '' }
    if ($Location -is [string]) { return $Location.Trim() }
    if ($Location -is [System.Collections.IEnumerable]) {
        return (@($Location | ForEach-Object { Get-LocationText $_ } | Where-Object { $_ }) -join ', ')
    }
    return [string](Get-PropertyValue $Location @('name','location','displayName','siteName','dataCenter','city','region','country') '')
}

function Get-EnvironmentDefinitions {
    @(
        [pscustomobject]@{ Key='hyperV';   Label='Hyper-V';  Api='kHyperV';              ParamNames=@('hypervParams','hyperVParams') },
        [pscustomobject]@{ Key='nutanix';  Label='Nutanix';  Api='kAcropolis';           ParamNames=@('acropolisParams','nutanixParams','ahvParams') },
        [pscustomobject]@{ Key='nas';       Label='NAS';       Api='kGenericNas,kIsilon'; ParamNames=@('genericNasParams','isilonParams','nasParams') },
        [pscustomobject]@{ Key='oracle';    Label='Oracle';    Api='kOracle';             ParamNames=@('oracleParams') },
        [pscustomobject]@{ Key='sql';       Label='SQL';       Api='kSQL';                ParamNames=@('sqlParams') },
        [pscustomobject]@{ Key='physical';  Label='Physical';  Api='kPhysical';           ParamNames=@('physicalParams') }
    )
}

function Get-PgObjects {
    param([object]$ProtectionGroup, [object]$Environment)
    $params = $null
    foreach ($name in $Environment.ParamNames) {
        $params = Get-PropertyValue $ProtectionGroup @($name) $null
        if ($null -ne $params) { break }
    }
    if ($null -eq $params) { return @() }

    $raw = @()
    if ($Environment.Key -eq 'physical') {
        $raw += Get-Collection (Get-NestedValue $params 'fileProtectionTypeParams.objects' $null)
        $raw += Get-Collection (Get-NestedValue $params 'volumeProtectionTypeParams.objects' $null)
    } else {
        $raw += Get-Collection (Get-PropertyValue $params @('objects') @())
    }

    $result = @()
    foreach ($item in @($raw | Where-Object { $null -ne $_ })) {
        if ($Environment.Key -in @('sql','oracle')) {
            $channels = Get-Collection (Get-NestedValue $item 'dbParams.dbChannels.dbChannel' $null)
            if (-not $channels.Count) { $channels = @($item) }
            foreach ($channel in $channels) {
                $name = [string](Get-PropertyValue $channel @('databaseUniqueName','databaseName','name','objectName') '')
                if (-not $name) { $name = [string](Get-PropertyValue $item @('sourceName','name') '') }
                $id = [string](Get-PropertyValue $channel @('id','objectId','databaseId') '')
                if (-not $id) { $id = [string](Get-PropertyValue $item @('id','objectId','sourceId') $name) }
                if ($name) { $result += [pscustomobject]@{ id=$id; name=$name; type='Database' } }
            }
        } else {
            $name = [string](Get-PropertyValue $item @('name','objectName','sourceName','vmName','hostName','displayName') '')
            $id = [string](Get-PropertyValue $item @('id','objectId','sourceId','vmId','entityId') $name)
            if ($name) { $result += [pscustomobject]@{ id=$id; name=$name; type=$Environment.Label } }
        }
    }
    return @($result | Sort-Object id,name -Unique)
}
