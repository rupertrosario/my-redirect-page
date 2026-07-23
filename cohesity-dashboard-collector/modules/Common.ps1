Set-StrictMode -Version Latest

function Get-PropertyValue {
    param([object]$Object, [string[]]$Names, $Default = $null)
    if ($null -eq $Object) { return $Default }
    foreach ($name in $Names) {
        if ($Object -is [System.Collections.IDictionary] -and $Object.Contains($name)) {
            $value = $Object[$name]
            if ($null -ne $value) { return $value }
        }
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
    if ($Response -is [System.Collections.IEnumerable] -and $Response -isnot [string]) {
        return @($Response)
    }
    return @($Response)
}

function ConvertTo-NullableDouble {
    param([object]$Value)
    if ($null -eq $Value) { return $null }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string] -and
        $Value -isnot [System.Collections.IDictionary]) {
        foreach ($item in @($Value)) {
            $number = ConvertTo-NullableDouble $item
            if ($null -ne $number) { return $number }
        }
        return $null
    }

    if ($Value -is [System.Collections.IDictionary] -or
        ($Value -isnot [string] -and @($Value.PSObject.Properties).Count -gt 0)) {
        foreach ($name in @('value','int64Value','doubleValue','intValue','longValue','bytes','count')) {
            $nested = Get-PropertyValue $Value @($name) $null
            if ($null -ne $nested -and -not [object]::ReferenceEquals($nested,$Value)) {
                $number = ConvertTo-NullableDouble $nested
                if ($null -ne $number) { return $number }
            }
        }
    }

    $parsed = 0.0
    $styles = [Globalization.NumberStyles]::Float -bor [Globalization.NumberStyles]::AllowThousands
    if ([double]::TryParse([string]$Value,$styles,[Globalization.CultureInfo]::InvariantCulture,[ref]$parsed)) {
        return [double]$parsed
    }
    if ([double]::TryParse([string]$Value,[ref]$parsed)) { return [double]$parsed }
    return $null
}

function Get-NumericPropertyValue {
    param([object]$Object, [string[]]$Names)
    foreach ($name in $Names) {
        $raw = Get-PropertyValue $Object @($name) $null
        $number = ConvertTo-NullableDouble $raw
        if ($null -ne $number) { return $number }
    }
    return $null
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
    if ([string]::IsNullOrWhiteSpace($Target)) { return 'NotConfigured' }
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

function Test-SuccessStatus {
    param([string]$Status)
    return ($Status -match '^(k)?(Success|Succeeded|Successful|Completed|SucceededWithWarning)$')
}
function Test-FailedStatus {
    param([string]$Status)
    return ($Status -match '^(k)?(Failed|Failure|Error)$')
}
function Test-CancelledStatus {
    param([string]$Status)
    return ($Status -match '^(k)?(Canceled|Cancelled|Canceling)$')
}

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
    return @(
        [pscustomobject]@{ Key='hyperV';  Label='Hyper-V'; Api='kHyperV';              ObjectType='kVirtualMachine' },
        [pscustomobject]@{ Key='nutanix'; Label='Nutanix'; Api='kAcropolis';           ObjectType='kVirtualMachine' },
        [pscustomobject]@{ Key='nas';      Label='NAS';     Api='kGenericNas,kIsilon'; ObjectType='kHost' },
        [pscustomobject]@{ Key='oracle';   Label='Oracle';  Api='kOracle';             ObjectType='kDatabase' },
        [pscustomobject]@{ Key='sql';      Label='SQL';     Api='kSQL';                ObjectType='kDatabase' },
        [pscustomobject]@{ Key='physical'; Label='Physical';Api='kPhysical';           ObjectType='kHost' }
    )
}

function Get-RunObjectCore {
    param([object]$RunObject)
    return (Get-PropertyValue $RunObject @('object') $RunObject)
}

function Test-RunObjectForEnvironment {
    param([object]$RunObject, [object]$Environment)
    $object = Get-RunObjectCore $RunObject
    $objectType = [string](Get-PropertyValue $object @('objectType','type','entityType') '')
    $objectEnvironment = [string](Get-PropertyValue $object @('environment','environmentType') '')
    if ($objectType -and $objectType -ne $Environment.ObjectType) { return $false }
    if (-not $objectType) { return $false }

    $allowed = @($Environment.Api -split ',')
    if ($objectEnvironment -and $objectEnvironment -notin $allowed) { return $false }
    return $true
}

function Get-RunObjectIdentity {
    param([object]$RunObject)
    $object = Get-RunObjectCore $RunObject
    $name = [string](Get-PropertyValue $object @(
        'name','objectName','databaseName','databaseUniqueName','sourceName','vmName','hostName','displayName'
    ) '')
    $id = [string](Get-PropertyValue $object @('id','objectId','databaseId','vmId','entityId') '')
    if (-not $id) {
        $sourceId = [string](Get-PropertyValue $object @('sourceId') '')
        $environment = [string](Get-PropertyValue $object @('environment','environmentType') '')
        $objectType = [string](Get-PropertyValue $object @('objectType','type','entityType') '')
        $id = '{0}|{1}|{2}|{3}' -f $environment,$objectType,$sourceId,$name
    }
    $resolvedName = if($name){$name}else{$id}
    return [pscustomobject]@{ id=$id; name=$resolvedName }
}

function Get-RunObjectFailedAttempts {
    param([object]$RunObject)
    $attempts = @()
    foreach ($localInfo in @(Get-Collection (
        Get-PropertyValue $RunObject @('localSnapshotInfo','localBackupInfo') @()
    ))) {
        $attempts += @(Get-Collection (Get-PropertyValue $localInfo @('failedAttempts') @()))
        foreach ($snapshotInfo in @(Get-Collection (
            Get-PropertyValue $localInfo @('snapshotInfo') @()
        ))) {
            $attempts += @(Get-Collection (
                Get-PropertyValue $snapshotInfo @('failedAttempts') @()
            ))
        }
    }
    foreach ($snapshotInfo in @(Get-Collection (
        Get-PropertyValue $RunObject @('snapshotInfo') @()
    ))) {
        $attempts += @(Get-Collection (
            Get-PropertyValue $snapshotInfo @('failedAttempts') @()
        ))
    }
    return @($attempts | Where-Object { $null -ne $_ })
}

function Get-RunObjectStatusValues {
    param([object]$RunObject)
    $statuses = @()
    $statuses += [string](Get-PropertyValue $RunObject @('status') '')
    $object = Get-RunObjectCore $RunObject
    $statuses += [string](Get-PropertyValue $object @('status') '')
    foreach ($localInfo in @(Get-Collection (
        Get-PropertyValue $RunObject @('localSnapshotInfo','localBackupInfo') @()
    ))) {
        $statuses += [string](Get-PropertyValue $localInfo @('status') '')
        foreach ($snapshotInfo in @(Get-Collection (
            Get-PropertyValue $localInfo @('snapshotInfo') @()
        ))) {
            $statuses += [string](Get-PropertyValue $snapshotInfo @('status') '')
        }
    }
    foreach ($snapshotInfo in @(Get-Collection (
        Get-PropertyValue $RunObject @('snapshotInfo') @()
    ))) {
        $statuses += [string](Get-PropertyValue $snapshotInfo @('status') '')
    }
    return @($statuses | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Get-RunObjectState {
    param([object]$RunObject, [string]$RunStatus)
    $statuses = @(Get-RunObjectStatusValues $RunObject)

    # A final object/snapshot success wins over failedAttempts left by an earlier
    # retry in the same run.
    if (@($statuses | Where-Object { Test-SuccessStatus $_ }).Count -gt 0) {
        return 'Success'
    }
    if (@($statuses | Where-Object { Test-FailedStatus $_ }).Count -gt 0) {
        return 'Failed'
    }
    if (@($statuses | Where-Object { Test-CancelledStatus $_ }).Count -gt 0) {
        return 'Cancelled'
    }
    if (Test-SuccessStatus $RunStatus) { return 'Success' }
    if (Test-FailedStatus $RunStatus) { return 'Failed' }
    if (Test-CancelledStatus $RunStatus) { return 'Cancelled' }
    if (@(Get-RunObjectFailedAttempts $RunObject).Count -gt 0) { return 'Failed' }
    return 'Success'
}

function Get-RunObjectMessage {
    param([object]$RunObject, [object]$RunInfo)
    $messages = @()
    foreach ($attempt in @(Get-RunObjectFailedAttempts $RunObject)) {
        $message = [string](Get-PropertyValue $attempt @(
            'message','error','reason','errorMessage','failureMessage'
        ) '')
        if ($message) { $messages += $message }
    }
    foreach ($container in @($RunObject,(Get-RunObjectCore $RunObject),$RunInfo)) {
        if ($null -eq $container) { continue }
        $message = [string](Get-PropertyValue $container @(
            'message','messages','error','reason','errorMessage','failureMessage','lastError'
        ) '')
        if ($message) { $messages += $message }
    }
    return (@($messages | Where-Object { $_ } | Select-Object -Unique) -join ' | ')
}

function Get-AlertPropertyValue {
    param([object]$Alert, [string[]]$Keys)
    $properties = @(Get-Collection (Get-PropertyValue $Alert @('propertyList','properties') @()))
    foreach ($key in $Keys) {
        $match = @($properties | Where-Object {
            [string](Get-PropertyValue $_ @('key','name') '') -ieq $key
        }) | Select-Object -First 1
        if ($null -ne $match) {
            $value = Get-PropertyValue $match @('value','values') $null
            if ($value -is [System.Collections.IEnumerable] -and $value -isnot [string]) {
                return (@($value) | Select-Object -First 1)
            }
            return $value
        }
    }
    return $null
}
