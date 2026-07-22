Set-StrictMode -Version Latest

function Get-PropertyValue {
    param([object]$Object, [string[]]$Names, $Default = $null)
    if ($null -eq $Object) { return $Default }
    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties[$name]
        if ($null -ne $property -and $null -ne $property.Value) { return $property.Value }
    }
    return $Default
}

function Get-Collection {
    param([object]$Response, [string[]]$Names)
    if ($null -eq $Response) { return @() }
    foreach ($name in $Names) {
        $value = Get-PropertyValue $Response @($name)
        if ($null -ne $value) { return @($value) }
    }
    if ($Response -is [System.Collections.IEnumerable] -and $Response -isnot [string]) { return @($Response) }
    return @($Response)
}

function Invoke-HeliosGet {
    param([string]$BaseUrl, [string]$Path, [hashtable]$Headers, [bool]$VerifyTls = $true)
    $params = @{ Uri = "{0}{1}" -f $BaseUrl.TrimEnd('/'), $Path; Method = 'Get'; Headers = $Headers }
    if (-not $VerifyTls -and $PSVersionTable.PSVersion.Major -ge 7) { $params.SkipCertificateCheck = $true }
    Invoke-RestMethod @params
}

function Get-VersionStatus {
    param([string]$Current, [string]$Target)
    try { if ([version]$Current -lt [version]$Target) { return 'BelowBaseline' } }
    catch { return 'Unknown' }
    return 'Current'
}
