function Get-HeliosData {
    [CmdletBinding()]
    param([hashtable]$Config, [hashtable]$Headers, [string]$FixtureDirectory)

    $result = @{}
    foreach ($name in $Config.Endpoints.Keys) {
        $fixture = if ($FixtureDirectory) { Join-Path $FixtureDirectory "$name.json" } else { $null }
        if ($fixture -and (Test-Path $fixture)) {
            $result[$name] = Get-Content $fixture -Raw | ConvertFrom-Json
            continue
        }
        try {
            $result[$name] = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints[$name] -Headers $Headers -VerifyTls $Config.VerifyTls
        }
        catch {
            $result[$name] = $null
            Write-Warning "Collection failed for $name ($($Config.Endpoints[$name])): $($_.Exception.Message)"
        }
    }
    return $result
}
