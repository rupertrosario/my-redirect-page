[CmdletBinding()]
param(
    [string]$ConfigPath,
    [int]$Port = 8765,
    [switch]$NoBrowser,
    [switch]$SkipInitialRefresh
)

$ErrorActionPreference = 'Stop'

# Resolve paths after parameter binding. $PSScriptRoot can be empty while default
# parameter expressions are evaluated on some Windows PowerShell launch methods.
$scriptRoot = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($scriptRoot) -and $MyInvocation.MyCommand.Path) {
    $scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
    throw 'Unable to determine the dashboard folder. Run Run-CohesityDashboard.ps1 from its saved folder.'
}
$scriptRoot = [IO.Path]::GetFullPath($scriptRoot)
if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $scriptRoot 'config.psd1'
} elseif (-not [IO.Path]::IsPathRooted($ConfigPath)) {
    $ConfigPath = Join-Path $scriptRoot $ConfigPath
}
$ConfigPath = [IO.Path]::GetFullPath($ConfigPath)

$collector = Join-Path $scriptRoot 'Collect-CohesityDashboard.ps1'
$configTemplate = Join-Path $scriptRoot 'config.example.psd1'
$statusPath = Join-Path $scriptRoot 'output\refresh-status.json'
$dataPath = Join-Path $scriptRoot 'output\dashboard.json'

foreach ($requiredFile in @($collector, $configTemplate, (Join-Path $scriptRoot 'index.html'))) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Required dashboard file not found: $requiredFile. Copy the complete cohesity-dashboard-collector folder."
    }
}

# Create the private local configuration automatically on first run.
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    Copy-Item -LiteralPath $configTemplate -Destination $ConfigPath
    Write-Host "Created local configuration: $ConfigPath" -ForegroundColor Green
}

if (-not $SkipInitialRefresh -or -not (Test-Path -LiteralPath $dataPath -PathType Leaf)) {
    & $collector -ConfigPath $ConfigPath -OutputPath $dataPath -StatusPath $statusPath
}

$root = $scriptRoot
$rootPrefix = $root.TrimEnd([char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)) + [IO.Path]::DirectorySeparatorChar
$listener = [Net.HttpListener]::new()
$listener.Prefixes.Add("http://localhost:$Port/")
$listener.Start()
$url = "http://localhost:$Port/"
$refreshProcess = $null
Write-Host "Dashboard: $url" -ForegroundColor Cyan
Write-Host 'Press Ctrl+C to stop.'
if (-not $NoBrowser) { Start-Process $url }

$types = @{'.html'='text/html; charset=utf-8';'.json'='application/json; charset=utf-8';'.js'='text/javascript; charset=utf-8';'.css'='text/css; charset=utf-8'}
function Send-Json($Context,$Object,[int]$Status=200) {
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Object | ConvertTo-Json -Depth 20))
    $Context.Response.StatusCode=$Status; $Context.Response.ContentType='application/json; charset=utf-8'; $Context.Response.ContentLength64=$bytes.Length
    $Context.Response.OutputStream.Write($bytes,0,$bytes.Length); $Context.Response.Close()
}

try {
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $route = $context.Request.Url.AbsolutePath
        if ($route -eq '/api/status') {
            $status = if(Test-Path -LiteralPath $statusPath -PathType Leaf){try{Get-Content -LiteralPath $statusPath -Raw|ConvertFrom-Json}catch{[pscustomobject]@{state='Unknown';message=$_.Exception.Message}}}else{[pscustomobject]@{state='Idle';message='No refresh has run.'}}
            Send-Json $context $status; continue
        }
        if ($route -eq '/api/refresh' -and $context.Request.HttpMethod -eq 'POST') {
            if ($refreshProcess -and -not $refreshProcess.HasExited) { Send-Json $context @{state='Running';message='A refresh is already running.'} 409; continue }
            $engine = (Get-Process -Id $PID).Path
            if ([string]::IsNullOrWhiteSpace($engine)) { throw 'Unable to locate the current PowerShell executable.' }
            $arguments = @('-NoProfile','-ExecutionPolicy','Bypass','-File',('"{0}"' -f $collector),'-ConfigPath',('"{0}"' -f $ConfigPath),'-OutputPath',('"{0}"' -f $dataPath),'-StatusPath',('"{0}"' -f $statusPath))
            $refreshProcess = Start-Process -FilePath $engine -ArgumentList $arguments -PassThru -WindowStyle Hidden
            Send-Json $context @{state='Running';message='Refresh started.';pid=$refreshProcess.Id} 202; continue
        }
        $relative = [Uri]::UnescapeDataString($route.TrimStart('/'))
        if ([string]::IsNullOrWhiteSpace($relative)) { $relative = 'index.html' }
        $path = [IO.Path]::GetFullPath((Join-Path $root $relative))
        if (-not $path.StartsWith($rootPrefix,[StringComparison]::OrdinalIgnoreCase) -or -not (Test-Path -LiteralPath $path -PathType Leaf)) { $context.Response.StatusCode=404; $context.Response.Close(); continue }
        $bytes=[IO.File]::ReadAllBytes($path); $extension=[IO.Path]::GetExtension($path).ToLower(); $context.Response.ContentType=$types[$extension]
        if(-not $context.Response.ContentType){$context.Response.ContentType='application/octet-stream'}
        $context.Response.ContentLength64=$bytes.Length; $context.Response.OutputStream.Write($bytes,0,$bytes.Length); $context.Response.Close()
    }
} finally { $listener.Stop(); $listener.Close() }
