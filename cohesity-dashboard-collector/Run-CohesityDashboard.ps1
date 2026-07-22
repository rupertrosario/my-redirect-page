[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config.psd1'),
    [int]$Port = 8765,
    [switch]$NoBrowser,
    [switch]$SkipInitialRefresh
)

$ErrorActionPreference = 'Stop'
$collector = Join-Path $PSScriptRoot 'Collect-CohesityDashboard.ps1'
$statusPath = Join-Path $PSScriptRoot 'output/refresh-status.json'
$dataPath = Join-Path $PSScriptRoot 'output/dashboard.json'
if (-not $SkipInitialRefresh -or -not (Test-Path $dataPath)) { & $collector -ConfigPath $ConfigPath }

$root = [IO.Path]::GetFullPath($PSScriptRoot)
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
            $status = if(Test-Path $statusPath){try{Get-Content $statusPath -Raw|ConvertFrom-Json}catch{[pscustomobject]@{state='Unknown';message=$_.Exception.Message}}}else{[pscustomobject]@{state='Idle';message='No refresh has run.'}}
            Send-Json $context $status; continue
        }
        if ($route -eq '/api/refresh' -and $context.Request.HttpMethod -eq 'POST') {
            if ($refreshProcess -and -not $refreshProcess.HasExited) { Send-Json $context @{state='Running';message='A refresh is already running.'} 409; continue }
            $engine = [Diagnostics.Process]::GetCurrentProcess().MainModule.FileName
            $arguments = @('-NoProfile','-ExecutionPolicy','Bypass','-File',('"{0}"' -f $collector),'-ConfigPath',('"{0}"' -f $ConfigPath))
            $refreshProcess = Start-Process -FilePath $engine -ArgumentList $arguments -PassThru -WindowStyle Hidden
            Send-Json $context @{state='Running';message='Refresh started.';pid=$refreshProcess.Id} 202; continue
        }
        $relative = [Uri]::UnescapeDataString($route.TrimStart('/'))
        if ([string]::IsNullOrWhiteSpace($relative)) { $relative = 'index.html' }
        $path = [IO.Path]::GetFullPath((Join-Path $root $relative))
        if (-not $path.StartsWith($root,[StringComparison]::OrdinalIgnoreCase) -or -not (Test-Path $path -PathType Leaf)) { $context.Response.StatusCode=404; $context.Response.Close(); continue }
        $bytes=[IO.File]::ReadAllBytes($path); $extension=[IO.Path]::GetExtension($path).ToLower(); $context.Response.ContentType=$types[$extension]
        if(-not $context.Response.ContentType){$context.Response.ContentType='application/octet-stream'}
        $context.Response.ContentLength64=$bytes.Length; $context.Response.OutputStream.Write($bytes,0,$bytes.Length); $context.Response.Close()
    }
} finally { $listener.Stop(); $listener.Close() }
