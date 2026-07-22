[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config.psd1'),
    [int]$Port = 8765,
    [switch]$NoBrowser
)

$ErrorActionPreference = 'Stop'
& (Join-Path $PSScriptRoot 'Collect-CohesityDashboard.ps1') -ConfigPath $ConfigPath

$root = [IO.Path]::GetFullPath($PSScriptRoot)
$listener = [Net.HttpListener]::new()
$listener.Prefixes.Add("http://localhost:$Port/")
$listener.Start()
$url = "http://localhost:$Port/"
Write-Host "Dashboard: $url"
Write-Host 'Press Ctrl+C to stop.'
if (-not $NoBrowser) { Start-Process $url }

$types = @{
    '.html'='text/html; charset=utf-8'; '.json'='application/json; charset=utf-8'
    '.js'='text/javascript; charset=utf-8'; '.css'='text/css; charset=utf-8'
}
try {
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $relative = [Uri]::UnescapeDataString($context.Request.Url.AbsolutePath.TrimStart('/'))
        if ([string]::IsNullOrWhiteSpace($relative)) { $relative = 'index.html' }
        $path = [IO.Path]::GetFullPath((Join-Path $root $relative))
        if (-not $path.StartsWith($root, [StringComparison]::OrdinalIgnoreCase) -or -not (Test-Path $path -PathType Leaf)) {
            $context.Response.StatusCode = 404
            $context.Response.Close()
            continue
        }
        $bytes = [IO.File]::ReadAllBytes($path)
        $context.Response.ContentType = $types[[IO.Path]::GetExtension($path).ToLower()]
        if (-not $context.Response.ContentType) { $context.Response.ContentType = 'application/octet-stream' }
        $context.Response.ContentLength64 = $bytes.Length
        $context.Response.OutputStream.Write($bytes,0,$bytes.Length)
        $context.Response.Close()
    }
}
finally { $listener.Stop(); $listener.Close() }
