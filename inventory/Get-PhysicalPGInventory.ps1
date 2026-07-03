# =====================================================================
# Cohesity Helios — Physical PG Inventory with Object Include/Exclude UI
# STRICTLY READ-ONLY / GET-only
#
# Included:
# - Automatic Helios cluster index using /v2/mcm/cluster-mgmt/info
# - Active Physical Protection Groups
# - PG summary CSV
# - Object detail CSV with object-level include/exclude and latest successful backup
# - Simple WinForms UI: click PG on left, object details show on right
# =====================================================================

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# -------------------------------
# Settings
# -------------------------------
$outDir     = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$baseUrl    = "https://helios.cohesity.com"

# Number of PG run pages to inspect for latest object-level successful backup.
# Increase only if old objects do not show a success date.
$MaxRunPagesPerPG = 5

$script:ErrorRows = @()

# -------------------------------
# Output folder
# -------------------------------
if (-not (Test-Path -Path $outDir -PathType Container)) {
    New-Item -Path $outDir -ItemType Directory -Force | Out-Null
}

# -------------------------------
# API key
# -------------------------------
if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

$commonHeaders = @{
    apiKey = $apiKey
    accept = "application/json"
}

# =====================================================================
# Error helper
# =====================================================================
function Add-ErrorRow {
    param(
        [string]$Stage,
        [string]$Cluster,
        [string]$PGName,
        [string]$Uri,
        [object]$ErrorObject
    )

    $message = ""

    try {
        $message = $ErrorObject.Exception.Message
    }
    catch {
        $message = "$ErrorObject"
    }

    $script:ErrorRows += [PSCustomObject]@{
        Stage        = $Stage
        Cluster      = $Cluster
        PGName       = $PGName
        Uri          = $Uri
        ErrorMessage = $message
    }
}

# =====================================================================
# GET wrapper
# =====================================================================
function Invoke-HeliosGetJson {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    try {
        if ($PSVersionTable.PSVersion.Major -lt 6) {
            $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
        }
        else {
            $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get
        }

        if (-not $resp -or [string]::IsNullOrWhiteSpace($resp.Content)) {
            return $null
        }

        return ($resp.Content | ConvertFrom-Json)
    }
    catch {
        $msg = $_.Exception.Message

        try {
            if ($_.Exception.Response -and $_.Exception.Response.GetResponseStream()) {
                $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $body = $reader.ReadToEnd()

                if (-not [string]::IsNullOrWhiteSpace($body)) {
                    $msg = "$msg | ResponseBody: $body"
                }
            }
        }
        catch {}

        throw "GET failed: $Uri | $msg"
    }
}

# =====================================================================
# Helpers
# =====================================================================
function As-Array {
    param([object]$Value)

    if ($null -eq $Value) {
        return @()
    }

    return @($Value)
}

function To-FlatString {
    param([object]$Value)

    if ($null -eq $Value) {
        return ""
    }

    $items = @()

    foreach ($item in @($Value)) {
        if ($null -ne $item -and "$item".Trim() -ne "") {
            $items += "$item"
        }
    }

    if ($items.Count -eq 0) {
        return ""
    }

    return (($items | Select-Object -Unique) -join ";")
}

function Get-FirstNonEmpty {
    param([object[]]$Values)

    foreach ($v in @($Values)) {
        foreach ($vv in @($v)) {
            if ($null -ne $vv -and "$vv".Trim() -ne "") {
                return $vv
            }
        }
    }

    return ""
}

function To-Number {
    param($Value)

    if ($null -eq $Value -or "$Value".Trim() -eq "") {
        return 0
    }

    try {
        return [double]$Value
    }
    catch {
        return 0
    }
}

function Normalize-Key {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }

    return $Value.Trim().ToUpper()
}

function Convert-UsecsToET {
    param($Usecs)

    if ($null -eq $Usecs -or "$Usecs".Trim() -eq "" -or "$Usecs" -eq "0") {
        return ""
    }

    try {
        $epochUtc = [DateTime]::SpecifyKind([datetime]"1970-01-01 00:00:00", [DateTimeKind]::Utc)
        $dtUtc = $epochUtc.AddSeconds(([double]$Usecs / 1000000))

        try {
            $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
            $dtEt = [TimeZoneInfo]::ConvertTimeFromUtc($dtUtc, $tz)
            return $dtEt.ToString("yyyy-MM-dd HH:mm:ss")
        }
        catch {
            return ($dtUtc.ToString("yyyy-MM-dd HH:mm:ss") + " UTC")
        }
    }
    catch {
        return ""
    }
}

function Test-SuccessStatus {
    param([string]$Status)

    if ([string]::IsNullOrWhiteSpace($Status)) {
        return $false
    }

    return ($Status -match '(?i)success|succeed|succeededwithwarning|warning')
}

function Get-RunMessage {
    param([object]$Run)

    return To-FlatString @(
        $Run.message,
        $Run.messages,
        $Run.errorMessage,
        $Run.failureMessage,
        $Run.localBackupInfo.message,
        $Run.localBackupInfo.messages,
        $Run.localSnapshotInfo.message,
        $Run.localSnapshotInfo.messages
    )
}

function Get-PgIdFromProtectionGroup {
    param([object]$Pg)

    return Get-FirstNonEmpty -Values @(
        $Pg.id,
        $Pg.protectionGroupId,
        $Pg.lastRun.protectionGroupId,
        $Pg.lastRun.originProtectionGroupId,
        $Pg.runs.protectionGroupId,
        $Pg.runs.originProtectionGroupId
    )
}

function Get-ObjectKeys {
    param([object]$Object)

    $keys = @()

    $values = @(
        $Object.id,
        $Object.sourceId,
        $Object.objectId,
        $Object.name,
        $Object.sourceName,
        $Object.hostName,
        $Object.displayName,
        $Object.object.name,
        $Object.object.id,
        $Object.object.sourceId,
        $Object.protectedObject.id,
        $Object.protectedObject.name
    )

    foreach ($v in @($values)) {
        if ($null -ne $v -and "$v".Trim() -ne "") {
            $keys += (Normalize-Key "$v")
        }
    }

    return @($keys | Where-Object { $_ -and $_.Trim() -ne "" } | Select-Object -Unique)
}

function Get-ObjectRunStatus {
    param(
        [object]$RunObject,
        [object]$Run
    )

    return Get-FirstNonEmpty -Values @(
        $RunObject.localBackupInfo.status,
        $RunObject.localSnapshotInfo.status,
        $RunObject.snapshotInfo.status,
        $RunObject.status,
        $Run.localBackupInfo.status,
        $Run.localSnapshotInfo.status,
        $Run.status
    )
}

function Get-ObjectRunEndUsecs {
    param(
        [object]$RunObject,
        [object]$Run
    )

    return Get-FirstNonEmpty -Values @(
        $RunObject.localBackupInfo.endTimeUsecs,
        $RunObject.localSnapshotInfo.endTimeUsecs,
        $RunObject.localSnapshotInfo.snapshotInfo.endTimeUsecs,
        $RunObject.snapshotInfo.endTimeUsecs,
        $RunObject.endTimeUsecs,
        $Run.localBackupInfo.endTimeUsecs,
        $Run.localSnapshotInfo.endTimeUsecs,
        $Run.endTimeUsecs
    )
}

function Get-LatestObjectSuccessMap {
    param(
        [string]$PgId,
        [hashtable]$Headers,
        [string]$ClusterName,
        [string]$PGName
    )

    $map = New-Object System.Collections.Hashtable ([StringComparer]::OrdinalIgnoreCase)

    if ([string]::IsNullOrWhiteSpace($PgId)) {
        return $map
    }

    $page = 0
    $cookie = ""

    do {
        $page++
        $runsUri = "$baseUrl/v2/data-protect/protection-groups/$PgId/runs"

        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $runsUri = "$runsUri`?paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        try {
            $runsJson = Invoke-HeliosGetJson -Uri $runsUri -Headers $Headers
        }
        catch {
            Add-ErrorRow -Stage "RunsLookup" -Cluster $ClusterName -PGName $PGName -Uri $runsUri -ErrorObject $_
            break
        }

        foreach ($run in @($runsJson.runs | Where-Object { $_ })) {
            $runObjects = @($run.objects | Where-Object { $_ })

            # Some API responses may not expose run.objects. In that case, no object-level date can be mapped.
            foreach ($ro in $runObjects) {
                $status = Get-ObjectRunStatus -RunObject $ro -Run $run
                if (-not (Test-SuccessStatus -Status $status)) {
                    continue
                }

                $endUsecs = Get-ObjectRunEndUsecs -RunObject $ro -Run $run
                $endNum = To-Number $endUsecs

                if ($endNum -le 0) {
                    continue
                }

                foreach ($key in (Get-ObjectKeys -Object $ro)) {
                    if (-not $map.ContainsKey($key) -or $endNum -gt [double]$map[$key].Usecs) {
                        $map[$key] = [PSCustomObject]@{
                            Status = $status
                            Usecs  = $endNum
                            EndET  = Convert-UsecsToET $endUsecs
                        }
                    }
                }
            }
        }

        $cookie = Get-FirstNonEmpty -Values @($runsJson.paginationCookie)

        if ($runsJson.isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) {
            break
        }

    } while (-not [string]::IsNullOrWhiteSpace($cookie) -and $page -lt $MaxRunPagesPerPG)

    return $map
}

function Find-LatestObjectSuccess {
    param(
        [hashtable]$ObjectSuccessMap,
        [object]$Object
    )

    foreach ($key in (Get-ObjectKeys -Object $Object)) {
        if ($ObjectSuccessMap.ContainsKey($key)) {
            return $ObjectSuccessMap[$key]
        }
    }

    return [PSCustomObject]@{
        Status = ""
        Usecs  = 0
        EndET  = ""
    }
}

function Get-PolicyName {
    param(
        [string]$PolicyId,
        [hashtable]$Headers,
        [hashtable]$PolicyCache,
        [string]$ClusterName,
        [string]$PGName
    )

    if ([string]::IsNullOrWhiteSpace($PolicyId)) {
        return ""
    }

    if ($PolicyCache.ContainsKey($PolicyId)) {
        return $PolicyCache[$PolicyId]
    }

    $policyUri = "$baseUrl/v2/data-protect/policies?ids=$PolicyId"

    try {
        $pJson = Invoke-HeliosGetJson -Uri $policyUri -Headers $Headers

        $policyName = (
            $pJson.policies |
            Where-Object { $_._id -eq $PolicyId -or $_.id -eq $PolicyId } |
            Select-Object -ExpandProperty name -First 1
        )

        if ([string]::IsNullOrWhiteSpace($policyName)) {
            $policyName = $PolicyId
        }

        $PolicyCache[$PolicyId] = $policyName
        return $policyName
    }
    catch {
        Add-ErrorRow -Stage "PolicyLookup" -Cluster $ClusterName -PGName $PGName -Uri $policyUri -ErrorObject $_
        $PolicyCache[$PolicyId] = $PolicyId
        return $PolicyId
    }
}

function Get-PhysicalProtectionGroups {
    param(
        [hashtable]$Headers,
        [string]$ClusterName
    )

    $all = @()
    $cookie = ""

    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"

        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri = "$uri&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        try {
            $json = Invoke-HeliosGetJson -Uri $uri -Headers $Headers
        }
        catch {
            Add-ErrorRow -Stage "ProtectionGroups" -Cluster $ClusterName -PGName "" -Uri $uri -ErrorObject $_
            break
        }

        if ($json.protectionGroups) {
            $all += @($json.protectionGroups | Where-Object { $_ })
        }

        $cookie = Get-FirstNonEmpty -Values @($json.paginationCookie)

        if ($json.isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) {
            break
        }

    } while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function ConvertTo-UiDataTable {
    param(
        [object[]]$Rows,
        [string[]]$Columns
    )

    $dt = New-Object System.Data.DataTable

    foreach ($col in $Columns) {
        [void]$dt.Columns.Add($col, [string])
    }

    foreach ($row in @($Rows)) {
        $dr = $dt.NewRow()

        foreach ($col in $Columns) {
            $value = $null

            if ($null -ne $row -and ($row.PSObject.Properties.Name -contains $col)) {
                $value = $row.$col
            }

            if ($null -eq $value) {
                $dr[$col] = ""
            }
            else {
                $dr[$col] = [string]$value
            }
        }

        [void]$dt.Rows.Add($dr)
    }

    return $dt
}

function Set-GridDefaults {
    param([System.Windows.Forms.DataGridView]$Grid)

    $Grid.ReadOnly = $true
    $Grid.AllowUserToAddRows = $false
    $Grid.AllowUserToDeleteRows = $false
    $Grid.MultiSelect = $false
    $Grid.SelectionMode = [System.Windows.Forms.DataGridViewSelectionMode]::FullRowSelect
    $Grid.AutoSizeColumnsMode = [System.Windows.Forms.DataGridViewAutoSizeColumnsMode]::DisplayedCells
    $Grid.AutoSizeRowsMode = [System.Windows.Forms.DataGridViewAutoSizeRowsMode]::DisplayedCells
    $Grid.ScrollBars = [System.Windows.Forms.ScrollBars]::Both
    $Grid.RowHeadersVisible = $false
}

function Show-PhysicalInventoryUI {
    param(
        [object[]]$SummaryRows,
        [object[]]$DetailRows,
        [string]$SummaryCsv,
        [string]$DetailCsv
    )

    try {
        Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop
        Add-Type -AssemblyName System.Drawing -ErrorAction Stop
    }
    catch {
        Write-Warning "Windows Forms UI is unavailable in this PowerShell host. Use the CSV files instead."
        return
    }

    if ($null -eq $SummaryRows -or @($SummaryRows).Count -eq 0) {
        Write-Warning "No PG summary rows available for UI."
        return
    }

    $summaryColumns = @(
        "PGIndex",
        "Cluster",
        "PGName",
        "PolicyName",
        "ProtectionType",
        "PGObjectCount",
        "GlobalExcludePaths",
        "JobExcludedVssWriters",
        "IsPaused",
        "LastRunStatus",
        "LastRunStartET",
        "LastRunEndET",
        "LastRunMessage"
    )

    $detailColumns = @(
        "ObjectName",
        "LastSuccessfulBackupStatus",
        "LastSuccessfulBackupEndET",
        "ObjectIncludedPaths",
        "ObjectExcludedPathsAll",
        "IncludedPath",
        "ExcludedPathsUnderIncludedPath",
        "SkipNestedVolumes",
        "GlobalExcludePaths",
        "ObjectExcludedVssWriters",
        "JobExcludedVssWriters"
    )

    $summaryTable = ConvertTo-UiDataTable -Rows $SummaryRows -Columns $summaryColumns
    $emptyDetailTable = ConvertTo-UiDataTable -Rows @() -Columns $detailColumns

    [System.Windows.Forms.Application]::EnableVisualStyles()

    $form = New-Object System.Windows.Forms.Form
    $form.Text = "Cohesity Physical PG Inventory"
    $form.StartPosition = "CenterScreen"
    $form.Width = 1650
    $form.Height = 900
    $form.MinimumSize = New-Object System.Drawing.Size(1200, 700)

    $mainLayout = New-Object System.Windows.Forms.TableLayoutPanel
    $mainLayout.Dock = [System.Windows.Forms.DockStyle]::Fill
    $mainLayout.RowCount = 2
    $mainLayout.ColumnCount = 1
    [void]$mainLayout.RowStyles.Add((New-Object System.Windows.Forms.RowStyle([System.Windows.Forms.SizeType]::Percent, 100)))
    [void]$mainLayout.RowStyles.Add((New-Object System.Windows.Forms.RowStyle([System.Windows.Forms.SizeType]::Absolute, 28)))

    $split = New-Object System.Windows.Forms.SplitContainer
    $split.Dock = [System.Windows.Forms.DockStyle]::Fill
    $split.Orientation = [System.Windows.Forms.Orientation]::Vertical
    $split.SplitterDistance = 760

    $leftLayout = New-Object System.Windows.Forms.TableLayoutPanel
    $leftLayout.Dock = [System.Windows.Forms.DockStyle]::Fill
    $leftLayout.RowCount = 2
    $leftLayout.ColumnCount = 1
    [void]$leftLayout.RowStyles.Add((New-Object System.Windows.Forms.RowStyle([System.Windows.Forms.SizeType]::Absolute, 28)))
    [void]$leftLayout.RowStyles.Add((New-Object System.Windows.Forms.RowStyle([System.Windows.Forms.SizeType]::Percent, 100)))

    $rightLayout = New-Object System.Windows.Forms.TableLayoutPanel
    $rightLayout.Dock = [System.Windows.Forms.DockStyle]::Fill
    $rightLayout.RowCount = 2
    $rightLayout.ColumnCount = 1
    [void]$rightLayout.RowStyles.Add((New-Object System.Windows.Forms.RowStyle([System.Windows.Forms.SizeType]::Absolute, 28)))
    [void]$rightLayout.RowStyles.Add((New-Object System.Windows.Forms.RowStyle([System.Windows.Forms.SizeType]::Percent, 100)))

    $pgLabel = New-Object System.Windows.Forms.Label
    $pgLabel.Text = "Protection Groups - click a PG to show object-level status/include/exclude"
    $pgLabel.Dock = [System.Windows.Forms.DockStyle]::Fill
    $pgLabel.TextAlign = [System.Drawing.ContentAlignment]::MiddleLeft

    $detailLabel = New-Object System.Windows.Forms.Label
    $detailLabel.Text = "Object details"
    $detailLabel.Dock = [System.Windows.Forms.DockStyle]::Fill
    $detailLabel.TextAlign = [System.Drawing.ContentAlignment]::MiddleLeft

    $pgGrid = New-Object System.Windows.Forms.DataGridView
    $pgGrid.Dock = [System.Windows.Forms.DockStyle]::Fill
    Set-GridDefaults -Grid $pgGrid
    $pgGrid.DataSource = $summaryTable

    $detailGrid = New-Object System.Windows.Forms.DataGridView
    $detailGrid.Dock = [System.Windows.Forms.DockStyle]::Fill
    Set-GridDefaults -Grid $detailGrid
    $detailGrid.DataSource = $emptyDetailTable

    $statusLabel = New-Object System.Windows.Forms.Label
    $statusLabel.Dock = [System.Windows.Forms.DockStyle]::Fill
    $statusLabel.TextAlign = [System.Drawing.ContentAlignment]::MiddleLeft
    $statusLabel.Text = "Summary CSV: $SummaryCsv    |    Detail CSV: $DetailCsv"

    [void]$leftLayout.Controls.Add($pgLabel, 0, 0)
    [void]$leftLayout.Controls.Add($pgGrid, 0, 1)

    [void]$rightLayout.Controls.Add($detailLabel, 0, 0)
    [void]$rightLayout.Controls.Add($detailGrid, 0, 1)

    [void]$split.Panel1.Controls.Add($leftLayout)
    [void]$split.Panel2.Controls.Add($rightLayout)

    [void]$mainLayout.Controls.Add($split, 0, 0)
    [void]$mainLayout.Controls.Add($statusLabel, 0, 1)

    [void]$form.Controls.Add($mainLayout)

    $updateDetails = {
        if ($pgGrid.SelectedRows.Count -eq 0) {
            return
        }

        $selectedRow = $pgGrid.SelectedRows[0]
        $selectedCluster = [string]$selectedRow.Cells["Cluster"].Value
        $selectedPgName  = [string]$selectedRow.Cells["PGName"].Value

        $pgDetails = @(
            $DetailRows |
            Where-Object {
                $_.Cluster -eq $selectedCluster -and
                $_.PGName  -eq $selectedPgName
            }
        )

        $detailGrid.DataSource = ConvertTo-UiDataTable -Rows $pgDetails -Columns $detailColumns
        $detailLabel.Text = "Object details - $selectedCluster / $selectedPgName ($($pgDetails.Count) rows)"
    }

    $pgGrid.Add_SelectionChanged($updateDetails)

    $form.Add_Shown({
        if ($pgGrid.Rows.Count -gt 0) {
            $pgGrid.ClearSelection()
            $pgGrid.Rows[0].Selected = $true
            $pgGrid.CurrentCell = $pgGrid.Rows[0].Cells[0]
            & $updateDetails
        }
    })

    [void]$form.ShowDialog()
}

# =====================================================================
# 1) Cluster menu — same working pattern as Oracle inventory
# =====================================================================
$cluJson = Invoke-HeliosGetJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
$json_clu = @($cluJson.cohesityClusters)

if (-not $json_clu -or $json_clu.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$clusters = $json_clu | ForEach-Object {
    $name = @(
        $_.name,
        $_.clusterName,
        $_.displayName
    ) |
        Where-Object { $_ -and "$($_)".Trim() -ne "" } |
        Select-Object -First 1

    $cid = @(
        $_.clusterId,
        $_.id
    ) |
        Where-Object { $_ } |
        Select-Object -First 1

    if (-not $name) {
        $name = "Unknown-$cid"
    }

    [PSCustomObject]@{
        ClusterName = $name
        ClusterId   = $cid
    }
} | Sort-Object ClusterName

$clusterMenu = for ($i = 0; $i -lt $clusters.Count; $i++) {
    [PSCustomObject]@{
        Index       = $i + 1
        ClusterName = $clusters[$i].ClusterName
        ClusterId   = $clusters[$i].ClusterId
    }
}

Write-Host ""
Write-Host "Available Helios Clusters (sorted):" -ForegroundColor Cyan
$clusterMenu | Format-Table -AutoSize
Write-Host ""
Write-Host "[0] All clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host "Select cluster: 0 for ALL, 1-$($clusterMenu.Count) for single, or X"

    if ($selection -match '^(x|X|q|Q)$') {
        return
    }

    $n = 0

    if (-not [int]::TryParse($selection, [ref]$n)) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($n -lt 0 -or $n -gt $clusterMenu.Count) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($n -eq 0) {
        $SelectedClusters = @($clusterMenu)
    }
    else {
        $SelectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $n })
    }

    break
}

# =====================================================================
# 2) Collect Physical PG summary + object detail
# =====================================================================
$summaryRows = @()
$detailRows  = @()
$policyCache = @{}
$pgIndex = 0

foreach ($c in $SelectedClusters) {

    $cluster_id   = $c.ClusterId
    $cluster_name = $c.ClusterName

    $headers = @{
        apiKey          = $apiKey
        accessClusterId = $cluster_id
        accept          = "application/json"
    }

    Write-Host "Collecting active Physical PGs from $cluster_name ..." -ForegroundColor Yellow

    $pgs = Get-PhysicalProtectionGroups -Headers $headers -ClusterName $cluster_name

    foreach ($pg in (@($pgs) | Where-Object { $_ })) {

        $physical = $pg.physicalParams

        if ($null -eq $physical) {
            continue
        }

        $pgIndex++
        $pgId = Get-PgIdFromProtectionGroup -Pg $pg

        $protectionType = Get-FirstNonEmpty -Values @($physical.protectionType)

        $fileParams   = $physical.fileProtectionTypeParams
        $volumeParams = $physical.volumeProtectionTypeParams

        if ($protectionType -eq "kVolume") {
            $objects = @(As-Array $volumeParams.objects | Where-Object { $_ })
            $globalExcludePaths = ""
            $jobExcludedVssWriters = To-FlatString $volumeParams.excludedVssWriters
        }
        else {
            $objects = @(As-Array $fileParams.objects | Where-Object { $_ })
            $globalExcludePaths = To-FlatString $fileParams.globalExcludePaths
            $jobExcludedVssWriters = To-FlatString $fileParams.excludedVssWriters
        }

        $lastRun = $pg.lastRun
        $localInfo = $lastRun.localBackupInfo

        if ($null -eq $localInfo) {
            $localInfo = $lastRun.localSnapshotInfo
        }

        $lastRunStatus = Get-FirstNonEmpty -Values @($localInfo.status, $lastRun.status)
        $startTimeUsecs = Get-FirstNonEmpty -Values @($localInfo.startTimeUsecs, $lastRun.startTimeUsecs)
        $endTimeUsecs  = Get-FirstNonEmpty -Values @($localInfo.endTimeUsecs, $lastRun.endTimeUsecs)
        $lastRunMessage = Get-RunMessage -Run $lastRun

        $policyName = Get-FirstNonEmpty -Values @($pg.policyName)

        if ([string]::IsNullOrWhiteSpace($policyName)) {
            $policyName = Get-PolicyName `
                -PolicyId $pg.policyId `
                -Headers $headers `
                -PolicyCache $policyCache `
                -ClusterName $cluster_name `
                -PGName $pg.name
        }

        Write-Host "  Reading latest object success dates for PG: $($pg.name)" -ForegroundColor DarkGray
        $objectSuccessMap = Get-LatestObjectSuccessMap -PgId $pgId -Headers $headers -ClusterName $cluster_name -PGName $pg.name

        $summaryRows += [PSCustomObject]@{
            PGIndex               = $pgIndex
            Cluster               = $cluster_name
            PGName                = $pg.name
            PolicyName            = $policyName
            ProtectionType        = $protectionType
            PGObjectCount         = @($objects).Count
            GlobalExcludePaths    = $globalExcludePaths
            JobExcludedVssWriters = $jobExcludedVssWriters
            IsActive              = $pg.isActive
            IsPaused              = $pg.isPaused
            LastRunStatus         = $lastRunStatus
            LastRunStartET        = Convert-UsecsToET $startTimeUsecs
            LastRunEndET          = Convert-UsecsToET $endTimeUsecs
            LastRunMessage        = $lastRunMessage
        }

        foreach ($obj in $objects) {

            $objectName = Get-FirstNonEmpty -Values @(
                $obj.name,
                $obj.sourceName,
                $obj.hostName,
                $obj.displayName,
                $obj.id
            )

            $objectSuccess = Find-LatestObjectSuccess -ObjectSuccessMap $objectSuccessMap -Object $obj

            $objectExcludedVssWriters = To-FlatString $obj.excludedVssWriters
            $filePaths = @(As-Array $obj.filePaths | Where-Object { $_ })

            $objectIncludedPaths = To-FlatString @(
                $filePaths | ForEach-Object { $_.includedPath }
            )

            $allObjectExcludedPaths = @()

            foreach ($fp in $filePaths) {
                $allObjectExcludedPaths += @(As-Array $fp.excludedPaths)
            }

            $objectExcludedPathsAll = To-FlatString $allObjectExcludedPaths

            if ($protectionType -eq "kVolume") {
                $detailRows += [PSCustomObject]@{
                    Cluster                         = $cluster_name
                    PGName                          = $pg.name
                    ObjectName                      = $objectName
                    LastSuccessfulBackupStatus      = $objectSuccess.Status
                    LastSuccessfulBackupEndET       = $objectSuccess.EndET
                    ObjectIncludedPaths             = To-FlatString $obj.volumeGuids
                    ObjectExcludedPathsAll          = ""
                    IncludedPath                    = To-FlatString $obj.volumeGuids
                    ExcludedPathsUnderIncludedPath  = ""
                    SkipNestedVolumes               = ""
                    GlobalExcludePaths              = $globalExcludePaths
                    ObjectExcludedVssWriters        = $objectExcludedVssWriters
                    JobExcludedVssWriters           = $jobExcludedVssWriters
                }
            }
            elseif ($filePaths.Count -eq 0) {
                $detailRows += [PSCustomObject]@{
                    Cluster                         = $cluster_name
                    PGName                          = $pg.name
                    ObjectName                      = $objectName
                    LastSuccessfulBackupStatus      = $objectSuccess.Status
                    LastSuccessfulBackupEndET       = $objectSuccess.EndET
                    ObjectIncludedPaths             = ""
                    ObjectExcludedPathsAll          = ""
                    IncludedPath                    = ""
                    ExcludedPathsUnderIncludedPath  = ""
                    SkipNestedVolumes               = ""
                    GlobalExcludePaths              = $globalExcludePaths
                    ObjectExcludedVssWriters        = $objectExcludedVssWriters
                    JobExcludedVssWriters           = $jobExcludedVssWriters
                }
            }
            else {
                foreach ($fp in $filePaths) {
                    $detailRows += [PSCustomObject]@{
                        Cluster                         = $cluster_name
                        PGName                          = $pg.name
                        ObjectName                      = $objectName
                        LastSuccessfulBackupStatus      = $objectSuccess.Status
                        LastSuccessfulBackupEndET       = $objectSuccess.EndET
                        ObjectIncludedPaths             = $objectIncludedPaths
                        ObjectExcludedPathsAll          = $objectExcludedPathsAll
                        IncludedPath                    = $fp.includedPath
                        ExcludedPathsUnderIncludedPath  = To-FlatString $fp.excludedPaths
                        SkipNestedVolumes               = $fp.skipNestedVolumes
                        GlobalExcludePaths              = $globalExcludePaths
                        ObjectExcludedVssWriters        = $objectExcludedVssWriters
                        JobExcludedVssWriters           = $jobExcludedVssWriters
                    }
                }
            }
        }
    }
}

# =====================================================================
# 3) Output table + CSV
# =====================================================================
$summaryRows = $summaryRows | Sort-Object Cluster, PGName
$detailRows  = $detailRows  | Sort-Object Cluster, PGName, ObjectName, IncludedPath

$stamp      = Get-Date -Format "yyyy-MM-dd_HHmm"
$summaryCsv = Join-Path $outDir "Physical_PG_Summary_$stamp.csv"
$detailCsv  = Join-Path $outDir "Physical_PG_Object_Detail_$stamp.csv"

$summaryRows | Export-Csv -Path $summaryCsv -NoTypeInformation -Encoding utf8
$detailRows  | Export-Csv -Path $detailCsv  -NoTypeInformation -Encoding utf8

try {
    $raw = $Host.UI.RawUI
    $targetWidth = 10000

    if ($raw.BufferSize.Width -lt $targetWidth) {
        $raw.BufferSize = New-Object Management.Automation.Host.Size ($targetWidth, $raw.BufferSize.Height)
    }
}
catch {}

Write-Host ""
Write-Host "Physical PG Summary" -ForegroundColor Cyan

$summaryRows |
    Format-Table PGIndex, Cluster, PGName, PolicyName, ProtectionType, PGObjectCount, GlobalExcludePaths, IsPaused, LastRunStatus, LastRunStartET, LastRunEndET -AutoSize -Wrap |
    Out-String -Width 10000 |
    Write-Host

Write-Host "Summary rows : $(@($summaryRows).Count)" -ForegroundColor Green
Write-Host "Detail rows  : $(@($detailRows).Count)" -ForegroundColor Green
Write-Host "Summary CSV  : $summaryCsv" -ForegroundColor Green
Write-Host "Detail CSV   : $detailCsv" -ForegroundColor Green

Show-PhysicalInventoryUI -SummaryRows $summaryRows -DetailRows $detailRows -SummaryCsv $summaryCsv -DetailCsv $detailCsv

# =====================================================================
# 4) Error exception output
# =====================================================================
if ($script:ErrorRows.Count -gt 0) {

    Write-Host ""
    Write-Host "Error Exceptions" -ForegroundColor Yellow

    $script:ErrorRows |
        Select-Object Stage, Cluster, PGName, Uri, ErrorMessage |
        Format-Table -AutoSize -Wrap |
        Out-String -Width 10000 |
        Write-Host

    $errPath = Join-Path $outDir "Physical_PG_Inventory_errors_$stamp.csv"

    $script:ErrorRows |
        Export-Csv -Path $errPath -NoTypeInformation -Encoding utf8

    Write-Host "Error CSV exported: $errPath" -ForegroundColor DarkYellow
}
else {
    Write-Host "No API exceptions captured." -ForegroundColor Green
}
