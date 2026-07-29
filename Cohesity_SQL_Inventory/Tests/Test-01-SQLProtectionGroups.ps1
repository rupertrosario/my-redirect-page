# Cohesity SQL Inventory - Test 01
# Purpose: Inspect SQL protection settings, alert policy, and latest-run object details.
# Scope: GET-only, grid output is mandatory, CSV export is created automatically.

[CmdletBinding()]
param(
    [string]$ClusterName,
    [int]$MaxProtectionGroups = 1,
    [switch]$ScanAllClusters,
    [string]$OutputDirectory = "X:\PowerShell\Data\Cohesity\SQLInventory\Tests"
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if ($MaxProtectionGroups -lt 1) {
    throw "MaxProtectionGroups must be at least 1."
}

if (-not (Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
    throw "Out-GridView is required for this test but is not available on this system."
}

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}

if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -UseBasicParsing `
            -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function Get-PropertyValue {
    param(
        $Object,
        [string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object) {
        return $Default
    }

    foreach ($name in $Names) {
        $property = @($Object.PSObject.Properties) |
            Where-Object { $_.Name -ieq $name } |
            Select-Object -First 1

        if ($null -ne $property -and $null -ne $property.Value) {
            return $property.Value
        }
    }

    return $Default
}

function Get-NestedValue {
    param(
        $Object,
        [string[]]$Paths,
        $Default = $null
    )

    foreach ($path in $Paths) {
        $current = $Object
        $found = $true

        foreach ($segment in ($path -split '\.')) {
            if ($null -eq $current) {
                $found = $false
                break
            }

            $property = @($current.PSObject.Properties) |
                Where-Object { $_.Name -ieq $segment } |
                Select-Object -First 1

            if ($null -eq $property) {
                $found = $false
                break
            }

            $current = $property.Value
        }

        if ($found -and $null -ne $current) {
            return $current
        }
    }

    return $Default
}

function Get-ClusterNameValue {
    param($Cluster)

    return [string](Get-PropertyValue $Cluster @(
        "name",
        "clusterName",
        "displayName"
    ) "Unknown")
}

function Get-ClusterIdValue {
    param($Cluster)

    $id = Get-PropertyValue $Cluster @(
        "clusterId",
        "id"
    ) $null

    if ($null -eq $id) {
        return $null
    }

    return [string]$id
}

function Convert-ToDisplayValue {
    param($Value)

    if ($null -eq $Value) {
        return "<null>"
    }

    if ($Value -is [bool]) {
        if ($Value) { return "True" }
        return "False"
    }

    if ($Value -is [datetime]) {
        return $Value.ToString("o")
    }

    return [string]$Value
}

function Expand-LeafValue {
    param(
        $Value,
        [string]$Path
    )

    if ($null -eq $Value) {
        [pscustomobject]@{
            Field = $Path
            Value = "<null>"
        }
        return
    }

    if (
        $Value -is [string] -or
        $Value -is [char] -or
        $Value -is [bool] -or
        $Value -is [byte] -or
        $Value -is [sbyte] -or
        $Value -is [int16] -or
        $Value -is [uint16] -or
        $Value -is [int32] -or
        $Value -is [uint32] -or
        $Value -is [int64] -or
        $Value -is [uint64] -or
        $Value -is [single] -or
        $Value -is [double] -or
        $Value -is [decimal] -or
        $Value -is [datetime] -or
        $Value -is [guid]
    ) {
        [pscustomobject]@{
            Field = $Path
            Value = Convert-ToDisplayValue $Value
        }
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        $keys = @($Value.Keys)
        if ($keys.Count -eq 0) {
            [pscustomobject]@{
                Field = $Path
                Value = "{}"
            }
            return
        }

        foreach ($key in $keys) {
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) {
                [string]$key
            }
            else {
                "$Path.$key"
            }

            Expand-LeafValue -Value $Value[$key] -Path $childPath
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @($Value)
        if ($items.Count -eq 0) {
            [pscustomobject]@{
                Field = $Path
                Value = "[]"
            }
            return
        }

        for ($index = 0; $index -lt $items.Count; $index++) {
            Expand-LeafValue -Value $items[$index] -Path "$Path[$index]"
        }
        return
    }

    $properties = @($Value.PSObject.Properties)
    if ($properties.Count -eq 0) {
        [pscustomobject]@{
            Field = $Path
            Value = Convert-ToDisplayValue $Value
        }
        return
    }

    foreach ($property in $properties) {
        $childPath = if ([string]::IsNullOrWhiteSpace($Path)) {
            $property.Name
        }
        else {
            "$Path.$($property.Name)"
        }

        Expand-LeafValue -Value $property.Value -Path $childPath
    }
}

function New-GridRow {
    param(
        [string]$Cluster,
        [string]$ProtectionGroupName,
        [string]$ProtectionGroupId,
        [string]$Environment,
        [string]$PolicyId,
        [string]$RecordType,
        [string]$ParameterSection,
        [string]$Field,
        [string]$Value,
        [string]$ObjectId = "",
        [string]$ObjectName = "",
        [string]$SourceId = "",
        [string]$SourceName = "",
        [string]$ObjectEnvironment = "",
        [string]$ObjectType = "",
        [string]$SnapshotStatus = ""
    )

    return [pscustomobject][ordered]@{
        Cluster             = $Cluster
        ProtectionGroupName = $ProtectionGroupName
        ProtectionGroupId   = $ProtectionGroupId
        Environment         = $Environment
        PolicyId            = $PolicyId
        RecordType          = $RecordType
        ParameterSection    = $ParameterSection
        Field               = $Field
        Value               = $Value
        ObjectId            = $ObjectId
        ObjectName          = $ObjectName
        SourceId            = $SourceId
        SourceName          = $SourceName
        ObjectEnvironment   = $ObjectEnvironment
        ObjectType          = $ObjectType
        SnapshotStatus      = $SnapshotStatus
    }
}

$commonHeaders = @{
    accept = "application/json"
    apiKey = $apiKey
}

$clusterResponse = Invoke-CohesityGet `
    -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers $commonHeaders

$clusters = @($clusterResponse.cohesityClusters)
if ($clusters.Count -eq 0) {
    throw "No clusters were returned by Cohesity Helios."
}

$clusters = @($clusters | Sort-Object { Get-ClusterNameValue $_ })

if (-not [string]::IsNullOrWhiteSpace($ClusterName)) {
    $clusters = @(
        $clusters | Where-Object {
            (Get-ClusterNameValue $_) -like $ClusterName
        }
    )

    if ($clusters.Count -eq 0) {
        throw "No cluster matched '$ClusterName'."
    }
}

$rows = @()
$issues = @()
$clustersChecked = 0
$protectionGroupsInspected = 0
$objectsResolved = 0

foreach ($cluster in $clusters) {
    $resolvedClusterName = Get-ClusterNameValue $cluster
    $clusterId = Get-ClusterIdValue $cluster

    if ([string]::IsNullOrWhiteSpace($clusterId)) {
        $issues += [pscustomobject]@{
            Cluster = $resolvedClusterName
            Stage   = "Cluster discovery"
            Issue   = "Cluster ID was not returned"
        }
        continue
    }

    $clustersChecked++

    $headers = @{
        accept          = "application/json"
        apiKey          = $apiKey
        accessClusterId = $clusterId
    }

    $uri = "$baseUrl/v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true"

    try {
        $pgResponse = Invoke-CohesityGet -Uri $uri -Headers $headers
        $protectionGroups = @($pgResponse.protectionGroups)

        if ($protectionGroups.Count -eq 0) {
            continue
        }

        foreach ($pg in @($protectionGroups | Select-Object -First $MaxProtectionGroups)) {
            $protectionGroupsInspected++

            $pgName = [string](Get-PropertyValue $pg @("name") "N/A")
            $pgId = [string](Get-PropertyValue $pg @("id") "N/A")
            $environment = [string](Get-PropertyValue $pg @("environment") "N/A")
            $policyId = [string](Get-PropertyValue $pg @("policyId") "N/A")

            $mssqlParams = Get-PropertyValue $pg @("mssqlParams") $null
            if ($null -eq $mssqlParams) {
                $issues += [pscustomobject]@{
                    Cluster = $resolvedClusterName
                    Stage   = "mssqlParams"
                    Issue   = "Protection group '$pgName' did not return mssqlParams"
                }
            }
            else {
                foreach ($flatField in @(Expand-LeafValue -Value $mssqlParams -Path "")) {
                    $fullPath = [string]$flatField.Field
                    $firstDot = $fullPath.IndexOf(".")

                    if ($firstDot -gt 0) {
                        $parameterSection = $fullPath.Substring(0, $firstDot)
                        $fieldName = $fullPath.Substring($firstDot + 1)
                    }
                    else {
                        $parameterSection = "mssqlParams"
                        $fieldName = $fullPath
                    }

                    $rows += New-GridRow `
                        -Cluster $resolvedClusterName `
                        -ProtectionGroupName $pgName `
                        -ProtectionGroupId $pgId `
                        -Environment $environment `
                        -PolicyId $policyId `
                        -RecordType "SQLSetting" `
                        -ParameterSection $parameterSection `
                        -Field $fieldName `
                        -Value ([string]$flatField.Value)
                }
            }

            $alertPolicy = Get-PropertyValue $pg @("alertPolicy") $null
            if ($null -eq $alertPolicy) {
                $issues += [pscustomobject]@{
                    Cluster = $resolvedClusterName
                    Stage   = "alertPolicy"
                    Issue   = "Protection group '$pgName' did not return alertPolicy"
                }
            }
            else {
                foreach ($flatField in @(Expand-LeafValue -Value $alertPolicy -Path "")) {
                    $rows += New-GridRow `
                        -Cluster $resolvedClusterName `
                        -ProtectionGroupName $pgName `
                        -ProtectionGroupId $pgId `
                        -Environment $environment `
                        -PolicyId $policyId `
                        -RecordType "AlertPolicy" `
                        -ParameterSection "alertPolicy" `
                        -Field ([string]$flatField.Field) `
                        -Value ([string]$flatField.Value)
                }
            }

            if ($pgId -eq "N/A") {
                $issues += [pscustomobject]@{
                    Cluster = $resolvedClusterName
                    Stage   = "Protection group runs"
                    Issue   = "Protection group '$pgName' did not return an ID; object names could not be resolved"
                }
                continue
            }

            try {
                $encodedPgId = [uri]::EscapeDataString($pgId)
                $runsUri = "$baseUrl/v2/data-protect/protection-groups/$encodedPgId/runs?numRuns=1&includeObjectDetails=true"
                $runsResponse = Invoke-CohesityGet -Uri $runsUri -Headers $headers
                $latestRun = @($runsResponse.runs) | Select-Object -First 1

                if ($null -eq $latestRun) {
                    $issues += [pscustomobject]@{
                        Cluster = $resolvedClusterName
                        Stage   = "Protection group runs"
                        Issue   = "No run was returned for protection group '$pgName'"
                    }
                    continue
                }

                $runObjects = @(Get-PropertyValue $latestRun @("objects") @())
                if ($runObjects.Count -eq 0) {
                    $issues += [pscustomobject]@{
                        Cluster = $resolvedClusterName
                        Stage   = "Protection group objects"
                        Issue   = "Latest run for protection group '$pgName' did not return object details"
                    }
                    continue
                }

                foreach ($runObject in $runObjects) {
                    $objectSummary = Get-PropertyValue $runObject @("object") $null
                    if ($null -eq $objectSummary) {
                        continue
                    }

                    $objectId = [string](Get-PropertyValue $objectSummary @("id") "N/A")
                    $objectName = [string](Get-PropertyValue $objectSummary @("name") "N/A")
                    $sourceId = [string](Get-PropertyValue $objectSummary @("sourceId") "N/A")
                    $sourceName = [string](Get-PropertyValue $objectSummary @("sourceName") "N/A")
                    $objectEnvironment = [string](Get-PropertyValue $objectSummary @("environment") "N/A")
                    $objectType = [string](Get-PropertyValue $objectSummary @("objectType") "N/A")
                    $snapshotStatus = [string](Get-NestedValue $runObject @(
                        "localSnapshotInfo.snapshotInfo.status",
                        "originalBackupInfo.snapshotInfo.status"
                    ) "N/A")

                    $rows += New-GridRow `
                        -Cluster $resolvedClusterName `
                        -ProtectionGroupName $pgName `
                        -ProtectionGroupId $pgId `
                        -Environment $environment `
                        -PolicyId $policyId `
                        -RecordType "ProtectedObject" `
                        -ParameterSection "latestRun.objects" `
                        -Field "object" `
                        -Value $objectName `
                        -ObjectId $objectId `
                        -ObjectName $objectName `
                        -SourceId $sourceId `
                        -SourceName $sourceName `
                        -ObjectEnvironment $objectEnvironment `
                        -ObjectType $objectType `
                        -SnapshotStatus $snapshotStatus

                    $objectsResolved++
                }
            }
            catch {
                $issues += [pscustomobject]@{
                    Cluster = $resolvedClusterName
                    Stage   = "Protection group runs"
                    Issue   = "Protection group '$pgName': $($_.Exception.Message)"
                }
            }
        }

        if (-not $ScanAllClusters) {
            break
        }
    }
    catch {
        $issues += [pscustomobject]@{
            Cluster = $resolvedClusterName
            Stage   = "SQL protection groups"
            Issue   = $_.Exception.Message
        }
    }
}

$rows = @(
    $rows |
        Sort-Object Cluster, ProtectionGroupName, RecordType, ParameterSection, Field, ObjectName
)

if ($rows.Count -eq 0) {
    if ($issues.Count -gt 0) {
        $issues | Format-Table -AutoSize -Wrap | Out-Host
    }
    throw "No SQL protection-group data was collected."
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$csvPath = Join-Path $OutputDirectory "Test-01-SQLProtectionGroupDetails_$timestamp.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host "`nSQL protection-group detail test" -ForegroundColor Cyan
Write-Host "Clusters checked: $clustersChecked"
Write-Host "Protection groups inspected: $protectionGroupsInspected"
Write-Host "Grid rows collected: $($rows.Count)"
Write-Host "Objects resolved to names: $objectsResolved"
Write-Host "CSV: $csvPath" -ForegroundColor Green

if ($issues.Count -gt 0) {
    $issuesPath = Join-Path $OutputDirectory "Test-01-SQLProtectionGroupIssues_$timestamp.csv"
    $issues | Export-Csv -Path $issuesPath -NoTypeInformation -Encoding UTF8
    Write-Warning "$($issues.Count) issue(s) were recorded. Issues CSV: $issuesPath"
}

$rows | Out-GridView -Title "Cohesity SQL Protection Group Details"
