# Hotfix for cr_stat_bkup_new_jun23
# Purpose: keep DB-like rows from protected-object output even when only the FS/server row matches the CI.

# Replace the block inside Search-ProtectedObjectsOnClusters after $MatchingFlatObjects is calculated.

$DbFlatObjects = @($FlatObjects | Where-Object {
    "$($_.Environment) $($_.ObjectType) $($_.ObjectName) $($_.SourceName) $($_.ParentName)" -match '(?i)kSQL|kOracle|kDatabase|database|mssql|oracle|sql'
})

$ObjectsToCheck = @(
    $MatchingFlatObjects
    $DbFlatObjects
) | Sort-Object ObjectName, SourceName, ObjectType -Unique

if (-not $ObjectsToCheck -or $ObjectsToCheck.Count -eq 0) {
    $ObjectsToCheck = $FlatObjects
}

# Also update Object-MatchesCiFlat to include sourceInfo when available:
# - sourceInfo.name
# - sourceInfo.displayName
# - sourceInfo.entity.name
# - sourceInfo.entity.displayName

# Result:
# If protected-object response contains kDatabase/kSQL rows for the server search,
# those DB rows are retained and displayed instead of being dropped by the FS-only match.
