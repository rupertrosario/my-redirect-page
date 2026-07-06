# Generic Protection VM Fields

Purpose: capture all Hyper-V and AHV protection parameters first. Remove columns later only after review.

## Hyper-V PG fields

- cloudMigration
- excludeObjectIds
- excludeVmTagIds
- globalExcludeDisks
- globalIncludeDisks
- protectionType
- sourceId
- sourceName
- vmTagIds
- appConsistentSnapshot
- fallbackToCrashConsistentSnapshot
- indexingPolicy.enableIndexing
- indexingPolicy.includePaths
- indexingPolicy.excludePaths

## Hyper-V object fields

- id
- name
- includeDisks
- excludeDisks

## AHV PG fields

- appConsistentSnapshot
- backupDirectlyAttachedVolumeGroups
- continueOnQuiesceFailure
- excludeObjectIds
- excludeVmTagIds
- globalExcludeDisks
- globalIncludeDisks
- indexingPolicy.enableIndexing
- indexingPolicy.includePaths
- indexingPolicy.excludePaths
- sourceId
- sourceName
- vmTagIds

## AHV object fields

- id
- name
- includeDisks
- excludeDisks

## Target CSV columns to add

PG summary:

- SourceId
- SourceName
- ProtectionType
- CloudMigration
- AppConsistentSnapshot
- FallbackToCrashConsistentSnapshot
- ContinueOnQuiesceFailure
- BackupDirectlyAttachedVolumeGroups
- GlobalIncludeDisks
- GlobalExcludeDisks
- ExcludeObjectIds
- VmTagIds
- ExcludeVmTagIds
- IndexingEnabled
- IndexingIncludePaths
- IndexingExcludePaths

Object detail:

- ObjectIncludeDisks
- ObjectExcludeDisks
