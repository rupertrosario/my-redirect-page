# DR Ready - Status Update

## Current status

The Cohesity Protection Group configuration export has been simplified to a single PowerShell 5.1-compatible, GET-only script:

`DR_Ready/Get-CohesityDRReadyPGConfig.ps1`

The script currently supports active Protection Groups for:

- NAS
- SQL
- Hyper-V
- Nutanix AHV
- Oracle
- Physical

## Current workflow

1. Select a Helios cluster.
2. Retrieve active Protection Groups using GET requests only.
3. Retrieve detailed Protection Group configuration where available.
4. Save raw configuration locally as JSON, grouped by workload type.
5. Generate a keys/types-only configuration structure (`FieldStructure.txt` and `FieldStructure.json`) without production values.
6. Record collection issues in `Errors.json`.

## Output

For each selected cluster, the script creates:

```text
<cluster_timestamp>\
    Raw\
        NAS.json
        SQL.json
        HyperV.json
        AHV.json
        Oracle.json
        Physical.json

    FieldStructure.txt
    FieldStructure.json
    Errors.json
```

## Safety / scope

- Cohesity API access is strictly GET-only.
- No POST, PUT, PATCH, or DELETE operations are implemented.
- No CSV output is generated.
- Raw production values remain local in the `Raw` JSON files.
- The structure outputs contain field names and data types only.

## Next step

Validate the current script against one selected cluster first. Review the raw JSON, `FieldStructure` output, and `Errors.json`. Any additional API lookups or field handling will only be added if the actual evidence shows they are required.
