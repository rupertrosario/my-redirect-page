# Cohesity Support Case — Garbage Reclamation Clarification

**Subject:** Clarification required: ~30 GB estimated garbage and ~30 GB reclaimed reported repeatedly, without corresponding reduction in physical usage

Hello Cohesity Support,

We are following up on our open support case regarding garbage collection and capacity reporting on our Cohesity cluster.

Over multiple days, we have observed approximately **30 GB reported as estimated garbage/reclaimable** and approximately **30 GB reported as reclaimed physical bytes**. The values appear to remain broadly unchanged, and we have not observed a corresponding reduction in reported physical storage usage. This raises a concern that the same approximately 30 GB of garbage might be remaining in the cluster, despite apparent reclamation activity. However, we recognize that these counters may have different reporting semantics, so we do not want to conclude that garbage collection has failed solely from the two readings.

We are collecting the **latest available reading** for each metric using the Helios read-only time-series statistics API; the reporting script does **not** subtract the values or claim that the reclaimed reading is a 24-hour total. We can share raw timestamped API responses and cluster capacity readings for the same observation time.

Could you please clarify:

1. What do **`EstimatedGarbageBytes`** (ApolloV2ClusterStats) and **`ReclaimedBytesPhysical`** (MRCounters) represent? In particular, is the reclaimed counter cumulative, interval-based, or a point-in-time value, and what events reset it?
2. Does a persistent approximately 30 GB estimated garbage value indicate a stable backlog that may be replenished by newly expired data, or can it indicate data that garbage collection is unable to remove?
3. Which Cohesity-supported metric, log, or diagnostic is definitive for verifying **actual physical bytes freed by garbage collection**, and how should this correlate with `localUsageBytes` and `localAvailableBytes`?
4. Can you examine the cluster's GC activity and confirm whether the same garbage objects remain pending across days, including any known blockers, reference dependencies, retention constraints, or GC failures?

Our objective is to establish **whether the 30 GB is genuinely being reclaimed and replenished, or whether the same 30 GB remains unreclaimed**, rather than infer that from two similar metric values.

Please advise what specific diagnostics or timestamps you need from us to confirm the underlying behavior.

Regards,  
Backup Operations Team

---

## Internal script reference (not part of the email)

- Branch: `Cohesity_Automations`
- Script: `Cohesity_Capacity_Garbage/Get-CohesityCapacityGarbage.ps1`
- GET-only; latest samples at a 30-second rollup interval; exports estimated garbage, reclaimed physical counter, and physical capacity as **separate** fields.
- This is a point-in-time comparison, **not** a 24-hour reclaimed-bytes calculation.
