Quick update — Splunk configurations for **Data Domain, NetWorker, and Avamar**:

| System           | Logging approach (today)              | Current status | IAM status                                 | Review + evidence + Confluence update                                                                                                                             |
| ---------------- | ------------------------------------- | -------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data Domain (DD) | Syslog forwarding to Splunk           | In place       | Aligned                                    | Review Splunk views/dashboards and confirm expected DD events are being captured, capture evidence, update Confluence                                             |
| NetWorker        | Splunk Universal Forwarder on Windows | In place       | Aligned (Windows agent-based forwarding)   | Review Splunk views/dashboards and confirm expected NetWorker events are being captured, capture evidence, update Confluence                                      |
| Avamar           | Syslog on port 514                    | In place (gap) | Not aligned (secure transport requirement) | Secure syslog/TLS (6514) to Splunk requires an EMC RPQ via the Dell account team (engineering qualification/support). Avamar decommission is planned for Q1 2026. |

**Dashboards & documentation**
Dashboards and the Confluence documentation are already available, but they need to be re-validated end-to-end to confirm the data capture and dashboard views are still accurate, and then the Confluence page should be updated accordingly.

**Timeline**
~1 month to complete the re-validation and Confluence update.

Also — I’m in training today, so I’ll start working on this from **Monday** onward.
