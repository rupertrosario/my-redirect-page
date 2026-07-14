# Data Protection IAM Compliance, Audit & Evidence

## Purpose

This page defines the Identity and Access Management (IAM) control requirements and records the compliance status of the Data Protection infrastructure. It applies to Cohesity, Dell EMC Data Domain, and Dell EMC NetWorker and provides a common structure for documenting control implementation and supporting evidence.

## Scope

| Platform | In Scope |
|---|---|
| Cohesity | Yes |
| Dell EMC Data Domain | Yes |
| Dell EMC NetWorker | Yes |

## IAM Control Requirements

The corporate IAM control baseline applies to all in-scope platforms. The detailed control requirements may be inserted here as the approved source image or linked to the authoritative IAM standard.

**IAM Control Baseline:** [Insert image or authoritative standard link]

## Platform Compliance Matrix

| Platform | Directory Integration | Account Inventory | Privileged Access | Access Review & Certification | Credential & Secret Management | Audit Logging | Overall Status | Evidence Repository |
|---|---|---|---|---|---|---|---|---|
| Cohesity | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Link] |
| Dell EMC Data Domain | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Link] |
| Dell EMC NetWorker | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Link] |

Use **Compliant**, **Partially Compliant**, **Not Compliant**, or **Not Applicable**.

## Common Control Evidence

Use this section where the same enterprise process or evidence applies to all platforms. Do not repeat the same evidence under each platform.

| IAM Control | Control Implementation | Evidence Reference |
|---|---|---|
| Account Inventory | Platform user, privileged-account, and service-account inventories are maintained and reviewed. | [Account Inventory Link] |
| Privileged Access Management | Privileged access is governed through the PAM process, including privileged-account inventory, approval, and periodic review. | [PAM Review Link] |
| Access Review & Certification | Periodic access certification is performed. Orphaned and terminated-user access is removed, and service-account ownership is maintained. | [Access Certification Link] / [Service Account Register Link] |
| Credential & Secret Management | Passwords, privileged credentials, service-account secrets, and applicable certificates are managed through approved enterprise processes. Credentials are vaulted in Delinea where applicable, and annual password rotation is completed through approved Change Requests. | [Delinea Link] / [Password Rotation CR Link] / [Certificate Evidence Link] |
| Audit Logging | Administrative access and security-relevant activity are logged and retained according to the applicable enterprise logging standard. | [Audit Log Evidence Link] |

## Platform-Specific Evidence

Only document controls here when the implementation or evidence differs by platform.

### Cohesity

| IAM Control | Implementation / Configuration | Evidence Reference | Status / Notes |
|---|---|---|---|
| Directory Integration | Integrated with Active Directory for centralized authentication. | [Cohesity AD Integration Link] | [Status] |
| Platform-Specific Access Controls | [Describe Cohesity-specific roles, local-account restrictions, MFA, or exceptions.] | [Link] | [Status] |
| Platform-Specific Audit Evidence | [Describe Cohesity audit-log evidence, if not covered by the common control.] | [Link] | [Status] |

### Dell EMC Data Domain

| IAM Control | Implementation / Configuration | Evidence Reference | Status / Notes |
|---|---|---|---|
| Directory Integration | [Describe Data Domain directory integration.] | [Data Domain AD/LDAP Link] | [Status] |
| Platform-Specific Access Controls | [Describe Data Domain-specific roles, local-account restrictions, MFA, or exceptions.] | [Link] | [Status] |
| Platform-Specific Audit Evidence | [Describe Data Domain audit-log evidence, if not covered by the common control.] | [Link] | [Status] |

### Dell EMC NetWorker

| IAM Control | Implementation / Configuration | Evidence Reference | Status / Notes |
|---|---|---|---|
| Directory Integration | [Describe NetWorker directory integration.] | [NetWorker AD/LDAP Link] | [Status] |
| Platform-Specific Access Controls | [Describe NetWorker-specific roles, local-account restrictions, MFA, or exceptions.] | [Link] | [Status] |
| Platform-Specific Audit Evidence | [Describe NetWorker audit-log evidence, if not covered by the common control.] | [Link] | [Status] |

## Exceptions, Gaps & Remediation

Record only controls that are not compliant, partially compliant, or not applicable.

| Platform | IAM Control | Gap / Exception | Risk | Owner | Remediation / Compensating Control | Target Date | Approval / Evidence |
|---|---|---|---|---|---|---|---|
| [Platform] | [Control] | [Description] | [Risk] | [Owner] | [Action] | [Date] | [Link] |

## Review & Approval

| Review Item | Owner | Frequency | Last Reviewed | Next Review | Evidence |
|---|---|---|---|---|---|
| IAM compliance status | [Owner] | Quarterly / Annually | [Date] | [Date] | [Link] |
| PAM review | [Owner] | Quarterly | [Date] | [Date] | [Link] |
| Access certification | [Owner] | [Frequency] | [Date] | [Date] | [Link] |
| Credential rotation | [Owner] | Annually | [Date] | [Date] | [CR / Evidence Link] |

## Document Control

| Field | Value |
|---|---|
| Document Owner | [Name / Team] |
| Version | 0.1 |
| Effective Date | [Date] |
| Last Updated | [Date] |
| Review Frequency | Annually or upon material control change |
