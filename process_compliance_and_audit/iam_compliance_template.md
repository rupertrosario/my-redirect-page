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

The applicable IAM control requirements are extracted from the Corporate IAM Confluence documentation and retained here as the governing reference for the in-scope Data Protection platforms.

**Corporate IAM Source Page:** [Insert link to the Corporate Confluence page]

**Referenced Section:** IAM

**Corporate IAM Requirements:** [Insert link to the pasted IAM requirements / first image or section anchor]

The Corporate IAM requirements are maintained by the owning enterprise team. This document records how Cohesity, Dell EMC Data Domain, and Dell EMC NetWorker align with those requirements and provides the supporting compliance evidence.

> Paste or retain the extracted Corporate IAM requirements below this note.

## Platform Compliance Matrix

The matrix below records the current IAM compliance status of the in-scope platforms. Detailed user, group, role, privilege, SSO, MFA, and access-alignment information is maintained in the corresponding platform account inventory.

| Platform | Directory Integration | Password Policy Compliance | Account Inventory & Access Alignment | PAM Review | Access Review & Certification | Credential Management | Overall Status | Evidence Repository |
|---|---|---|---|---|---|---|---|---|
| Cohesity | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Link] |
| Dell EMC Data Domain | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Link] |
| Dell EMC NetWorker | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Link] |

Use **Compliant**, **Partially Compliant**, **Not Compliant**, or **Not Applicable**.

## Compliance Evidence Mapping

Evidence links are maintained within this document. Account inventory evidence is platform-specific. Enterprise processes are referenced once where they apply consistently across all platforms.

| IAM Control | Control Implementation | Evidence Reference |
|---|---|---|
| Directory Integration | Centralized authentication is implemented through the approved directory or identity provider, where supported. | [Platform evidence links below] |
| Password Policy Compliance | Platform password settings are reviewed against the approved corporate password baseline. Any platform limitation or deviation is documented. | [Password Policy Compliance Evidence Link] |
| Privileged Access Management | Privileged-account review, approval, access validation, and applicable audit evidence are maintained through the PAM review process. | [PAM Review Link] |
| Access Review & Certification | Periodic access certification is performed. Orphaned and terminated-user access is removed, and service-account ownership and certification evidence are maintained. | [Access Certification Link] / [Service Account & Certificate SharePoint Link] |
| Credential Management | Privileged credentials, service-account passwords, secrets, and applicable certificates are managed through approved processes. Delinea is used where applicable, and annual password rotation is completed through approved Change Requests. | [Delinea Link] / [Annual Password Rotation CR Link] / [Certificate Evidence Link] |

## Platform Account Inventory & Compliance Evidence

The account inventory is the primary evidence for platform access controls. Each platform inventory should include users and groups, assigned roles, privileged or non-privileged classification, account or group owner, access alignment, review status, and evidence notes.

### Cohesity

**Account Inventory:** [Cohesity Account Inventory Link]

| User / Group / Account | Account Type | Assigned Role(s) | Privileged? | Owner | Access Aligned? | SSO | MFA | Review Status / Notes |
|---|---|---|---|---|---|---|---|---|
| [User / Group] | [Named / Group / Service / Local / Helios] | [Role] | [Yes / No] | [Owner] | [Yes / No] | [Enabled / Not Applicable / Exception] | [Enabled / Not Applicable / Exception] | [Notes] |
| Helios users | [Named / Group] | [Role(s)] | [Yes / No] | [Owner] | [Yes / No] | [Add SSO configuration] | [Add MFA configuration] | [Compliance notes] |

**Directory Integration Evidence:** [Cohesity AD / SSO Evidence Link]

**Password Policy Compliance Evidence:** [Cohesity Password Policy Evidence Link]

### Dell EMC Data Domain

**Account Inventory:** [Data Domain Account Inventory Link]

| User / Group / Account | Account Type | Assigned Role(s) | Privileged? | Owner | Access Aligned? | Review Status / Notes |
|---|---|---|---|---|---|---|
| [User / Group] | [Named / Group / Service / Local] | [Role] | [Yes / No] | [Owner] | [Yes / No] | [Notes] |

**Directory Integration Evidence:** [Data Domain AD / LDAP Evidence Link]

**Password Policy Compliance Evidence:** [Data Domain Password Policy Evidence Link]

### Dell EMC NetWorker

**Account Inventory:** [NetWorker Account Inventory Link]

| User / Group / Account | Account Type | Assigned Role(s) | Privileged? | Owner | Access Aligned? | Review Status / Notes |
|---|---|---|---|---|---|---|
| [User / Group] | [Named / Group / Service / Local] | [Role] | [Yes / No] | [Owner] | [Yes / No] | [Notes] |

**Directory Integration Evidence:** [NetWorker AD / LDAP Evidence Link]

**Password Policy Compliance Evidence:** [NetWorker Password Policy Evidence Link]

## Exceptions, Gaps & Remediation

Record only controls that are not compliant, partially compliant, or not applicable.

| Platform | IAM Control | Gap / Exception | Risk | Owner | Remediation / Compensating Control | Target Date | Approval / Evidence |
|---|---|---|---|---|---|---|---|
| [Platform] | [Control] | [Description] | [Risk] | [Owner] | [Action] | [Date] | [Link] |

## Review & Approval

| Review Item | Owner | Frequency | Last Reviewed | Next Review | Evidence |
|---|---|---|---|---|---|
| Platform account inventory and access alignment | [Owner] | Quarterly / Annually | [Date] | [Date] | [Platform Inventory Link] |
| PAM review | [Owner] | Quarterly | [Date] | [Date] | [PAM Review Link] |
| Access certification | [Owner] | [Frequency] | [Date] | [Date] | [Certification Link] |
| Password policy compliance | [Owner] | Annually / upon change | [Date] | [Date] | [Evidence Link] |
| Credential and password rotation | [Owner] | Annually | [Date] | [Date] | [CR / Evidence Link] |

## Document Control

| Field | Value |
|---|---|
| Document Owner | [Name / Team] |
| Version | 0.3 |
| Effective Date | [Date] |
| Last Updated | [Date] |
| Review Frequency | Annually or upon material control change |
