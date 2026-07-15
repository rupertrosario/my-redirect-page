# Data Protection IAM Compliance & Evidence

## Purpose

This document provides implementation details and supporting evidence demonstrating compliance with the Corporate Identity & Access Management (IAM) requirements for Cohesity, Dell EMC Data Domain, and Dell EMC NetWorker.

## Scope

| Platform | In Scope |
|---|---|
| Cohesity | Yes |
| Dell EMC Data Domain | Yes |
| Dell EMC NetWorker | Yes |

## Corporate IAM Requirements (Reference)

The applicable IAM requirements are extracted from the Corporate IAM Confluence documentation and retained here as the governing reference for the in-scope Data Protection platforms.

**Corporate IAM Source Page:** [Insert Corporate Confluence link]

**Referenced Section:** IAM

**Corporate IAM Requirements:** [Insert section anchor/link]

The Corporate IAM requirements are maintained by the owning enterprise team. This document records how Cohesity, Dell EMC Data Domain, and Dell EMC NetWorker comply with those requirements and provides supporting evidence.

> Paste or retain the extracted Corporate IAM requirements below this note.

## Platform Compliance Matrix

The matrix below provides a high-level compliance summary for each in-scope Data Protection platform against the applicable Corporate IAM requirements. Detailed implementation, supporting evidence, and any approved deviations are documented in the sections that follow.

| Platform | Assets in Scope | Directory Integration | Password Policy Compliance | Account Inventory & Access Alignment | PAM Review | Access Review & Certification | Credential Management | Overall Status | Last Reviewed | Evidence Repository |
|---|---|---|---|---|---|---|---|---|---|---|
| Cohesity | [No. of Clusters] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Date] | [Link] |
| Dell EMC Data Domain | [No. of Appliances] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Date] | [Link] |
| Dell EMC NetWorker | [No. of Servers] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Date] | [Link] |

Use **Compliant**, **Partially Compliant**, **Not Compliant**, or **Not Applicable**.

## Compliance, Audit & Evidence

### Access Control

| Corporate IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| All user and service accounts are uniquely identifiable | Cohesity, Data Domain, and NetWorker user and group listings identify named AD users, AD groups, service accounts, DD Boost accounts, Helios users, and vendor local accounts, as applicable. | [Cohesity User & Group Listing] / [Data Domain User & Group Listing] / [NetWorker User & Group Listing] |
| No generic, shared, or default production accounts | Shared accounts are not used. Vendor-required local accounts are documented, controlled through Delinea where applicable, and reviewed through the PAM process. | [Platform User & Group Listings] / [PAM Review] / [Delinea Evidence] |
| Least-privilege access is enforced | Roles and group memberships are reviewed against operational responsibilities and recorded as aligned or not aligned in the platform listings. | [Platform User & Group Listings] |
| Privileged accounts are segregated from standard user accounts | Privileged access is identified separately from standard named-user access in the Cohesity, Data Domain, and NetWorker listings. | [Platform User & Group Listings] |

### Privileged Access

| Corporate IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| Privileged accounts are inventoried and approved | Privileged accounts and assignments are documented in the platform listings and reviewed through the manager-approved PAM process. | [Platform User & Group Listings] / [PAM Review] |
| Privileged access is reviewed quarterly | Privileged access is reviewed monthly through the PAM review process, exceeding the quarterly requirement. | [Monthly PAM Review] |
| Administrative access is monitored and logged | Administrative activity and applicable PAM use cases are reviewed monthly, with supporting security logs retained in Splunk. | [PAM Review] / [Splunk Evidence] |
| MFA is enabled for privileged users wherever supported | MFA and SSO are enabled for Cohesity Helios where supported and documented in the Cohesity user and group listing. Data Domain and NetWorker applicability is recorded separately. | [Cohesity Helios SSO/MFA Evidence] / [Platform Listings] |

### Access Reviews

| Corporate IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| Periodic access certifications are completed on schedule | Enterprise access certification is performed through Zilla and reviewed by the responsible application manager. | [Zilla Access Certification Evidence] |
| Orphaned accounts are removed promptly | Orphaned accounts are identified during platform, PAM, or access-certification reviews and removed as required. | [Platform Review Evidence] / [PAM Review] / [Zilla Evidence] |
| Terminated users are removed within the defined SLA | AD-integrated user access is removed through the enterprise identity lifecycle process. | [Enterprise IAM / Zilla Evidence] |
| Service-account ownership is identified and documented | Service accounts, including DD Boost accounts where applicable, are documented with platform, purpose, owner, credential location, and review status. | [Service Account Register / SharePoint Link] |

### Password Policy Compliance

| Platform | Implementation | Evidence Reference | Status / Notes |
|---|---|---|---|
| Cohesity | Password settings and authentication controls are reviewed against the applicable corporate requirements. Helios SSO and MFA are documented separately. | [Cohesity Password Policy Evidence] | [Status] |
| Dell EMC Data Domain | Password settings and authentication controls are reviewed against the applicable corporate requirements. | [Data Domain Password Policy Evidence] | [Status] |
| Dell EMC NetWorker | Password settings and authentication controls are reviewed against the applicable corporate requirements. | [NetWorker Password Policy Evidence] | [Status] |

### Credential Management

| Corporate IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| Passwords, secrets, certificates, and keys are rotated per policy | Applicable privileged, service, vendor-local, and application credentials are managed through Delinea and approved Change Requests. Password rotation is performed every 365 days. Certificates are tracked separately. | [Delinea Evidence] / [Annual Password Rotation CR] / [Certificate SharePoint Link] |
| Service-account credentials are maintained in approved vaults | Service-account and applicable DD Boost credentials are maintained in Delinea and tracked through SharePoint. | [Delinea Evidence] / [Service Account Register] |
| Expiring credentials are monitored and remediated | SharePoint tracking provides 30-, 60-, and 90-day expiry notifications for applicable credentials and certificates, with remediation completed through the approved change process. | [SharePoint Tracking] / [Change Request Evidence] |

## Platform Account Inventories

The platform user and group listings are the primary evidence for named users, AD groups, service accounts, DD Boost accounts, Helios users, vendor local accounts, assigned roles, privileged classification, ownership, access alignment, and review status.

### Cohesity

**User & Group Listing:** [Cohesity Link]

Include:
- AD users and groups
- Cohesity roles
- Privileged or standard classification
- Service accounts
- Vendor local accounts, where applicable
- Helios users
- Helios SSO status
- Helios MFA status
- Owner, access alignment, and review status

### Dell EMC Data Domain

**User & Group Listing:** [Data Domain Link]

Include:
- AD users and groups
- Data Domain roles
- Privileged or standard classification
- Service accounts
- DD Boost application accounts
- Vendor local accounts, where applicable
- Owner, access alignment, and review status

### Dell EMC NetWorker

**User & Group Listing:** [NetWorker Link]

Include:
- AD users and groups
- NetWorker roles
- Privileged or standard classification
- Service accounts
- Vendor local accounts, where applicable
- Owner, access alignment, and review status

## Service Account Register

Use the existing SharePoint evidence or linked register. A separate duplicate inventory is not required where the existing record contains the required information.

| Account | Platform | Account Type | Purpose | Owner | Vault / Credential Location | Rotation / Expiry | Review Status | Evidence |
|---|---|---|---|---|---|---|---|---|
| [Account] | [Cohesity / Data Domain / NetWorker] | [Service / DD Boost / Vendor Local] | [Purpose] | [Owner] | [Delinea / Approved Location] | [Date] | [Status] | [Link] |

## Non-Compliance, Exceptions & Deviations

The table below records approved exceptions, deviations, or non-compliant controls identified during the review. Fully compliant platforms or assets do not require an entry.

| Platform | Asset / Device ID | IAM Control | Current Status | Reason for Deviation | Risk / Impact | Compensating Control | Target Remediation Date | Evidence |
|---|---|---|---|---|---|---|---|---|
| [Platform] | [Asset ID] | [IAM Control] | [Partially Compliant / Not Compliant / Not Applicable] | [Reason] | [Risk] | [Compensating Control] | [Date] | [Link] |

## Review & Approval

| Review Item | Owner | Frequency | Last Reviewed | Next Review | Evidence |
|---|---|---|---|---|---|
| Platform user and group listings | [Owner] | [Frequency] | [Date] | [Date] | [Link] |
| PAM review | [Owner] | Monthly | [Date] | [Date] | [PAM Review Link] |
| Zilla access certification | [Application Manager] | [Enterprise Schedule] | [Date] | [Date] | [Zilla Link] |
| Password policy compliance | [Owner] | Annually or upon change | [Date] | [Date] | [Evidence Link] |
| Credential rotation | [Owner] | Every 365 days | [Date] | [Date] | [CR / Evidence Link] |

## Document Control

| Field | Value |
|---|---|
| Document Owner | [Name / Team] |
| Version | 1.0 Draft |
| Effective Date | [Date] |
| Last Updated | [Date] |
| Review Frequency | Annually or upon material control change |
