# Data Protection IAM Compliance & Evidence

## Purpose

This document provides implementation details and supporting evidence demonstrating compliance with the Enterprise Identity & Access Management (IAM) requirements for Cohesity, Dell EMC Data Domain, and Dell EMC NetWorker.

## Scope

| Platform | In Scope |
|---|---|
| Cohesity | Yes |
| Dell EMC Data Domain | Yes |
| Dell EMC NetWorker | Yes |

## Enterprise IAM Requirements (Reference)

The applicable IAM requirements are extracted from the Enterprise IAM Confluence documentation and retained here as the governing reference for the in-scope Data Protection platforms.

**Enterprise IAM Source Page:** [Insert Enterprise Confluence link]

**Referenced Section:** IAM

**Enterprise IAM Requirements:** [Insert section anchor/link]

The Enterprise IAM requirements are maintained by the owning enterprise team. This document records how Cohesity, Dell EMC Data Domain, and Dell EMC NetWorker comply with those requirements and provides supporting evidence.

> Paste or retain the extracted Enterprise IAM requirements below this note.

## Platform Compliance Matrix

The matrix below provides a high-level compliance summary for each in-scope Data Protection platform against the applicable Enterprise IAM requirements. Detailed implementation, supporting evidence, and approved deviations are documented in the sections that follow.

| Platform | Assets in Scope | Directory Integration | Password Policy Compliance | Account Inventory & Access Alignment | PAM Review | Access Review & Certification | Credential Management | Overall Status | Last Reviewed | Evidence Repository |
|---|---|---|---|---|---|---|---|---|---|---|
| Cohesity | [No. of Clusters] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Date] | [Link] |
| Dell EMC Data Domain | [No. of Appliances] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Date] | [Link] |
| Dell EMC NetWorker | [No. of Servers] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Status] | [Date] | [Link] |

Use the Confluence Status macro for **Compliant**, **Partially Compliant**, **Not Compliant**, or **Not Applicable**.

## Non-Compliance, Exceptions & Deviations

The table below records approved exceptions, deviations, or non-compliant controls identified during the review. Fully compliant platforms or assets do not require an entry.

| Platform | Asset / Device ID | IAM Control | Current Status | Reason for Deviation | Risk / Impact | Compensating Control | Target Remediation Date | Evidence |
|---|---|---|---|---|---|---|---|---|
| [Platform] | [Asset ID] | [IAM Control] | [Partially Compliant / Not Compliant / Not Applicable] | [Reason] | [Risk] | [Compensating Control] | [Date] | [Link] |

## Compliance, Audit & Evidence

Documents how the platforms meet Enterprise IAM requirements and links the supporting evidence.

### Access Control

Covers authentication, authorization, account governance, and access alignment.

#### Directory Integration

Shows how Cohesity, Data Domain, and NetWorker use Enterprise AD.

| Platform | Implementation | Evidence Reference |
|---|---|---|
| Cohesity | Centralized authentication is implemented through Active Directory. Cohesity Helios SSO is documented separately. | [Cohesity AD / SSO Evidence] |
| Dell EMC Data Domain | Centralized authentication is implemented through Active Directory or the approved directory integration. | [Data Domain AD Evidence] |
| Dell EMC NetWorker | Centralized authentication is implemented through Active Directory or the approved directory integration. | [NetWorker AD Evidence] |

#### Account Inventory & Access Alignment

Records users, groups, service accounts, DD Boost accounts, roles, ownership, and privilege status.

| Enterprise IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| All user and service accounts are uniquely identifiable | The Platform User & Group Listing identifies named AD users, AD groups, service accounts, DD Boost accounts, Helios users, and vendor local accounts across Cohesity, Data Domain, and NetWorker, as applicable. | [Platform User & Group Listing] |
| No generic, shared, or default production accounts | Shared accounts are not used. Vendor-required local accounts are documented, controlled through Delinea where applicable, and reviewed through the PAM process. | [Platform User & Group Listing] / [PAM Review] / [Delinea Evidence] |
| Least-privilege access is enforced | Roles and group memberships are reviewed against operational responsibilities and recorded as aligned or not aligned in the Platform User & Group Listing. | [Platform User & Group Listing] |
| Privileged accounts are segregated from standard user accounts | Privileged access is identified separately from standard named-user access in the Platform User & Group Listing. | [Platform User & Group Listing] |

### Privileged Access

Covers PAM review, monitoring, approval, and privileged access controls.

| Enterprise IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| Privileged accounts are inventoried and approved | Privileged accounts and assignments are documented in the Platform User & Group Listing and reviewed through the manager-approved PAM process. | [Platform User & Group Listing] / [PAM Review] |
| Privileged access is reviewed quarterly | Privileged access is reviewed monthly through the PAM review process, exceeding the quarterly requirement. | [Monthly PAM Review] |
| Administrative access is monitored and logged | Administrative activity and applicable PAM use cases are reviewed monthly, with supporting security logs retained in Splunk. | [PAM Review] / [Splunk Evidence] |
| MFA is enabled for privileged users wherever supported | MFA and SSO are enabled for Cohesity Helios where supported and documented in the Platform User & Group Listing. Data Domain and NetWorker applicability is recorded separately. | [Cohesity Helios SSO/MFA Evidence] / [Platform User & Group Listing] |

### Access Reviews

Covers Zilla certification for Cohesity and manual certification for Data Domain and NetWorker.

| Enterprise IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| Periodic access certifications are completed on schedule | Cohesity access certification is performed through Zilla and reviewed by the responsible application manager. Data Domain and NetWorker access certifications are completed manually and documented. | [Cohesity Zilla Certification Evidence] / [Data Domain Manual Certification Evidence] / [NetWorker Manual Certification Evidence] |
| Orphaned accounts are removed promptly | Orphaned accounts are identified during platform, PAM, or access-certification reviews and removed as required. | [Platform Review Evidence] / [PAM Review] / [Certification Evidence] |
| Terminated users are removed within the defined SLA | AD-integrated user access is removed through the enterprise identity lifecycle process. | [Enterprise IAM Evidence] |
| Service-account ownership is identified and documented | Service accounts and DD Boost accounts are listed in the Platform User & Group Listing. Ownership, credential location, rotation or expiry, and review status are maintained in SharePoint. | [Platform User & Group Listing] / [SharePoint Evidence] |

### Credential Management

Covers password policy, Delinea, rotation, secrets, and certificate tracking.

#### Password Policy Compliance

| Platform | Implementation | Evidence Reference | Status / Notes |
|---|---|---|---|
| Cohesity | AD-integrated users inherit Enterprise AD password controls. Cohesity password and authentication settings are validated programmatically through the Cohesity API. Vendor-local and service-account credentials are managed through Delinea and rotated every 365 days through the approved Change Request process. Helios uses SSO and MFA. | [Cohesity Validation Script / Output] / [Delinea Evidence] / [Rotation CR] / [Helios SSO/MFA Evidence] | [Status] |
| Dell EMC Data Domain | AD-integrated users inherit Enterprise AD password controls. Vendor-local, service, and DD Boost credentials are managed through Delinea and rotated every 365 days through the approved Change Request process. | [Data Domain Password Policy Evidence] / [Delinea Evidence] / [Rotation CR] | [Status] |
| Dell EMC NetWorker | AD-integrated users inherit Enterprise AD password controls. Vendor-local and service-account credentials are managed through Delinea and rotated every 365 days through the approved Change Request process. | [NetWorker Password Policy Evidence] / [Delinea Evidence] / [Rotation CR] | [Status] |

#### Credential Vaulting, Rotation & Certificate Management

| Enterprise IAM Requirement | Implementation | Evidence Reference |
|---|---|---|
| Passwords, secrets, certificates, and keys are rotated per policy | Applicable privileged, service, vendor-local, and application credentials are managed through Delinea and approved Change Requests. Password rotation is performed every 365 days. SSL/TLS certificates are tracked separately. | [Delinea Evidence] / [Annual Password Rotation CR] / [Certificate SharePoint Link] |
| Service-account credentials are maintained in approved vaults | Service-account and applicable DD Boost credentials are maintained in Delinea. Ownership, credential location, rotation or expiry, and review status are maintained in SharePoint. | [Delinea Evidence] / [SharePoint Evidence] |
| Expiring credentials are monitored and remediated | SharePoint tracking provides 30-, 60-, and 90-day expiry notifications for applicable credentials and SSL/TLS certificates, with remediation completed through the approved change process. | [SharePoint Tracking] / [Change Request Evidence] |

## Platform User & Group Listing

The Platform User & Group Listing is maintained as a separate Confluence page and serves as the authoritative account inventory for all Data Protection platforms.

**Evidence:** [Platform User & Group Listing]

It includes named AD users, AD groups, platform roles, privileged or standard classification, service accounts, DD Boost accounts, vendor local accounts where applicable, Helios users, Helios SSO and MFA status, ownership, access alignment, and review status. Credential location, rotation, and expiry details are maintained separately in SharePoint and Delinea.

## Review & Approval

| Review Item | Owner | Frequency | Last Reviewed | Next Review | Evidence |
|---|---|---|---|---|---|
| Platform User & Group Listing | [Owner] | [Frequency] | [Date] | [Date] | [Link] |
| PAM review | [Owner] | Monthly | [Date] | [Date] | [PAM Review Link] |
| Cohesity Zilla access certification | [Application Manager] | [Enterprise Schedule] | [Date] | [Date] | [Zilla Link] |
| Data Domain manual access certification | [Application Manager] | [Schedule] | [Date] | [Date] | [Evidence Link] |
| NetWorker manual access certification | [Application Manager] | [Schedule] | [Date] | [Date] | [Evidence Link] |
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
