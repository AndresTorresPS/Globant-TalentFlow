# Globant-TalentFlow

**Globant-TalentFlow** is a modern Data Engineering Proof of Concept (PoC) that bridges scalable data processing with a robust REST interface. It orchestrates historical hiring data migration, enforces strict data quality rules, manages AVRO-based disaster recovery, and exposes SQL-driven analytics—simulating a production-ready ecosystem.

---

## Table of Contents

1. [Architecture and Technologies](#-architecture-and-technologies)
2. [Release Plan (Gitflow)](#-release-plan-gitflow)
3. [API Endpoints](#-api-endpoints)
4. [Conventional Commits Guide](#-conventional-commits-guide)
5. [Deployment and Local Execution](#-deployment-and-local-execution)

---

## Architecture and Technologies

The project decouples massive processing power from the serving layer, utilizing:

*   **Data Engine (ETL/ELT):** PySpark and Databricks (Delta Lake) for historical migration, strict validation, and AVRO-format backups.
*   **REST API:** FastAPI (Python) to manage continuous ingestion, trigger backups, and serve analytical queries via `databricks-sql-connector`.
*   **Version Control:** Strict Gitflow to ensure integration and deployment quality.

---

## Release Plan (Gitflow)

This project was planned and executed using the **Gitflow** methodology, breaking down the requirements into three main delivery phases:

### Release 1.0: Data Foundation and Historical Migration
*   **Feature 1:** Creation of Delta schemas in Databricks and strict data validation (nulls, ISO 8601 formats, referential integrity). Isolation of invalid records in a `bad_records_log` table.
*   **Feature 2:** Ingestion of historical CSV files (`hired_employees.csv`, `departments.csv`, `jobs.csv`) into the Data Lake from Azure Blob Storage.

### Release 1.1: Ingestion API and Disaster Recovery
*   **Feature 3:** Implementation of a generic `POST /api/v1/ingest/{table}` endpoint with FastAPI, validating schemas via Pydantic (up to 1000 records per batch).
*   **Feature 4 & 5:** Integration of full Backup (export to AVRO) and Restore (overwrite from AVRO) processes through dedicated endpoints.

### Release 2.0: Data Analytics (SQL Metrics)
*   **Feature 6:** Analytical query grouping hires by job and department (separated by quarters - Q1 to Q4) for the year 2021.
*   **Feature 7:** Calculation of departments that hired above the global average in 2021, utilizing CTEs/Window Functions.

---

## Cloud Storage Infrastructure: Azure Blob Storage

To support the Data Engineering pipeline and simulate a Landing Zone for the raw CSV files, an Azure Storage Account was provisioned. The infrastructure was designed following Cloud Governance and Security best practices.

### Core Configuration

| Property | Value |
| :--- | :--- |
| **Resource Group** | `globant-talentflow` |
| **Location** | Brazil South |
| **Storage Account Name** | `blobglobanttalentflow` |
| **Main Service** | Azure Blob Storage |
| **Performance & Replication** | Standard / LRS (Locally Redundant Storage) |
| **Access Tier** | Cool |
| **Hierarchical Namespace** | Disabled |

### Security & Data Governance

*   **Anonymous Access:** Disabled (Prevents unauthorized public access to sensitive employee data).
*   **Authentication:** Storage Account Key Access Enabled (Required for Databricks integration via `wasbs://`).
*   **In-Transit Encryption:** Secure Transfer Required (Minimum TLS Version 1.2).

### Data Protection & Backup

*   **Blob Soft Delete:** Enabled
*   **Container Soft Delete:** Enabled
*   **Retention Period:** 7 days
*   *Note: This serves as a native infrastructure-level failsafe, complementing the Data Lake's Time Travel capabilities.*

### Resource Tagging (FinOps)

| Tag Name | Value |
| :--- | :--- |
| **Environment** | `Proof_of_Concept` |
| **Area** | `Data_Governance` |
| **Owner** | `Andres_Torres` |

---

## Secrets Management: Azure Key Vault

To adhere to strict security standards and prevent hardcoded credentials in the source code, **Azure Key Vault** was implemented as the centralized secrets management solution. This ensures secure, programmatic access to the Storage Account keys from Databricks without exposing sensitive data.

### Core Configuration

| Property | Value |
| :--- | :--- |
| **Resource Group** | `globant-talentflow` |
| **Location** | Brazil South |
| **Key Vault Name** | `talentflow-secrets` |
| **Pricing Tier** | Standard |

### Security & Access Policies

| Property | Value |
| :--- | :--- |
| **Permission Model** | Azure Role-Based Access Control (RBAC) |
| **Connectivity** | Public Endpoint (All networks) |
| **VMs for Deployment** | Disabled |
| **ARM for Template Deployment** | Disabled |
| **Azure Disk Encryption** | Disabled |

### Data Protection

*   **Soft Delete:** Enabled
*   **Retention Period:** 90 days
*   **Purge Protection:** Disabled

### Resource Tagging

| Tag Name | Value |
| :--- | :--- |
| **Environment** | `Proof_of_Concept` |
| **Area** | `Data_Governance` |
| **Owner** | `Andres_Torres` |

---

## Cloud Computing: Azure Databricks

To process the raw data and execute the Data Engineering pipelines, an Azure Databricks workspace was provisioned. The Premium tier was selected to enable enterprise-grade features, specifically the native integration with Azure Key Vault for secure secrets management (Zero Trust architecture).

### Core Configuration

| Property | Value |
| :--- | :--- |
| **Resource Group** | `globant-talentflow` |
| **Location** | Brazil South |
| **Workspace Name** | `talentflow-databricks` |
| **Pricing Tier** | Premium |
| **Workspace Type** | Serverless |

### Security, Compliance & Networking

| Property | Value |
| :--- | :--- |
| **Public Network Access** | Enabled |
| **Customer-Managed Key (CMK) for Managed Services** | Disabled |
| **Compliance Security Profile** | Disabled |
| **Enhanced Security Monitoring** | Disabled |
| **Automatic Cluster Update** | Disabled |

---

## API Endpoints

The API exposes the following main services (interactively documented at `/docs` via Swagger UI):

*   **Ingestion:** 
    *   `POST /api/v1/ingest/hired_employees`
    *   `POST /api/v1/ingest/departments`
    *   `POST /api/v1/ingest/jobs`
*   **Disaster Recovery:**
    *   `POST /api/v1/backup/{table_name}`
    *   `POST /api/v1/restore/{table_name}`
*   **Analytics:**
    *   `GET /api/v1/analytics/hires-by-quarter`
    *   `GET /api/v1/analytics/departments-above-average`

---

## Conventional Commits Guide

To keep the repository history readable and facilitate automated versioning, this project adheres to the *Conventional Commits* standard. 

*   **`feat:`** Adds a new feature to the code (e.g., *feat: adds endpoint for hires by quarter*).
*   **`fix:`** Fixes a bug in production code (the most common instead of deb).
*   **`docs:`** Documentation-only changes (like updating this `README.md` file).
*   **`style:`** Formatting changes that do not alter logic (spaces, semicolons, indentation, linting).
*   **`refactor:`** Code modifications that neither add new features nor fix bugs, but improve structure.
*   **`perf:`** Code changes strictly aimed at improving performance.
*   **`test:`** Adding, modifying, or fixing unit or integration tests.
*   **`ci:`** Changes to Continuous Integration configuration files and scripts (like GitHub Actions, Travis, or GitLab CI).
*   **`build:`** Changes affecting the build system or external dependencies (like npm, Maven, Gradle, Docker).
*   **`chore:`** Routine tasks and maintenance that do not affect production code (e.g., *chore: updates .gitignore file*).
