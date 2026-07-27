# First Data Migration

**File:** `first_migration_logs.md`
**Execution Date:** July 27, 2026
**Environment:** Azure Databricks
**Source:** Azure Blob Storage (Container: `raw-data`)
**Destination:** Databricks Delta Lake

---

## Processing Summary

This document contains the Databricks console output during the first successful execution of the ETL pipeline. The process includes the extraction of headless CSV files, schema validation, application of data quality rules, and loading into Delta tables.

### Validation Metrics

| Target Table | Extracted Records | Valid Records | Invalid Records (Discarded) |
| :--- | :--- | :--- | :--- |
| **Departments** | 12 | 12 | 0 |
| **Jobs** | 183 | 183 | 0 |
| **Hired Employees** | 1,999 | 1,929 | 70 |

> **Note:** The 70 invalid records in the hired employees table were captured because they failed null checks, ISO 8601 date formatting, or referential integrity checks. These records were safely stored in the `bad_records_log` Delta table for further analysis.

---

## Execution Logs (Console)

```text
[2026-07-27 06:32:19] [AUDIT] INITIALIZATION  | Starting ETL pipeline execution.
[2026-07-27 06:32:19] [AUDIT] CONFIG          | Retrieving SAS token and configuring Spark session.
[2026-07-27 06:32:20] [AUDIT] CONFIG          | Targeting base path: wasbs://raw-data@blobglobanttalentflow.blob.core.windows.net/
[2026-07-27 06:32:20] [AUDIT] EXTRACTION      | Defining schemas and loading headless CSV files...
[2026-07-27 06:32:21] [AUDIT] EXTRACTION      | Loaded Departments: 12 records.
[2026-07-27 06:32:22] [AUDIT] EXTRACTION      | Loaded Jobs: 183 records.
[2026-07-27 06:32:22] [AUDIT] EXTRACTION      | Loaded Employees: 1999 records.
[2026-07-27 06:32:22] [AUDIT] TRANSFORMATION  | Validating Departments (Null checks)...
[2026-07-27 06:32:23] [AUDIT] TRANSFORMATION  | Departments Valid: 12 | Invalid: 0
[2026-07-27 06:32:32] [AUDIT] LOAD            | Departments table successfully saved to Delta Lake.
[2026-07-27 06:32:32] [AUDIT] TRANSFORMATION  | Validating Jobs (Null checks)...
[2026-07-27 06:32:33] [AUDIT] TRANSFORMATION  | Jobs Valid: 183 | Invalid: 0
[2026-07-27 06:32:36] [AUDIT] LOAD            | Jobs table successfully saved to Delta Lake.
[2026-07-27 06:32:36] [AUDIT] TRANSFORMATION  | Starting Hired Employees validation gates...
[2026-07-27 06:32:36] [AUDIT] TRANSFORMATION  | Employees passing Null Check: 1929 records.
[2026-07-27 06:32:36] [AUDIT] TRANSFORMATION  | Employees passing Date Regex: 1929 records.
[2026-07-27 06:32:38] [AUDIT] TRANSFORMATION  | Employees passing Foreign Keys: 1929 records.
[2026-07-27 06:32:40] [AUDIT] VALIDATION      | Total Hired Employees rejected (Invalid): 70 records.
[2026-07-27 06:32:40] [AUDIT] LOAD            | Writing valid Hired Employees to Delta Lake...
[2026-07-27 06:32:43] [AUDIT] LOAD            | Appending invalid records to bad_records_log Delta table...
[2026-07-27 06:32:47] [AUDIT] COMPLETION      | Data migration completed successfully!