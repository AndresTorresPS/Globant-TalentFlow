from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime

# =============================================================
# HELPER FUNCTION: Audit Logging
# =============================================================
def audit_log(step, message):
    """Prints a formatted, timestamped log for pipeline auditing."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [AUDIT] {step.ljust(15)} | {message}")

audit_log("INITIALIZATION", "Starting ETL pipeline execution.")

# =============================================================
# AZURE BLOB STORAGE CONFIGURATION (SAS Token via Key Vault)
# =============================================================
storage_account_name = "blobglobanttalentflow"
container_name = "raw-data"
secret_scope = "akv-talentflow-scope"  
secret_sas_name = "blob-sas-token"   

audit_log("CONFIG", "Retrieving SAS token and configuring Spark session.")

# Retrieve the SAS Token from the Secret Scope
sas_token = dbutils.secrets.get(scope=secret_scope, key=secret_sas_name)

# Inject the SAS Token into the Spark configuration specifically for this container
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",
    sas_token
)

# Define the base path using the legacy wasbs protocol
path_data = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
audit_log("CONFIG", f"Targeting base path: {path_data}")

# =============================================================
# SCHEMA DEFINITION & DATA EXTRACTION
# =============================================================
audit_log("EXTRACTION", "Defining schemas and loading headless CSV files...")

# Define explicit schemas since the CSV files do not contain headers
dept_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("department", StringType(), True)
])

job_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("job", StringType(), True)
])

emp_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("datetime", StringType(), True),  # Kept as string for Regex validation
    StructField("department_id", IntegerType(), True),
    StructField("job_id", IntegerType(), True)
])

# Load the data WITHOUT headers and applying the explicit schemas
df_departments = spark.read.option("header", "false").schema(dept_schema).csv(f"{path_data}departments.csv")
df_jobs = spark.read.option("header", "false").schema(job_schema).csv(f"{path_data}jobs.csv")
df_employees = spark.read.option("header", "false").schema(emp_schema).csv(f"{path_data}hired_employees.csv")

audit_log("EXTRACTION", f"Loaded Departments: {df_departments.count()} records.")
audit_log("EXTRACTION", f"Loaded Jobs: {df_jobs.count()} records.")
audit_log("EXTRACTION", f"Loaded Employees: {df_employees.count()} records.")

# =============================================================
# DEPARTMENTS VALIDATION & LOAD
# Rule: 'id' and 'department' must not be null
# =============================================================
audit_log("TRANSFORMATION", "Validating Departments (Null checks)...")
df_departments_valid = df_departments.filter(col("id").isNotNull() & col("department").isNotNull())
df_departments_invalid = df_departments.subtract(df_departments_valid)

audit_log("TRANSFORMATION", f"Departments Valid: {df_departments_valid.count()} | Invalid: {df_departments_invalid.count()}")

# Save to Delta Lake
df_departments_valid.write.format("delta").mode("overwrite").saveAsTable("departments")
audit_log("LOAD", "Departments table successfully saved to Delta Lake.")

# =============================================================
# JOBS VALIDATION & LOAD
# Rule: 'id' and 'job' must not be null
# =============================================================
audit_log("TRANSFORMATION", "Validating Jobs (Null checks)...")
df_jobs_valid = df_jobs.filter(col("id").isNotNull() & col("job").isNotNull())
df_jobs_invalid = df_jobs.subtract(df_jobs_valid)

audit_log("TRANSFORMATION", f"Jobs Valid: {df_jobs_valid.count()} | Invalid: {df_jobs_invalid.count()}")

# Save to Delta Lake
df_jobs_valid.write.format("delta").mode("overwrite").saveAsTable("jobs")
audit_log("LOAD", "Jobs table successfully saved to Delta Lake.")

# =============================================================
# HIRED_EMPLOYEES VALIDATION
# Rules: 
# - No nulls in required fields
# - 'datetime' must match ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)
# - Referential integrity (department_id and job_id must exist)
# =============================================================
audit_log("TRANSFORMATION", "Starting Hired Employees validation gates...")

# Regex pattern for ISO 8601 UTC format
iso_regex = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

# Gate 5A: Null checks for all fields
emp_base = df_employees.filter(
    col("id").isNotNull() & 
    col("name").isNotNull() & 
    col("datetime").isNotNull() & 
    col("department_id").isNotNull() & 
    col("job_id").isNotNull()
)
audit_log("TRANSFORMATION", f"Employees passing Null Check: {emp_base.count()} records.")

# Gate 5B: Date format validation using Regex
emp_date_valid = emp_base.filter(col("datetime").rlike(iso_regex))
audit_log("TRANSFORMATION", f"Employees passing Date Regex: {emp_date_valid.count()} records.")

# Gate 5C: Referential integrity checks (Foreign Keys)
valid_dept_ids = df_departments_valid.select("id").distinct()
valid_job_ids = df_jobs_valid.select("id").distinct()

emp_fully_valid = emp_date_valid.join(valid_dept_ids, emp_date_valid.department_id == valid_dept_ids.id, "inner") \
                                .drop(valid_dept_ids.id) \
                                .join(valid_job_ids, emp_date_valid.job_id == valid_job_ids.id, "inner") \
                                .drop(valid_job_ids.id)

audit_log("TRANSFORMATION", f"Employees passing Foreign Keys: {emp_fully_valid.count()} records.")

# =============================================================
# CAPTURE INVALID RECORDS & FINAL LOAD
# =============================================================
# Any record from the original dataframe not present in the fully valid one is considered invalid
emp_invalid = df_employees.subtract(emp_fully_valid)
invalid_count = emp_invalid.count()
audit_log("VALIDATION", f"Total Hired Employees rejected (Invalid): {invalid_count} records.")

# Add audit columns to the bad records dataframe
emp_invalid_logged = emp_invalid.withColumn("error_reason", lit("Failed validation rules: nulls, invalid ISO date, or missing foreign key")) \
                                .withColumn("logged_at", current_timestamp())

# Save valid records to Delta Lake
audit_log("LOAD", "Writing valid Hired Employees to Delta Lake...")
emp_fully_valid.write.format("delta").mode("overwrite").saveAsTable("hired_employees")

# Append bad records to the log table only if any exist
if invalid_count > 0:
    audit_log("LOAD", "Writing invalid records to bad_records_log Delta table...")
    emp_invalid_logged.write.format("delta").mode("overwrite").saveAsTable("bad_records_log")
else:
    audit_log("LOAD", "No invalid records to write. Skipping bad_records_log.")

audit_log("COMPLETION", "Data migration completed successfully!")