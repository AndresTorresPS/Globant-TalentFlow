from pyspark.sql.functions import col, lit, current_timestamp

# -------------------------------------------------------------
# 1. Azure Blob Storage configuration with SAS Token from Key Vault
# -------------------------------------------------------------
storage_account_name = "globanttalentflow"
container_name = "raw-data"
secret_scope = "akv-talentflow-scope"  
secret_sas_name = "blob-sas-token"   

# Retrieve the SAS Token from the Secret Scope
sas_token = dbutils.secrets.get(
    scope=secret_scope,
    key=secret_sas_name,
)

# Inject the SAS Token into the Spark configuration specifically for this container
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",
    sas_token
)

# Define the base path using the legacy wasbs protocol
path_data = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

# -------------------------------------------------------------
# 2. Load the raw CSV files (inferring schema & headers)
# -------------------------------------------------------------
df_departments = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_data}departments.csv")
df_jobs = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_data}jobs.csv")
df_employees = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_data}hired_employees.csv")

# -------------------------------------------------------------
# 3. DEPARTMENTS validation
# Rules: id and department should not be null
# -------------------------------------------------------------
df_departments_valid = df_departments.filter(col("id").isNotNull() & col("department").isNotNull())
df_departments_invalid = df_departments.subtract(df_departments_valid)

# Save to Delta Lake
df_departments_valid.write.format("delta").mode("overwrite").saveAsTable("departments")

# -------------------------------------------------------------
# 4. JOBS validation
# Rules: id and job should not be null
# -------------------------------------------------------------
df_jobs_valid = df_jobs.filter(col("id").isNotNull() & col("job").isNotNull())
df_jobs_invalid = df_jobs.subtract(df_jobs_valid)

# Save to Delta Lake
df_jobs_valid.write.format("delta").mode("overwrite").saveAsTable("jobs")

# -------------------------------------------------------------
# 5. HIRED_EMPLOYEES validation
# Rules required:
# - All required fields (id, name, datetime, department_id, job_id) should not be null.
# - datetime format in ISO 8601 (e.g., YYYY-MM-DDTHH:MM:SSZ). With regular expression (RLIKE).
# - Referential integrity: department_id must exist in departments, job_id in jobs.
# -------------------------------------------------------------

# Regex pattern for ISO 8601 UTC format: (YYYY-MM-DDTHH:MM:SSZ)
iso_regex = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

# Null checks for required fields
emp_base = df_employees.filter(
    col("id").isNotNull() & 
    col("name").isNotNull() & 
    col("datetime").isNotNull() & 
    col("department_id").isNotNull() & 
    col("job_id").isNotNull()
)

# Date format validation using regex
emp_date_valid = emp_base.filter(col("datetime").rlike(iso_regex))

# Referential integrity checks (Foreign keys)
valid_dept_ids = df_departments_valid.select("id").distinct()
valid_job_ids = df_jobs_valid.select("id").distinct()

emp_fully_valid = emp_date_valid.join(valid_dept_ids, emp_date_valid.department_id == valid_dept_ids.id, "inner") \
                                .drop(valid_dept_ids.id) \
                                .join(valid_job_ids, emp_date_valid.job_id == valid_job_ids.id, "inner") \
                                .drop(valid_job_ids.id)

# -------------------------------------------------------------
# 6. Capture invalid records for auditing
# -------------------------------------------------------------
# Everything that is not fully valid is considered invalid
emp_invalid = df_employees.subtract(emp_fully_valid)

# New columns for logging: error_reason and logged_at
emp_invalid_logged = emp_invalid.withColumn("error_reason", lit("Failed validation rules: nulls, invalid ISO date, or missing foreign key")) \
                                  .withColumn("logged_at", current_timestamp())

# Save valid and invalid records to Delta Lake tables
emp_fully_valid.write.format("delta").mode("overwrite").saveAsTable("hired_employees")
emp_invalid_logged.write.format("delta").mode("append").saveAsTable("bad_records_log")

print("Data migration completed successfully. Valid records are saved in Delta Lake tables, and invalid records are logged for auditing.")