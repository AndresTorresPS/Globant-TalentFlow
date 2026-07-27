import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// 1. Read the existing Delta tables
val badRecordsDF = spark.read.table("bad_records_log")

// Read dimension tables and rename their 'id' columns to avoid ambiguity during joins
val departmentsDF = spark.read.table("departments").select(col("id").alias("dim_dept_id"))
val jobsDF = spark.read.table("jobs").select(col("id").alias("dim_job_id"))

// 2. Perform Left Joins to check for existence without filtering rows out
val joinedDF = badRecordsDF
  .join(departmentsDF, col("department_id").cast("int") === col("dim_dept_id"), "left")
  .join(jobsDF, col("job_id").cast("int") === col("dim_job_id"), "left")

// 3. Apply logical validations by creating new boolean columns
val detailedBadRecordsDF = joinedDF
  /* 
   * ID Validation: 
   * Attempt to cast to integer. If valid, it will not be null and must be greater than 0.
   */
  .withColumn("id_validation", 
    col("id").cast("int").isNotNull && col("id").cast("int") > 0
  )
  
  /* 
   * Name Validation: 
   * Verify that it is not null and that, after trimming spaces, its length is greater than 0.
   */
  .withColumn("name_validation", 
    col("name").isNotNull && length(trim(col("name"))) > 0
  )
  
  /* 
   * Datetime Validation: 
   * Use to_timestamp with the expected ISO 8601 format. 
   * If Spark cannot parse it, it returns null.
   */
  .withColumn("datetime_validation", 
    to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss'Z'").isNotNull
  )
  
  /* 
   * Department ID Validation 1 (Data Type & Format):
   * Verifies if the field can be parsed as a valid integer greater than 0.
   */
  .withColumn("department_id_validation", 
    col("department_id").cast("int").isNotNull && col("department_id").cast("int") > 0
  )
  
  /* 
   * Job ID Validation 1 (Data Type & Format):
   * Verifies if the field can be parsed as a valid integer greater than 0.
   */
  .withColumn("job_id_validation", 
    col("job_id").cast("int").isNotNull && col("job_id").cast("int") > 0
  )
  
  /* 
   * Department ID Validation 2 (Referential Integrity):
   * Evaluates if the Left Join found a matching record in the departments table.
   */
  .withColumn("department_id_validation_2", 
    col("dim_dept_id").isNotNull
  )
  
  /* 
   * Job ID Validation 2 (Referential Integrity):
   * Evaluates if the Left Join found a matching record in the jobs table.
   */
  .withColumn("job_id_validation_2", 
    col("dim_job_id").isNotNull
  )
  
  /*
   * Clean up: Drop the temporary dimension columns used strictly for the joins
   */
  .drop("dim_dept_id", "dim_job_id")

// 4. Write the result into a new Delta table
detailedBadRecordsDF.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true") 
  .saveAsTable("detailed_bad_records_log")

// 5. Show a sample to visually validate in Databricks
display(detailedBadRecordsDF)