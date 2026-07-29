import io.delta.tables._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import java.util.UUID

// =============================================================
// INFRASTRUCTURE & AUTHENTICATION CONFIGURATION
// =============================================================
val storageAccount = "blobglobanttalentflow"
val container = "raw-data" 
val secretScope = "akv-talentflow-scope"
val secretSasName = "blob-sas-token"

println("[CONFIG] Retrieving SAS token from Azure Key Vault...")

val rawSasToken = dbutils.secrets.get(scope = secretScope, key = secretSasName)
val sasToken = rawSasToken.stripPrefix("?")

spark.conf.set(
  s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net",
  sasToken
)
println(s"[CONFIG] Authentication set for container: $container")

// =============================================================
// CREATE AUDIT LOG TABLE (Runs once)
// =============================================================
// This table will permanently store the log of each execution
spark.sql("""
  CREATE TABLE IF NOT EXISTS default.etl_audit_log (
    run_id STRING,
    target_table STRING,
    batch_id LONG,
    files_processed LONG,
    processed_at TIMESTAMP
  )
""")

// =============================================================
// TABLE MAPPINGS
// =============================================================
val tableMappings = Map(
  "employees" -> "hired_employees",
  "departments" -> "departments",
  "jobs" -> "jobs"
)

// =============================================================
// INCREMENTAL LOAD & UPSERT LOGIC
// =============================================================
def processTableQueue(sourceFolder: String, targetTable: String): Unit = {
  println(s"\n[STREAM] Starting Auto Loader for source: $sourceFolder -> target: $targetTable")
  
  val rawDataPath = s"wasbs://$container@$storageAccount.blob.core.windows.net/json_ingestion/$sourceFolder/"
  val checkpointPath = s"wasbs://$container@$storageAccount.blob.core.windows.net/checkpoints/${targetTable}_ingestion"
  val schemaPath = s"wasbs://$container@$storageAccount.blob.core.windows.net/schemas/${targetTable}_schema"

  // Generate a unique run ID for tracking
  val currentRunId = UUID.randomUUID().toString

  // AMMONITE PROTECTION: Reassign to local variables (primitives) to prevent 
  // the closure from attempting to read from the notebook's global environment.
  val localTargetTable = targetTable
  val localRunId = currentRunId

  val df = spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schemaPath) 
    .option("cloudFiles.inferColumnTypes", "true")
    .option("multiline", "true") 
    .load(rawDataPath)
    .withColumn("_source_file", org.apache.spark.sql.functions.col("_metadata.file_path"))

  // UPSERT logic - 100% isolated function
  def upsertToDelta(microBatchDF: DataFrame, batchId: Long): Unit = {
    // AMMONITE PROTECTION: Extract the local Spark session
    val localSpark = microBatchDF.sparkSession
    
    // AMMONITE PROTECTION: Import functions INSIDE the method
    import org.apache.spark.sql.functions.{col, explode}
    import io.delta.tables.DeltaTable
    
    microBatchDF.cache()
    
    if (!microBatchDF.isEmpty) {
        val uniqueFilesCount = microBatchDF.select("_source_file").distinct().count()
        
        // Use localSpark instead of the global session
        localSpark.sql(s"""
          INSERT INTO default.etl_audit_log 
          VALUES ('$localRunId', '$localTargetTable', $batchId, $uniqueFilesCount, current_timestamp())
        """)
        
        val cleanMicroBatchDF = microBatchDF.drop("_source_file")

        val flattenedDF = if (cleanMicroBatchDF.columns.contains("data")) {
          cleanMicroBatchDF.select(explode(col("data")).alias("record")).select("record.*")
        } else {
          cleanMicroBatchDF
        }
        
        // AMMONITE PROTECTION: Explicitly pass localSpark to DeltaTable
        val deltaTable = DeltaTable.forName(localSpark, s"default.$localTargetTable") 
        
        deltaTable.as("target")
          .merge(
            flattenedDF.as("source"), 
            "target.id = source.id" 
          )
          .whenMatched().updateAll()  
          .whenNotMatched().insertAll() 
          .execute()
    }
    
    microBatchDF.unpersist()
  }

  // Start the stream
  val query = df.writeStream
    .foreachBatch(upsertToDelta _)
    .option("checkpointLocation", checkpointPath)
    .trigger(Trigger.AvailableNow()) 
    .start()
    
  query.awaitTermination()
    
  // =========================================================
  // EXTRACT THE FINAL COUNT FROM THE AUDIT TABLE
  // =========================================================
  val auditResultDF = spark.sql(s"""
    SELECT COALESCE(SUM(files_processed), 0) 
    FROM default.etl_audit_log 
    WHERE run_id = '$currentRunId'
  """)
  
  // Safely extract the value
  val totalProcessed = if (!auditResultDF.isEmpty) auditResultDF.collect()(0).getLong(0) else 0L

  println(s"============================================================")
  println(s"[SUCCESS] Finished processing $totalProcessed pending file(s) for $targetTable")
  println(s"============================================================\n")
}

// =============================================================
// EXECUTE FOR ALL TABLES
// =============================================================
tableMappings.foreach { case (sourceFolder, targetTable) => 
  processTableQueue(sourceFolder, targetTable)
}