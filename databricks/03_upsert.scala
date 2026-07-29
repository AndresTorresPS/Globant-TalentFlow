import io.delta.tables._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, explode, input_file_name}

// =============================================================
// INFRASTRUCTURE & AUTHENTICATION CONFIGURATION
// =============================================================
val storageAccount = "blobglobanttalentflow"
val container = "raw-data" 
val secretScope = "akv-talentflow-scope"
val secretSasName = "blob-sas-token"

println("[CONFIG] Retrieving SAS token from Azure Key Vault...")

// Retrieve the token and strip the '?' prefix if it exists
val rawSasToken = dbutils.secrets.get(scope = secretScope, key = secretSasName)
val sasToken = rawSasToken.stripPrefix("?")

// Inject the clean token into the Spark configuration
spark.conf.set(
  s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net",
  sasToken
)
println(s"[CONFIG] Authentication set for container: $container")

// =============================================================
// TABLE MAPPINGS (Source JSON folder -> Target Delta Table)
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

  val df = spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schemaPath) 
    .option("cloudFiles.inferColumnTypes", "true")
    .option("multiline", "true") 
    .load(rawDataPath)
    .withColumn("_source_file", input_file_name()) // <--- Capture the file name here

  // UPSERT logic for the micro-batch
  def upsertToDelta(microBatchDF: DataFrame, batchId: Long): Unit = {
    println(s"  -> Processing Batch ID: $batchId for table: $targetTable")
    
    if (!microBatchDF.isEmpty) {
        
        // --- NEW LOGGING LOGIC ---
        // Extract distinct file names from this specific micro-batch and log them
        val processedFiles = microBatchDF.select("_source_file").distinct().collect().map(_.getString(0))
        println(s"  -> Files detected in this batch:")
        processedFiles.foreach(fileName => println(s"     - $fileName"))
        
        // Drop the temporary file name column so it doesn't break the target Delta schema
        val cleanMicroBatchDF = microBatchDF.drop("_source_file")
        // -------------------------

        // Extract and flatten the records if Auto Loader nested them in the 'data' column
        val flattenedDF = if (cleanMicroBatchDF.columns.contains("data")) {
          cleanMicroBatchDF.select(explode(col("data")).alias("record")).select("record.*")
        } else {
          cleanMicroBatchDF
        }
        
        val deltaTable = DeltaTable.forName(s"default.$targetTable") 
        
        deltaTable.as("target")
          .merge(
            flattenedDF.as("source"), 
            "target.id = source.id" 
          )
          .whenMatched().updateAll()  
          .whenNotMatched().insertAll() 
          .execute()
    }
  }

  df.writeStream
    .foreachBatch(upsertToDelta _)
    .option("checkpointLocation", checkpointPath)
    .trigger(Trigger.AvailableNow()) 
    .start()
    .awaitTermination()
    
  println(s"[SUCCESS] Finished processing pending files for $targetTable")
}

// =============================================================
// EXECUTE FOR ALL TABLES
// =============================================================
tableMappings.foreach { case (sourceFolder, targetTable) => 
  processTableQueue(sourceFolder, targetTable)
}