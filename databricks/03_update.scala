import io.delta.tables._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, explode}

// =============================================================
// 1. INFRASTRUCTURE & AUTHENTICATION CONFIGURATION
// =============================================================
val storageAccount = "blobglobanttalentflow"
val container = "raw-data" // Using "raw-data" as configured in previous steps
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
// 2. TABLE MAPPINGS (Source JSON folder -> Target Delta Table)
// =============================================================
// Maps the JSON folder name to the actual Delta table name
val tableMappings = Map(
  "employees" -> "hired_employees",
  "departments" -> "departments",
  "jobs" -> "jobs"
)

// =============================================================
// 3. INCREMENTAL LOAD & UPSERT LOGIC
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

  // UPSERT logic for the micro-batch
  def upsertToDelta(microBatchDF: DataFrame, batchId: Long): Unit = {
    println(s"  -> Processing Batch ID: $batchId for table: $targetTable")
    
    if (!microBatchDF.isEmpty) {
        
        // Extraer y aplanar los registros si Auto Loader los anidó en la columna 'data'
        val flattenedDF = if (microBatchDF.columns.contains("data")) {
          microBatchDF.select(explode(col("data")).alias("record")).select("record.*")
        } else {
          microBatchDF
        }
        
        val deltaTable = DeltaTable.forName(s"default.$targetTable") 
        
        deltaTable.as("target")
          .merge(
            flattenedDF.as("source"), // Usar el DF aplanado aquí
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
// 4. EXECUTE FOR ALL TABLES
// =============================================================
tableMappings.foreach { case (sourceFolder, targetTable) => 
  processTableQueue(sourceFolder, targetTable)
}