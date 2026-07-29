import io.delta.tables._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

// Variables definition
val storageAccount = "blobglobanttalentflow"
val container = "raw_data"
val tableNames = Seq("employees", "departments", "jobs")

// Function to Process Each Table
def processTableQueue(tableName: String): Unit = {
  println(s"Starting Auto Loader for table: $tableName")
  
  val rawDataPath = s"wasbs://$container@$storageAccount.blob.core.windows.net/json_ingestion/$tableName/"
  val checkpointPath = s"dbfs:/mnt/checkpoints/${tableName}_ingestion"
  val schemaPath = s"dbfs:/mnt/schemas/${tableName}_schema"

  // Read Stream using Auto Loader (cloudFiles)
  val df = spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schemaPath) 
    .option("cloudFiles.inferColumnTypes", "true")
    // schemaEvolutionMode is off
    // .option("cloudFiles.schemaEvolutionMode", "addNewColumns") 
    .load(rawDataPath)

  // UPSERT logic for the micro-batch
  def upsertToDelta(microBatchDF: DataFrame, batchId: Long): Unit = {
    val deltaTable = DeltaTable.forName(s"default.$tableName") 
    
    deltaTable.as("target")
      .merge(
        microBatchDF.as("source"),
        "target.id = source.id" // Primary key matching
      )
      .whenMatched().updateAll()  // Update if exists
      .whenNotMatched().insertAll() // Insert if new
      .execute()
  }

  // Writes Stream with Trigger.AvailableNow
  df.writeStream
    .foreachBatch(upsertToDelta _)
    .option("checkpointLocation", checkpointPath)
    .trigger(Trigger.AvailableNow()) // Process everything pending, then stop
    .start()
    .awaitTermination()
}

// Executes for all tables
tableNames.foreach(processTableQueue)