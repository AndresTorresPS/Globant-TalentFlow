import org.apache.spark.sql.SaveMode

// IMPORTANT: Replace this with the specific timestamp you want to restore
// e.g., "20260729_050952"
// val targetBackupTimestamp = "REPLACE_WITH_YOUR_TIMESTAMP" 
val targetBackupTimestamp = "20260729_052151" 


// =============================================================
// INFRASTRUCTURE & AUTHENTICATION CONFIGURATION
// =============================================================
val storageAccount = "blobglobanttalentflow"
val container = "backups"
val secretScope = "akv-talentflow-scope"
val secretSasName = "blob-sas-token"

println("[CONFIG] Retrieving SAS token from Azure Key Vault for restore process...")

// Retrieve the token and strip the '?' prefix if it exists
val rawSasToken = dbutils.secrets.get(scope = secretScope, key = secretSasName)
val sasToken = rawSasToken.stripPrefix("?")

// Inject the SAS Token into the Spark configuration
spark.conf.set(
  s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net",
  sasToken
)

println(s"[CONFIG] Authentication set for container: $container")

// =============================================================
// RESTORE EXECUTION
// =============================================================
val tables = Seq("hired_employees", "departments", "jobs", "bad_records_log", "detailed_bad_records_log")

if (targetBackupTimestamp == "REPLACE_WITH_YOUR_TIMESTAMP") {
    println("[ABORTED] Please provide a valid targetBackupTimestamp before running.")
} else {
    tables.foreach { tableName =>
      println(s"Initiating restore for table: $tableName...")
      
      val backupPath = s"wasbs://$container@$storageAccount.blob.core.windows.net/avro/$tableName/backup_$targetBackupTimestamp"
      
      try {
        // Read the AVRO backup
        val backupDF = spark.read.format("avro").load(backupPath)
        
        // Overwrite the Delta table completely, allowing schema changes if they occurred
        backupDF.write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", "true") 
          .saveAsTable(s"default.$tableName")
          
        println(s"[SUCCESS] $tableName restored successfully from $backupPath")
        
      } catch {
        case e: Exception => println(s"[ERROR] Failed to restore $tableName from $backupPath: ${e.getMessage}")
      }
    }
}