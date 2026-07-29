import org.apache.spark.sql.SaveMode
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

// =============================================================
// 1. INFRASTRUCTURE & AUTHENTICATION CONFIGURATION
// =============================================================
val storageAccount = "blobglobanttalentflow"
val container = "backups" 
val secretScope = "akv-talentflow-scope"  
val secretSasName = "blob-sas-token"   

println("[CONFIG] Retrieving SAS token from Azure Key Vault...")

// Obtener el token y limpiar el prefijo '?' si existe (Spark lo requiere así)
val rawSasToken = dbutils.secrets.get(scope = secretScope, key = secretSasName)
val sasToken = rawSasToken.stripPrefix("?")

// Inject the SAS Token into the Spark configuration for the 'backups' container
spark.conf.set(
  s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net",
  sasToken
)

println(s"[CONFIG] Authentication set for container: $container")

// =============================================================
// 2. BACKUP EXECUTION
// =============================================================
val tables = Seq("hired_employees", "departments", "jobs", "bad_records_log", "detailed_bad_records_log")

// Generate Timestamp for Versioning (Format: YYYYMMDD_HHMMSS)
val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now())

tables.foreach { tableName =>
  println(s"Starting backup for table: $tableName...")
  
  try {
    val df = spark.read.table(s"default.$tableName")
    val backupPath = s"wasbs://$container@$storageAccount.blob.core.windows.net/avro/$tableName/backup_$timestamp"
    
    df.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(backupPath)
      
    println(s"[SUCCESS] $tableName backed up to: $backupPath")
    
  } catch {
    case e: Exception => println(s"[ERROR] Failed to backup $tableName: ${e.getMessage}")
  }
}