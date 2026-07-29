import io.delta.tables._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

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

// Ejecuta esto una vez para limpiar la memoria de Auto Loader
dbutils.fs.rm(s"wasbs://raw-data@blobglobanttalentflow.blob.core.windows.net/checkpoints/", true)
dbutils.fs.rm(s"wasbs://raw-data@blobglobanttalentflow.blob.core.windows.net/schemas/", true)

println(s"[SUCCESS] Auto Loader Cache has been removed")