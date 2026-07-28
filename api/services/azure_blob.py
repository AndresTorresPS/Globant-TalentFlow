import os
import json
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Carga las variables de entorno (.env)
load_dotenv()

# Configuraciones de Azure Blob Storage
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER")
ACCOUNT_URL = os.getenv("AZURE_STORAGE_ACCOUNT_URL")

# Inicializamos las credenciales. 
# Leerá automáticamente AZURE_TENANT_ID, AZURE_CLIENT_ID y AZURE_CLIENT_SECRET.
credential = DefaultAzureCredential()


def get_blob_service_client() -> BlobServiceClient:
    """
    Retorna el cliente de Blob Storage autenticado directamente 
    mediante RBAC (Service Principal) sin usar SAS Tokens.
    """
    try:
        blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=credential)
        return blob_service_client
    except Exception as e:
        print(f"[Error] Fallo al inicializar Blob Storage con DefaultAzureCredential: {e}")
        raise e


def upload_json_to_blob(table_name: str, chunk_index: int, json_data: str) -> str:
    """
    Sube un bloque (chunk) específico a Blob Storage dentro de la estructura de carpetas.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    
    # Estructura de carpetas: json_ingestion / nombre_tabla / archivo.json
    base_folder = "json_ingestion"
    blob_name = f"{base_folder}/{table_name}/raw_{table_name}_{timestamp}_part{chunk_index}.json"

    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        
        blob_client.upload_blob(json_data, overwrite=True)
        return blob_name
    except Exception as e:
        print(f"[Error] Fallo al subir el blob {blob_name}: {e}")
        raise e


# ==========================================
# Bloque de prueba local
# ==========================================
if __name__ == "__main__":
    print("Iniciando prueba de conexión directa con RBAC (Service Principal)...")
    try:
        # 1. Probar conexión
        blob_service_client = get_blob_service_client()
        print("✅ Cliente de Blob Storage inicializado correctamente.")
        
        # 2. Probar subida de un JSON de ejemplo
        test_json = json.dumps({
            "status": "success", 
            "message": "RBAC y Service Principal funcionando al 100%",
            "architecture": "Enterprise"
        })
        
        # Subimos el archivo simulando que es el chunk 1 de la tabla 'test_system'
        uploaded_blob_name = upload_json_to_blob("test_system", 1, test_json)
        print(f"✅ ¡Éxito! Blob subido a: {uploaded_blob_name}")
        
    except Exception as e:
        print(f"❌ Error durante la prueba: {e}")