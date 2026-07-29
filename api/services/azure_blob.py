import os
import json
import logging
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Configure logging for the module
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Azure Blob Storage configurations
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER")
ACCOUNT_URL = os.getenv("AZURE_STORAGE_ACCOUNT_URL")

# Initialize credentials. 
# It will automatically read AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET.
credential = DefaultAzureCredential()


def get_blob_service_client() -> BlobServiceClient:
    """
    Returns the authenticated Blob Storage client directly 
    via RBAC (Service Principal) without using SAS Tokens.
    """
    try:
        blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=credential)
        return blob_service_client
    except Exception as e:
        logger.error("Failed to initialize Blob Storage with DefaultAzureCredential: %s", e)
        raise e


def upload_json_to_blob(table_name: str, json_data: str) -> str:
    """
    Uploads a JSON payload to Blob Storage within the standardized folder structure.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    
    # Folder structure: json_ingestion / table_name / file.json
    base_folder = "json_ingestion"
    blob_name = f"{base_folder}/{table_name}/raw_{table_name}_{timestamp}.json"

    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        
        blob_client.upload_blob(json_data, overwrite=True)
        logger.info("Successfully uploaded payload to %s", blob_name)
        return blob_name
    except Exception as e:
        logger.error("Failed to upload blob %s: %s", blob_name, e)
        raise e


# ==========================================
# Local Testing 
# ==========================================
if __name__ == "__main__":
    logger.info("Starting direct connection test with RBAC (Service Principal)...")
    try:
        # 1. Test connection
        blob_service_client = get_blob_service_client()
        logger.info("Blob Storage client initialized successfully.")
        
        # 2. Test uploading a sample JSON
        test_json = json.dumps({
            "status": "success", 
            "message": "RBAC and Service Principal working 100%",
            "architecture": "Enterprise"
        })
        
        # Upload the file simulating the 'test_system' table payload
        uploaded_blob_name = upload_json_to_blob("test_system", test_json)
        logger.info("Success! Blob uploaded to: %s", uploaded_blob_name)
        
    except Exception as e:
        logger.error("Error during the test: %s", e)