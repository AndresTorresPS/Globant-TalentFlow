from fastapi import FastAPI
from routers import ingestion

app = FastAPI(
    title="TalentFlow Ingestion API",
    description="REST API for unified-batch data ingestion: departments, jobs, and employees. Uploaded to Azure Blob Storage after referential integrity validation.",
    version="2.0.0"
)

# Endpoint registration
app.include_router(ingestion.router)

@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy"}