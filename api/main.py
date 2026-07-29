from fastapi import FastAPI
from routers import employees #, departments, jobs (import the others once created)

app = FastAPI(
    title="TalentFlow Ingestion API",
    description="REST API for batch data ingestion",
    version="1.0.0"
)

# Endpoints registration
app.include_router(employees.router)
# app.include_router(departments.router)
# app.include_router(jobs.router)

@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy"}