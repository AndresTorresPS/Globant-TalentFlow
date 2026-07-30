from fastapi import APIRouter, HTTPException
from databricks import sql
import os
from dotenv import load_dotenv

router = APIRouter(
    prefix="/api/v3/reports",
    tags=["Reports"]
)

# To read variables from .env file
load_dotenv()

# Environment variables for Databricks connection
DBX_SERVER_HOSTNAME = os.getenv("DBX_SERVER_HOSTNAME")
DBX_HTTP_PATH = os.getenv("DBX_HTTP_PATH")
DBX_TOKEN = os.getenv("DBX_TOKEN")

def execute_databricks_query(query: str):
    try:
        with sql.connect(
            server_hostname=DBX_SERVER_HOSTNAME,
            http_path=DBX_HTTP_PATH,
            access_token=DBX_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/quarterly-hires-2021")
async def get_hires_by_quarter():
    """Returns number of employees hired for each job and department in 2021 by quarter."""
    query = """
        WITH employees AS (
            SELECT
                d.department,
                j.job,
                QUARTER(CAST(e.datetime AS TIMESTAMP)) AS quarter
            FROM default.hired_employees e
            JOIN default.departments d
                ON e.department_id = d.id
            JOIN default.jobs j
                ON e.job_id = j.id
            WHERE YEAR(CAST(e.datetime AS TIMESTAMP)) = 2021
        )

        SELECT 
            department,
            job,
            COALESCE(Q1, 0) AS Q1,
            COALESCE(Q2, 0) AS Q2,
            COALESCE(Q3, 0) AS Q3,
            COALESCE(Q4, 0) AS Q4
        FROM employees
        PIVOT (
            COUNT(*) FOR quarter IN (
                1 AS Q1,
                2 AS Q2,
                3 AS Q3,
                4 AS Q4
            )
        )
        ORDER BY department, job;
    """
    return execute_databricks_query(query)

@router.get("/2021-top-hiring-departments")
async def get_top_departments():
    """Returns departments that hired more employees than the average across all departments in 2021."""
    query = """
        WITH dept_hires AS (
            SELECT 
                d.id,
                d.department,
                COUNT(e.id) AS hired
            FROM default.hired_employees e
            JOIN default.departments d
                ON e.department_id = d.id
            WHERE YEAR(CAST(e.datetime AS TIMESTAMP)) = 2021
            GROUP BY 
                d.id, 
                d.department
        )

        SELECT 
            id, 
            department, 
            hired
        FROM dept_hires
        WHERE hired > (SELECT AVG(hired) FROM dept_hires)
        ORDER BY hired DESC;
    """
    return execute_databricks_query(query)