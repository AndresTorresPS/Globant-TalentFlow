import json
import logging
import datetime
from random import randint

# Sets the logging level
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ==========================================
# 1. Static Data (Catalogs)
# ==========================================
DEPARTMENTS = [
    {"id": 1, "department": "Product Management"}, {"id": 2, "department": "Sales"},
    {"id": 3, "department": "Research and Development"}, {"id": 4, "department": "Business Development"},
    {"id": 5, "department": "Engineering"}, {"id": 6, "department": "Human Resources"},
    {"id": 7, "department": "Services"}, {"id": 8, "department": "Support"},
    {"id": 9, "department": "Marketing"}, {"id": 10, "department": "Training"},
    {"id": 11, "department": "Legal"}, {"id": 12, "department": "Accounting"}, 
    {"id": 13, "department": "Data Science"}
]

# Using a subset just for the generator logic to work smoothly
JOBS = [{"id": i, "job": f"Job Title {i}"} for i in range(1, 184)] 

# ==========================================
# 2. Generator Function
# ==========================================
def generate_test_payload(scenario: str, total_simulations: int, batch_limit: int, first_id: int, initial_datetime: str, date_window_days: int):
    """
    Generates a unified test payload (departments, jobs, employees) and writes it to a JSON file.
    scenario options: 'fine', 'exceeding', 'inconsistent', 'ref_error'
    """
    hired_employees = []
    total_departments = len(DEPARTMENTS)
    total_jobs = len(JOBS)

    # Generate base employees
    for i in range(1, total_simulations + 1):
        calculated_datetime = datetime.datetime.strptime(initial_datetime, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(days=(i-1)//(max(1, total_simulations//date_window_days)))

        hired_employees.append({
            "id": first_id + i - 1,
            "name": f"Employee {first_id + i - 1}",
            "datetime": calculated_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "department_id": randint(1, total_departments),
            "job_id": randint(1, total_jobs)
        })

    # Apply specific errors based on the chosen scenario
    if scenario == "inconsistent":
        # Only schema validation errors (types, lengths, nulls)
        hired_employees[0]["id"] = -1  # Negative ID
        hired_employees[1]["name"] = "."  # Name length < 2
        hired_employees[2]["datetime"] = "2024-13-01T12:00:00Z"  # Invalid date format
        hired_employees[3]["id"] = None  # Null ID
        hired_employees[4]["name"] = None  # Null Name
        hired_employees[5]["datetime"] = None  # Null Date
        hired_employees[6]["department_id"] = None  # Null Dept ID
        hired_employees[7]["job_id"] = None  # Null Job ID
        
    elif scenario == "ref_error":
        # Only referential integrity errors (Foreign Keys)
        hired_employees[0]["department_id"] = 9999  # Dept doesn't exist in DEPARTMENTS list
        hired_employees[1]["job_id"] = 9999  # Job doesn't exist in JOBS list

    # Build the Unified Payload
    unified_payload = {
        "departments": DEPARTMENTS,
        "jobs": JOBS,
        "employees": hired_employees
    }

    # Save to file
    filename = f"test_payloads/test_{scenario}_payload.json"
    with open(filename, "w") as f:
        json.dump(unified_payload, f)

    # Logging results
    if scenario == "exceeding":
        logging.warning(f"[{scenario}] Payload generated with {total_simulations} employees. Exceeds limit of {batch_limit}. File: {filename}")
    elif scenario == "inconsistent":
        logging.info(f"[{scenario}] Payload generated with intentional schema validation errors. File: {filename}")
    elif scenario == "ref_error":
        logging.info(f"[{scenario}] Payload generated with intentional referential integrity (foreign key) errors. File: {filename}")
    else:
        logging.info(f"[{scenario}] Perfect payload generated successfully within limits. File: {filename}")


# ==========================================
# 3. Execution
# ==========================================
if __name__ == "__main__":
    # General Parameters
    BATCH_LIMIT = 1000
    FIRST_ID = 2000
    INITIAL_DATETIME = "2024-06-01T12:00:00Z"
    DATE_WINDOW_DAYS = 365

    # 1. Generate the EXCEEDING payload (1001 records, should fail at Pydantic max_length)
    generate_test_payload(
        scenario="exceeding",
        total_simulations=1001,
        batch_limit=BATCH_LIMIT,
        first_id=FIRST_ID,
        initial_datetime=INITIAL_DATETIME,
        date_window_days=DATE_WINDOW_DAYS
    )

    # 2. Generate the FINE payload (1000 records, should pass everything and upload to Azure)
    generate_test_payload(
        scenario="fine",
        total_simulations=1000,
        batch_limit=BATCH_LIMIT,
        first_id=FIRST_ID,
        initial_datetime=INITIAL_DATETIME,
        date_window_days=DATE_WINDOW_DAYS
    )

    # 3. Generate the INCONSISTENT payload (1000 records, should fail Pydantic schema validation)
    generate_test_payload(
        scenario="inconsistent",
        total_simulations=1000,
        batch_limit=BATCH_LIMIT,
        first_id=FIRST_ID,
        initial_datetime=INITIAL_DATETIME,
        date_window_days=DATE_WINDOW_DAYS
    )

    # 4. Generate the REF_ERROR payload (1000 records, should pass schema but fail @model_validator)
    generate_test_payload(
        scenario="ref_error",
        total_simulations=1000,
        batch_limit=BATCH_LIMIT,
        first_id=FIRST_ID,
        initial_datetime=INITIAL_DATETIME,
        date_window_days=DATE_WINDOW_DAYS
    )