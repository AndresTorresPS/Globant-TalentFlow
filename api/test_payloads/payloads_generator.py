import json
import logging
import datetime
from random import randint

# Sets the logging level
logging.basicConfig(level=logging.INFO)


def generate_test_payload(total_simulations, batch_limit, first_id, initial_datetime, date_window_days, total_departments, total_jobs, fine):
    """
    Generates a test payload of hired employees and writes it to a JSON file.
    """
    hired_employees = []
    for i in range(1, total_simulations+1):

        # Calculate the datetime for each employee based on the initial date and the date window
        calculated_datetime = datetime.datetime.strptime(initial_datetime, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(days=(i-1)//(total_simulations//date_window_days))

        hired_employees.append({
            "id": first_id + i - 1,
            "name": f"Employee {first_id + i - 1}",
            "datetime": calculated_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "department_id": randint(1, total_departments),
            "job_id": randint(1, total_jobs)
        })

    if not fine: 
        # Alters some records of hired_employees to introduce inconsistencies and errors for data validation testing
        hired_employees[0]["id"] = -1  # ID negativo
        hired_employees[1]["name"] = "."  # Nombre con Longitud<2
        hired_employees[2]["datetime"] = "2024-13-01T12:00:00Z"  # Fecha inválida (mes 13)
        hired_employees[3]["department_id"] = 13  # ID de departamento fuera del rango
        hired_employees[4]["job_id"] = 184  # ID de trabajo fuera del rango
        hired_employees[5]["id"] = None  # ID nulo
        hired_employees[6]["name"] = None  # name nulo
        hired_employees[7]["datetime"] = None  # Fecha nula
        hired_employees[8]["department_id"] = None  # ID de departamento nulo
        hired_employees[9]["job_id"] = None  # ID de cargo nulo

    if len(hired_employees) > batch_limit:
        with open("test_payloads/test_exceeding_payload.json", "w") as f:
                json.dump(hired_employees, f)

        logging.warning(f"The generated payload exceeds the batch limit of {batch_limit} records. This is intentional for testing validation.")

    else:

        if fine:
            with open("test_payloads/test_fine_payload.json", "w") as f:
                            json.dump(hired_employees, f)

            logging.info(f"Generated fine payload with {len(hired_employees)} records, within the batch limit: {batch_limit}.")

        else:
            with open("test_payloads/test_inconsistent_payload.json", "w") as f:
                    json.dump(hired_employees, f)

            logging.info(f"Generated inconsistent payload with {len(hired_employees)} records, within the batch limit: {batch_limit}.")


# Parameters for generating the test payload
exceeding_simulations = 1001
fine_simulations = 1000  # This is the maximum allowed by the batch_limit in schemas.py
batch_limit = 1000
first_id = 2000
initial_datetime = "2024-06-01T12:00:00Z"
date_window_days = 365
total_departments = 12
total_jobs = 183

# Generate the test_exceeding_payload to test validation
generate_test_payload(exceeding_simulations, 
                      batch_limit,
                      first_id, 
                      initial_datetime, 
                      date_window_days, 
                      total_departments, 
                      total_jobs, 
                      fine=True)

"""
# Generate the test_fine_payload to test validation
generate_test_payload(fine_simulations, 
                      batch_limit,
                      first_id, 
                      initial_datetime, 
                      date_window_days, 
                      total_departments, 
                      total_jobs, 
                      fine=True)

# Generate the test_inconsistent_payload to test validation
generate_test_payload(fine_simulations,
                      batch_limit,
                      first_id,
                      initial_datetime,
                      date_window_days,
                      total_departments,
                      total_jobs,
                      fine=False) 
"""