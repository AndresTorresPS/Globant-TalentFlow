import json
import logging
import datetime

# Sets the logging level
logging.basicConfig(level=logging.INFO)

# Parameters for generating the test payload
initial_datetime = "2024-06-01T12:00:00Z"
date_window_days = 200
last_id = 1999
total_rows = 1200 
total_departments = 12
total_jobs = 183

hired_employees = []
for i in range(1, total_rows + 1):
    hired_employees.append({
        "id": last_id + i,
        "name": f"Employee {last_id + i}",
        "datetime": (datetime.datetime.strptime(initial_datetime, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(days=(i-1)//(total_rows//date_window_days))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "department_id": (i-1)//(total_rows//total_departments),
        "job_id": (i-1)//(total_rows//total_jobs)
    })

# Alters some records of hired_employees to introduce inconsistencies and errors for data validation testing
hired_employees[0]["id"] = -1  # ID negativo
hired_employees[1]["name"] = "."  # Nombre con Longitud<2
hired_employees[2]["datetime"] = "2024-06-01T12:00:00"  # Formato de fecha incorrecto
hired_employees[3]["department_id"] = ""  # ID de departamento negativo
hired_employees[4]["job_id"] = -1  # ID de trabajo negativo
hired_employees[5]["id"] = None  # ID nulo
hired_employees[6]["name"] = None  # name nulo
hired_employees[7]["datetime"] = "2024-13-01T12:00:00Z"  # Fecha inválida (mes 13)
hired_employees[8]["department_id"] = None  # ID de departamento nulo
hired_employees[9]["job_id"] = None  # ID de cargo nulo

with open("test_employees_payload.json", "w") as f:
    json.dump(hired_employees, f)

# Escribe el print en forma de log
logging.info(f"test_employees_payload.json creado con {len(hired_employees)} registros.")

