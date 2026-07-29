-- ==========================================
-- Display of Tables
-- ==========================================
SELECT * 
FROM etl_audit_log
ORDER BY run_id DESC
LIMIT 10;

SELECT * 
FROM departments;

SELECT * 
FROM jobs
ORDER BY id DESC 
LIMIT 10;

SELECT * 
FROM hired_employees
ORDER BY id DESC
LIMIT 10;

-- 
SELECT * 
FROM bad_records_log
ORDER BY id DESC
LIMIT 10;

-- ==========================================
-- Analytical
-- ==========================================

-- ¿How many hired per department?
SELECT 
    d.department, 
    COUNT(e.id) AS total_employees
FROM hired_employees e
JOIN departments d ON e.department_id = d.id
GROUP BY d.department
ORDER BY total_employees DESC;

-- ¿How many were hired per year and month?
SELECT 
    YEAR(CAST(datetime AS TIMESTAMP)) AS hire_year,
    MONTH(CAST(datetime AS TIMESTAMP)) AS hire_month,
    COUNT(id) AS total_hires
FROM hired_employees
GROUP BY hire_year, hire_month
ORDER BY hire_year, hire_month;