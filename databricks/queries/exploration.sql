-- 1. Revisar los Departamentos
SELECT * 
FROM departments 
LIMIT 10;

-- 2. Revisar los Trabajos (Jobs)
SELECT * 
FROM jobs 
LIMIT 10;

-- 3. Revisar los Empleados Contratados (Datos Limpios)
SELECT * 
FROM hired_employees 
LIMIT 10;

-- 4. Revisar los Registros Descartados (Auditoría)
-- Esto te mostrará exactamente qué filas fallaron y por qué
SELECT * 
FROM bad_records_log
LIMIT 10;

-- ==========================================
-- 🚀 CONSULTAS ANALÍTICAS DE PRUEBA
-- ==========================================

-- 5. ¿Cuántos empleados se contrataron por departamento?
SELECT 
    d.department, 
    COUNT(e.id) AS total_employees
FROM hired_employees e
JOIN departments d ON e.department_id = d.id
GROUP BY d.department
ORDER BY total_employees DESC;

-- 6. ¿Cuántas contrataciones hubo por año y mes?
SELECT 
    YEAR(CAST(datetime AS TIMESTAMP)) AS hire_year,
    MONTH(CAST(datetime AS TIMESTAMP)) AS hire_month,
    COUNT(id) AS total_hires
FROM hired_employees
GROUP BY hire_year, hire_month
ORDER BY hire_year, hire_month;