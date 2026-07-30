-- Best Option: Optimized for Spark/Databricks using CTE reuse.
-- Catalyst is smart enough to evaluate the 'dept_hires' CTE once. 
-- It computes the aggregations, calculates the scalar average in the subquery, 
-- and then filters the results without scanning the base tables twice.
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


-- Option B: Traditional Subquery approach (Slower).
-- This forces the engine to parse and potentially execute the expensive 
-- base operations (CAST, YEAR filtering, and GROUP BY) twice: 
-- once for the main query and once inside the HAVING clause to get the average.
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
HAVING COUNT(e.id) > (
    SELECT AVG(dept_count)
    FROM (
        SELECT COUNT(id) AS dept_count
        FROM default.hired_employees
        WHERE YEAR(CAST(datetime AS TIMESTAMP)) = 2021
        GROUP BY department_id
    )
)
ORDER BY hired DESC;