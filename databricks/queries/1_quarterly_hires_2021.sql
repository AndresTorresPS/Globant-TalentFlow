-- Best Option: Optimized for Spark/Databricks.
-- Even though PIVOT implies an additional aggregation step, COUNT(*) is highly vectorized.
-- It is faster because QUARTER() is evaluated only once per row in the CTE, 
-- and the strict SELECT limits the data moved into the PIVOT phase.
-- Best Option: Optimized for Spark/Databricks.
-- Even though PIVOT implies an additional aggregation step, COUNT(*) is highly vectorized.
-- It is faster because QUARTER() is evaluated only once per row in the CTE, 
-- and the strict SELECT limits the data moved into the PIVOT phase.
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

-- Option B: Conditional Aggregation (Traditional approach).
-- Slower in this engine because the QUARTER() function and the CASE condition 
-- must be evaluated 4 separate times per row. Additionally, the SELECT * 
-- in the subquery introduces unnecessary memory overhead before grouping.
SELECT
    d.department,
    j.job,
    SUM(CASE WHEN QUARTER(ts) = 1 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN QUARTER(ts) = 2 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN QUARTER(ts) = 3 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN QUARTER(ts) = 4 THEN 1 ELSE 0 END) AS Q4
FROM (
    SELECT *,
           CAST(datetime AS TIMESTAMP) AS ts
    FROM default.hired_employees
) e
JOIN default.departments d
    ON e.department_id = d.id
JOIN default.jobs j
    ON e.job_id = j.id
WHERE YEAR(ts) = 2021
GROUP BY
    d.department,
    j.job
ORDER BY
    department,
    job;


