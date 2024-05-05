{{ config(materialized='table') }}

SELECT 
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(WEEK FROM date) AS week,
    EXTRACT(QUARTER FROM date) AS quarter
FROM 
    air_quality
GROUP BY 
    date