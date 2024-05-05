{{ config(materialized='table') }}

SELECT 
    city,
    LOWER(city) AS city_lower
FROM 
    air_quality
GROUP BY 
    city