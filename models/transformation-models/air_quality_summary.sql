{{ config(materialized='table') }}

SELECT 
    city_key,
    date_key,
    AVG(pm10) AS avg_pm10,
    AVG(pm2_5) AS avg_pm2_5,
   ...
FROM 
    fact_air_quality
GROUP BY 
    city_key,
    date_key