{{ config(materialized='table') }}

SELECT 
    dc.city_key,
    dd.date_key,
    pm10,
    pm2_5,
    carbon_monoxide,
    nitrogen_dioxide,
    ozone,
    dust,
    uv_index,
    ammonia,
    alder_pollen,
    grass_pollen,
    ragweed_pollen,
    european_aqi
FROM 
    air_quality aq
JOIN 
    dim_city dc ON aq.city = dc.city
JOIN 
    dim_date dd ON aq.date = dd.date