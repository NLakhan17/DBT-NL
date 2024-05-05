{{ config(materialized='table') }}

SELECT 
    dc.city_key,
    dd.date_key,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    rain,
    snowfall,
    surface_pressure,
    cloud_cover,
    wind_speed_10m,
    soil_temperature_0_to_7cm,
    soil_moisture_0_to_7cm,
    is_day,
    sunshine_duration
FROM 
    weather_data wd
JOIN 
    dim_city dc ON wd.city = dc.city
JOIN 
    dim_date dd ON wd.date = dd.date