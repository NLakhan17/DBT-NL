

-- models/trans-models/air_quality_unique_dates.sql

-- Define a new model to create a table with unique dates

-- models/trans-models/weather_t.sql

-- Define a new model to create a table with unique dates


--{{ config(materialized ='table') }}

WITH source_data AS (
  SELECT DISTINCT
    date,
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
    sunshine_duration,
    City
  FROM
    `dm2-hnbay.DM_2.weather-data`
)

-- Select all columns from the source_data CTE
SELECT *
FROM source_data