-- models/weather_data_with_wind_speed.sql

{{ config(materialized='table') }}

WITH weather_data_with_uv AS (
  SELECT
    *,
    u_component_10m,
    v_component_10m
  FROM
    {{ ref('weather_data') }}
)

SELECT
  *,
  sqrt(pow(u_component_10m, 2) + pow(v_component_10m, 2)) AS wind_speed
FROM
  weather_data_with_uv