-- models/weather_data_with_wind_direction.sql

{{ config(materialized='table') }}

WITH weather_data_with_u_component AS (
  SELECT
    *,
    wind_speed_10m * COS(RADIANS(wind_direction_10m)) AS u_component_10m
  FROM
    {{ ref('weather_data') }}
)

SELECT
  *,
  DEGREES(ATAN2(u_component_10m, wind_speed_10m * SIN(RADIANS(90 - wind_direction_10m)))) AS wind_direction_from_speed
FROM
  weather_data_with_u_component;