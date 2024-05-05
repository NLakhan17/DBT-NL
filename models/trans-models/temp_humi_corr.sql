--{{ config(materialized='table') }}

WITH weather_factors AS (
  SELECT
    date,
    temperature_2m,
    relative_humidity_2m,
    cloud_cover,
    wind_speed_10m,
    sunshine_duration
  FROM
    `dm2-hnbay.dbt_nlakhan.weather_t`
)

-- Calculate correlations between temperature, humidity, and weather factors
SELECT
  date,
  temperature_2m,
  relative_humidity_2m,
  cloud_cover,
  wind_speed_10m,
  sunshine_duration,
  CORR(temperature_2m, cloud_cover) AS correlation_temp_cloud_cover,
  CORR(temperature_2m, wind_speed_10m) AS correlation_temp_wind_speed,
  CORR(temperature_2m, sunshine_duration) AS correlation_temp_sunshine,
  CORR(relative_humidity_2m, cloud_cover) AS correlation_humidity_cloud_cover,
  CORR(relative_humidity_2m, wind_speed_10m) AS correlation_humidity_wind_speed,
  CORR(relative_humidity_2m, sunshine_duration) AS correlation_humidity_sunshine
FROM
  weather_factors
GROUP BY
  date, temperature_2m, relative_humidity_2m, cloud_cover, wind_speed_10m, sunshine_duration






