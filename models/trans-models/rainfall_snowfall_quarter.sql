--{{ config(materialized='table') }}

WITH weather_analysis AS (
  SELECT
    w.date AS weather_date,
    w.rain,
    w.snowfall,
    d.quarter,
    d.month_name
  FROM
    {{ref("weather_t")}} w
  INNER JOIN
    {{ref("date_details")}} d
  ON
    w.date = d.date
)

SELECT *
FROM weather_analysis
ORDER BY weather_date
