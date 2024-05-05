--{{ config(materialized ='table') }}


WITH weather_air_data AS (
    SELECT
      date,
      EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
      EXTRACT(QUARTER FROM date) AS quarter,
      FORMAT_DATE('%B', DATE_TRUNC(date, MONTH)) AS month_name
    FROM
      `dm2-hnbay.dbt_nlakhan.weather_t`
  )

  SELECT *
  FROM weather_air_data
  ORDER BY date