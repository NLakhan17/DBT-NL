--{{ config(materialized ='table') }}

WITH source_data AS (
    SELECT
        w.city,
        AVG(w.temperature_2m) AS avg_temperature,
        AVG(w.relative_humidity_2m) AS avg_relative_humidity,
        SUM(w.precipitation) AS total_precipitation,
        MAX(w.snowfall) AS max_snowfall,
        MAX(w.rain) AS max_rainfall,
        AVG(w.cloud_cover) AS avg_cloud_cover,
        AVG(a.aqi_city) AS avg_air_aqi
    FROM
        `dm2-hnbay.DM_2.weather-data` w
    LEFT JOIN
        `dm2-hnbay.dbt_nlakhan.aqi` a
    ON
        w.date = a.date AND w.city = a.city
    GROUP BY
        w.city
)

SELECT *
FROM source_data
ORDER BY
    city
