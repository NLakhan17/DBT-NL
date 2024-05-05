
--{{ config(materialized ='table') }}

WITH source_data AS (
  SELECT DISTINCT
    date,
    city,
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
    `dm2-hnbay.DM_2.air_quality`
)

-- Select all columns from the source_data CTE
SELECT *
FROM source_data