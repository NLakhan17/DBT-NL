--{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
      date,
      city,
      -- Calculate Air Quality Index (AQI) based on maximum pollutant concentrations
      GREATEST(
        (MAX(pm10) / 50), 
        (MAX(pm2_5) / 25),
        (CASE
          WHEN MAX(dust) <= 50 THEN (MAX(dust) / 50) * 50
          WHEN MAX(dust) <= 100 THEN ((MAX(dust) - 50) / 50) * (100 - 51) + 51
          WHEN MAX(dust) <= 150 THEN ((MAX(dust) - 100) / 50) * (150 - 101) + 101
          ELSE 300
        END),
        (CASE
          WHEN MAX(ozone) <= 50 THEN (MAX(ozone) / 50) * 50
          WHEN MAX(ozone) <= 100 THEN ((MAX(ozone) - 50) / 50) * (100 - 51) + 51
          WHEN MAX(ozone) <= 150 THEN ((MAX(ozone) - 100) / 50) * (150 - 101) + 101
          ELSE 300
        END),
        (CASE
          WHEN MAX(alder_pollen) <= 50 THEN (MAX(alder_pollen) / 50) * 50
          WHEN MAX(alder_pollen) <= 100 THEN ((MAX(alder_pollen) - 50) / 50) * (100 - 51) + 51
          WHEN MAX(alder_pollen) <= 150 THEN ((MAX(alder_pollen) - 100) / 50) * (150 - 101) + 101
          ELSE 300
        END),
        (CASE
          WHEN MAX(grass_pollen) <= 50 THEN (MAX(grass_pollen) / 50) * 50
          WHEN MAX(grass_pollen) <= 100 THEN ((MAX(grass_pollen) - 50) / 50) * (100 - 51) + 51
          WHEN MAX(grass_pollen) <= 150 THEN ((MAX(grass_pollen) - 100) / 50) * (150 - 101) + 101
          ELSE 300
        END),
        (CASE
          WHEN MAX(ragweed_pollen) <= 50 THEN (MAX(ragweed_pollen) / 50) * 50
          WHEN MAX(ragweed_pollen) <= 100 THEN ((MAX(ragweed_pollen) - 50) / 50) * (100 - 51) + 51
          WHEN MAX(ragweed_pollen) <= 150 THEN ((MAX(ragweed_pollen) - 100) / 50) * (150 - 101) + 101
          ELSE 300
        END),
        (CASE
          WHEN MAX(ammonia) <= 50 THEN (MAX(ammonia) / 50) * 50
          WHEN MAX(ammonia) <= 100 THEN ((MAX(ammonia) - 50) / 50) * (100 - 51) + 51
          WHEN MAX(ammonia) <= 150 THEN ((MAX(ammonia) - 100) / 50) * (150 - 101) + 101
          ELSE 300
        END)
      ) AS aqi_city,
      -- Determine AQI threshold category based on AQI value
      CASE
        WHEN GREATEST(
          (MAX(pm10) / 50), 
          (MAX(pm2_5) / 25),
          (CASE
            WHEN MAX(dust) <= 50 THEN (MAX(dust) / 50) * 50
            WHEN MAX(dust) <= 100 THEN ((MAX(dust) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(dust) <= 150 THEN ((MAX(dust) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ozone) <= 50 THEN (MAX(ozone) / 50) * 50
            WHEN MAX(ozone) <= 100 THEN ((MAX(ozone) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ozone) <= 150 THEN ((MAX(ozone) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(alder_pollen) <= 50 THEN (MAX(alder_pollen) / 50) * 50
            WHEN MAX(alder_pollen) <= 100 THEN ((MAX(alder_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(alder_pollen) <= 150 THEN ((MAX(alder_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(grass_pollen) <= 50 THEN (MAX(grass_pollen) / 50) * 50
            WHEN MAX(grass_pollen) <= 100 THEN ((MAX(grass_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(grass_pollen) <= 150 THEN ((MAX(grass_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ragweed_pollen) <= 50 THEN (MAX(ragweed_pollen) / 50) * 50
            WHEN MAX(ragweed_pollen) <= 100 THEN ((MAX(ragweed_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ragweed_pollen) <= 150 THEN ((MAX(ragweed_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ammonia) <= 50 THEN (MAX(ammonia) / 50) * 50
            WHEN MAX(ammonia) <= 100 THEN ((MAX(ammonia) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ammonia) <= 150 THEN ((MAX(ammonia) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END)
        ) <= 50 THEN 'Good'
        WHEN GREATEST(
          (MAX(pm10) / 50), 
          (MAX(pm2_5) / 25),
          (CASE
            WHEN MAX(dust) <= 50 THEN (MAX(dust) / 50) * 50
            WHEN MAX(dust) <= 100 THEN ((MAX(dust) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(dust) <= 150 THEN ((MAX(dust) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ozone) <= 50 THEN (MAX(ozone) / 50) * 50
            WHEN MAX(ozone) <= 100 THEN ((MAX(ozone) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ozone) <= 150 THEN ((MAX(ozone) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(alder_pollen) <= 50 THEN (MAX(alder_pollen) / 50) * 50
            WHEN MAX(alder_pollen) <= 100 THEN ((MAX(alder_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(alder_pollen) <= 150 THEN ((MAX(alder_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(grass_pollen) <= 50 THEN (MAX(grass_pollen) / 50) * 50
            WHEN MAX(grass_pollen) <= 100 THEN ((MAX(grass_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(grass_pollen) <= 150 THEN ((MAX(grass_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ragweed_pollen) <= 50 THEN (MAX(ragweed_pollen) / 50) * 50
            WHEN MAX(ragweed_pollen) <= 100 THEN ((MAX(ragweed_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ragweed_pollen) <= 150 THEN ((MAX(ragweed_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ammonia) <= 50 THEN (MAX(ammonia) / 50) * 50
            WHEN MAX(ammonia) <= 100 THEN ((MAX(ammonia) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ammonia) <= 150 THEN ((MAX(ammonia) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END)
        ) <= 50 THEN 'Good'
        WHEN GREATEST(
          (MAX(pm10) / 50), 
          (MAX(pm2_5) / 25),
          (CASE
            WHEN MAX(dust) <= 50 THEN (MAX(dust) / 50) * 50
            WHEN MAX(dust) <= 100 THEN ((MAX(dust) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(dust) <= 150 THEN ((MAX(dust) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ozone) <= 50 THEN (MAX(ozone) / 50) * 50
            WHEN MAX(ozone) <= 100 THEN ((MAX(ozone) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ozone) <= 150 THEN ((MAX(ozone) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(alder_pollen) <= 50 THEN (MAX(alder_pollen) / 50) * 50
            WHEN MAX(alder_pollen) <= 100 THEN ((MAX(alder_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(alder_pollen) <= 150 THEN ((MAX(alder_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(grass_pollen) <= 50 THEN (MAX(grass_pollen) / 50) * 50
            WHEN MAX(grass_pollen) <= 100 THEN ((MAX(grass_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(grass_pollen) <= 150 THEN ((MAX(grass_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ragweed_pollen) <= 50 THEN (MAX(ragweed_pollen) / 50) * 50
            WHEN MAX(ragweed_pollen) <= 100 THEN ((MAX(ragweed_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ragweed_pollen) <= 150 THEN ((MAX(ragweed_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ammonia) <= 50 THEN (MAX(ammonia) / 50) * 50
            WHEN MAX(ammonia) <= 100 THEN ((MAX(ammonia) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ammonia) <= 150 THEN ((MAX(ammonia) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END)
        ) <= 100 THEN 'Moderate'
        WHEN GREATEST(
          (MAX(pm10) / 50), 
          (MAX(pm2_5) / 25),
          (CASE
            WHEN MAX(dust) <= 50 THEN (MAX(dust) / 50) * 50
            WHEN MAX(dust) <= 100 THEN ((MAX(dust) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(dust) <= 150 THEN ((MAX(dust) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ozone) <= 50 THEN (MAX(ozone) / 50) * 50
            WHEN MAX(ozone) <= 100 THEN ((MAX(ozone) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ozone) <= 150 THEN ((MAX(ozone) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(alder_pollen) <= 50 THEN (MAX(alder_pollen) / 50) * 50
            WHEN MAX(alder_pollen) <= 100 THEN ((MAX(alder_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(alder_pollen) <= 150 THEN ((MAX(alder_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(grass_pollen) <= 50 THEN (MAX(grass_pollen) / 50) * 50
            WHEN MAX(grass_pollen) <= 100 THEN ((MAX(grass_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(grass_pollen) <= 150 THEN ((MAX(grass_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ragweed_pollen) <= 50 THEN (MAX(ragweed_pollen) / 50) * 50
            WHEN MAX(ragweed_pollen) <= 100 THEN ((MAX(ragweed_pollen) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ragweed_pollen) <= 150 THEN ((MAX(ragweed_pollen) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END),
          (CASE
            WHEN MAX(ammonia) <= 50 THEN (MAX(ammonia) / 50) * 50
            WHEN MAX(ammonia) <= 100 THEN ((MAX(ammonia) - 50) / 50) * (100 - 51) + 51
            WHEN MAX(ammonia) <= 150 THEN ((MAX(ammonia) - 100) / 50) * (150 - 101) + 101
            ELSE 300
          END)
        ) <= 150 THEN 'Unhealthy for Sensitive Groups'
        ELSE 'Unhealthy'
      END AS aqi_threshold
    FROM
      `dm2-hnbay.dbt_nlakhan.air_quality`
    GROUP BY
      date, city
    ORDER BY
      date
  )

SELECT *
FROM source_data
