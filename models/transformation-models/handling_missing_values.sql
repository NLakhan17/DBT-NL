-- dbt model to handle missing values and outliers

-- Define a macro to replace missing values with a default value
{% macro replace_missing_values(column_name, default_value) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN {{ default_value }}
        ELSE {{ column_name }}
    END
{% endmacro %}

-- Define a macro to detect and handle outliers based on a specified threshold
{% macro handle_outliers(column_name, threshold) %}
    CASE
        WHEN {{ column_name }} > {{ threshold }} THEN {{ threshold }}
        ELSE {{ column_name }}
    END
{% endmacro %}

-- Define a dbt model that uses the macros to handle missing values and outliers
{% set threshold_value = 100.0 %}

SELECT
    replace_missing_values(air_quality.pm10, 0) AS pm10,
    replace_missing_values(air_quality.pm2_5, 0) AS pm2_5,
    replace_missing_values(air_quality.carbon_monoxide, 0) AS carbon_monoxide,
    replace_missing_values(air_quality.nitrogen_dioxide, 0) AS nitrogen_dioxide,
    replace_missing_values(air_quality.ozone, 0) AS ozone,
    replace_missing_values(air_quality.dust, 0) AS dust,
    replace_missing_values(air_quality.uv_index, 0) AS uv_index,
    replace_missing_values(air_quality.ammonia, 0) AS ammonia,
    replace_missing_values(air_quality.alder_pollen, 0) AS alder_pollen,
    replace_missing_values(air_quality.grass_pollen, 0) AS grass_pollen,
    replace_missing_values(air_quality.ragweed_pollen, 0) AS ragweed_pollen,
    handle_outliers(weather_data.temperature_2m, {{ threshold_value }}) AS temperature_2m,
    handle_outliers(weather_data.relative_humidity_2m, {{ threshold_value }}) AS relative_humidity_2m,
    handle_outliers(weather_data.precipitation, {{ threshold_value }}) AS precipitation,
    handle_outliers(weather_data.rain, {{ threshold_value }}) AS rain,
    handle_outliers(weather_data.snowfall, {{ threshold_value }}) AS snowfall,
    handle_outliers(weather_data.surface_pressure, {{ threshold_value }}) AS surface_pressure,
    handle_outliers(weather_data.cloud_cover, {{ threshold_value }}) AS cloud_cover,
    handle_outliers(weather_data.wind_speed_10m, {{ threshold_value }}) AS wind_speed_10m,
    handle_outliers(weather_data.soil_temperature_0_to_7cm, {{ threshold_value }}) AS soil_temperature_0_to_7cm,
    handle_outliers(weather_data.soil_moisture_0_to_7cm, {{ threshold_value }}) AS soil_moisture_0_to_7cm,
    weather_data.is_day,
    handle_outliers(weather_data.sunshine_duration, {{ threshold_value }}) AS sunshine_duration,
    weather_data.City
FROM
    {{ ref('air_quality') }} AS air_quality
JOIN
    {{ ref('weather_data') }} AS weather_data
ON
    air_quality.date = weather_data.date AND air_quality.city = weather_data.city