# File: models/core/covid_metrics.sql
{{ config(
    materialized='incremental',
    unique_key='country',
    incremental_strategy='merge',
    merge_update_columns=['cases', 'deaths', 'recovered', 'active', 'population', 'case_fatality_rate', 'metrics_timestamp'],
    tags=['covid', 'core', 'metrics']
) }}

WITH source_data AS (
    SELECT
        country,
        CAST(cases AS NUMBER) AS cases,
        CAST(deaths AS NUMBER) AS deaths,
        CAST(recovered AS NUMBER) AS recovered,
        CAST(active AS NUMBER) AS active,
        CAST(population AS NUMBER) AS population,
        load_ts AS source_batch_id,
        transform_id AS data_lineage_id,
        transform_timestamp AS metrics_timestamp,
        -- Calculate metrics
        CASE
            WHEN CAST(cases AS NUMBER) > 0 
            THEN ROUND((CAST(deaths AS NUMBER) / CAST(cases AS NUMBER)) * 100, 2)
            ELSE NULL
        END AS case_fatality_rate,
        continent
    FROM {{ source('transform_layer', 'TRANSFORM_COVID') }}
    WHERE country IS NOT NULL
    {% if is_incremental() %}
        AND transform_timestamp > (SELECT MAX(metrics_timestamp) FROM {{ this }})
    {% endif %}
),

-- Add data quality validations
validated_data AS (
    SELECT
        *,
        -- Flag records where deaths > cases (shouldn't be possible)
        deaths > cases AS invalid_deaths_cases,
        -- Flag records where recovered > cases (shouldn't be possible)
        recovered > cases AS invalid_recovered_cases,
        -- Flag records with missing critical data
        population IS NULL OR population <= 0 AS invalid_population
    FROM source_data
),

-- Apply corrections to maintain data integrity
corrected_data AS (
    SELECT
        country,
        cases,
        -- Cap deaths at number of cases if exceeding
        LEAST(deaths, cases) AS deaths,
        -- Cap recovered at number of cases if exceeding
        LEAST(recovered, cases) AS recovered,
        -- Ensure active cases is consistent: active = cases - deaths - recovered
        cases - LEAST(deaths, cases) - LEAST(recovered, cases) AS active,
        population,
        source_batch_id,
        data_lineage_id,
        metrics_timestamp,
        -- Recalculate metrics after corrections
        CASE
            WHEN cases > 0 
            THEN ROUND((LEAST(deaths, cases) / cases) * 100, 2)
            ELSE NULL
        END AS case_fatality_rate,
        continent,
        -- Track corrections
        CASE
            WHEN invalid_deaths_cases OR invalid_recovered_cases OR invalid_population
            THEN TRUE
            ELSE FALSE
        END AS was_corrected,
        -- Generate SHA-256 hash for data lineage
        SHA2_HEX(CONCAT(
            country, '|', 
            CAST(cases AS VARCHAR), '|',
            CAST(LEAST(deaths, cases) AS VARCHAR), '|',
            metrics_timestamp
        )) AS lineage_hash
    FROM validated_data
)

SELECT
    country,
    cases,
    deaths,
    recovered,
    active,
    population,
    case_fatality_rate,
    -- Calculate additional metrics
    CASE 
        WHEN population > 0 
        THEN (cases / population) * 1000000 
        ELSE NULL 
    END AS cases_per_million,
    continent,
    source_batch_id,
    data_lineage_id,
    metrics_timestamp,
    lineage_hash,
    was_corrected,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM corrected_data