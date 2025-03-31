# File: models/core/covid_regional_metrics.sql
{{ config(
    materialized='table',
    tags=['covid', 'core', 'aggregation']
) }}

WITH country_metrics AS (
    SELECT
        continent,
        country,
        cases,
        deaths,
        recovered,
        active,
        population,
        case_fatality_rate,
        metrics_timestamp,
        lineage_hash
    FROM {{ ref('covid_metrics') }}
    WHERE continent IS NOT NULL
)

SELECT
    continent,
    COUNT(DISTINCT country) AS country_count,
    SUM(cases) AS total_cases,
    SUM(deaths) AS total_deaths,
    SUM(recovered) AS total_recovered,
    SUM(active) AS total_active,
    SUM(population) AS total_population,
    
    -- Calculate regional metrics
    ROUND(AVG(case_fatality_rate), 2) AS avg_case_fatality_rate,
    ROUND(SUM(deaths) / NULLIF(SUM(cases), 0) * 100, 2) AS region_case_fatality_rate,
    ROUND(SUM(cases) / NULLIF(SUM(population), 0) * 1000000, 2) AS cases_per_million,
    
    -- Calculate intra-region variance metrics
    ROUND(STDDEV(case_fatality_rate), 2) AS case_fatality_rate_stddev,
    
    -- Store aggregation metadata for compliance tracking
    MAX(metrics_timestamp) AS latest_data_timestamp,
    MIN(metrics_timestamp) AS earliest_data_timestamp,
    
    -- Generate new lineage hash based on input hashes
    SHA2_HEX(LISTAGG(lineage_hash, '|') WITHIN GROUP (ORDER BY country)) AS lineage_hash,
    
    CURRENT_TIMESTAMP() AS aggregated_at
FROM country_metrics
GROUP BY continent