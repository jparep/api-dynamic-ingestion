# File: models/mart/covid_trends.sql
{{ config(
    materialized='incremental',
    unique_key=['country', 'metric_date'],
    partition_by={
        'field': 'metric_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['country'],
    tags=['covid', 'mart', 'trends']
) }}

WITH metrics_history AS (
    SELECT
        country,
        metrics_timestamp::DATE AS metric_date,
        cases,
        deaths,
        recovered,
        active,
        case_fatality_rate,
        lineage_hash,
        -- Add row number to get latest record per day
        ROW_NUMBER() OVER (
            PARTITION BY country, metrics_timestamp::DATE
            ORDER BY metrics_timestamp DESC
        ) AS rn
    FROM {{ ref('covid_metrics') }}
    {% if is_incremental() %}
        WHERE metrics_timestamp::DATE >= (SELECT MAX(metric_date) FROM {{ this }})
    {% endif %}
),

daily_metrics AS (
    SELECT
        country,
        metric_date,
        cases,
        deaths,
        recovered,
        active,
        case_fatality_rate,
        lineage_hash
    FROM metrics_history
    WHERE rn = 1 -- Get only latest record for each day
),

-- Calculate rolling averages and changes
trend_metrics AS (
    SELECT
        country,
        metric_date,
        cases,
        deaths,
        recovered,
        active,
        case_fatality_rate,
        
        -- Calculate day-over-day changes
        cases - LAG(cases, 1) OVER (PARTITION BY country ORDER BY metric_date) AS new_cases,
        deaths - LAG(deaths, 1) OVER (PARTITION BY country ORDER BY metric_date) AS new_deaths,
        
        -- Calculate 7-day rolling averages
        AVG(cases - LAG(cases, 1) OVER (PARTITION BY country ORDER BY metric_date)) 
            OVER (PARTITION BY country ORDER BY metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_new_cases_7d,
        AVG(deaths - LAG(deaths, 1) OVER (PARTITION BY country ORDER BY metric_date)) 
            OVER (PARTITION BY country ORDER BY metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_new_deaths_7d,
            
        -- Calculate growth rates
        CASE 
            WHEN LAG(cases, 7) OVER (PARTITION BY country ORDER BY metric_date) > 0
            THEN ROUND((cases - LAG(cases, 7) OVER (PARTITION BY country ORDER BY metric_date)) / 
                       LAG(cases, 7) OVER (PARTITION BY country ORDER BY metric_date) * 100, 2)
            ELSE NULL
        END AS weekly_growth_rate,
        
        lineage_hash
    FROM daily_metrics
)

SELECT
    country,
    metric_date,
    cases,
    deaths,
    recovered,
    active,
    case_fatality_rate,
    new_cases,
    new_deaths,
    avg_new_cases_7d,
    avg_new_deaths_7d,
    weekly_growth_rate,
    lineage_hash,
    CURRENT_TIMESTAMP() AS processed_at
FROM trend_metrics
WHERE 
    -- Exclude rows with negative daily changes (likely data corrections)
    (new_cases >= 0 OR new_cases IS NULL) AND
    (new_deaths >= 0 OR new_deaths IS NULL)
