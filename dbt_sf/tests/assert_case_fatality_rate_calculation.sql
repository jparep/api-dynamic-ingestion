# File: tests/assert_case_fatality_rate_calculation.sql
-- HIPAA-compliant test to ensure case fatality rates are correctly calculated
WITH test_data AS (
    SELECT
        country,
        cases,
        deaths,
        case_fatality_rate,
        -- Calculate expected case fatality rate
        CASE 
            WHEN CAST(cases AS NUMBER) > 0
            THEN ROUND((CAST(deaths AS NUMBER) / CAST(cases AS NUMBER)) * 100, 2)
            ELSE NULL
        END AS expected_case_fatality_rate
    FROM {{ ref('covid_metrics') }}
    WHERE cases > 0
)

SELECT
    country,
    cases,
    deaths,
    case_fatality_rate,
    expected_case_fatality_rate,
    ABS(case_fatality_rate - expected_case_fatality_rate) AS difference
FROM test_data
WHERE ABS(case_fatality_rate - expected_case_fatality_rate) > 0.01