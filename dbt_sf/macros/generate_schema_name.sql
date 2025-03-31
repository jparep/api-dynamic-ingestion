# File: macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

# File: macros/hipaa_lineage.sql
{% macro generate_lineage_hash(source_table, source_column) %}
    SHA2_HEX(CONCAT(
        '{{ source_table }}', '.',
        '{{ source_column }}', '.',
        CURRENT_TIMESTAMP()
    ))
{% endmacro %}

# File: macros/data_quality.sql
{% macro validate_cases_deaths(model_name) %}
    {% set query %}
        SELECT
            COUNT(*) AS total_records,
            SUM(CASE WHEN deaths > cases THEN 1 ELSE 0 END) AS invalid_records
        FROM {{ ref(model_name) }}
    {% endset %}
    
    {% set results = run_query(query) %}
    {% set total_records = results.columns[0].values()[0] %}
    {% set invalid_records = results.columns[1].values()[0] %}
    
    {% if invalid_records > 0 %}
        {{ log("⚠️ WARNING: " ~ invalid_records ~ " records found where deaths > cases in " ~ model_name, True) }}
        {% 