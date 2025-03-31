# COVID-19 Data Analytics Pipeline

This dbt project implements a HIPAA-compliant analytics pipeline for COVID-19 data sourced from the Disease.sh API.

## Models

### Core Layer
- `covid_metrics`: Country-level COVID-19 metrics with data quality controls
- `covid_regional_metrics`: Aggregated metrics by continent/region

### Mart Layer
- `covid_trends`: Time-series analysis of COVID metrics with rolling averages

## HIPAA Compliance

This pipeline maintains HIPAA compliance through:

1. **Complete Data Lineage**: Every metric maintains tracking back to source data
2. **Comprehensive Audit Logging**: All transformations are tracked and logged
3. **Data Quality Validation**: Automated tests ensure data integrity
4. **Historical Snapshots**: Complete record of all data changes over time

## Integration with Snowflake

This dbt project is designed to work with the dynamic schema detection from Snowflake, automatically adapting to schema changes while maintaining compliance controls.

## Usage

```bash
# Run full pipeline
dbt run

# Run data quality tests
dbt test

# Generate documentation
dbt docs generate
```

For more information, please see the [official documentation](https://docs.getdbt.com/).

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
