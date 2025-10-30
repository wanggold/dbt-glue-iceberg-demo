# dbt-glue Iceberg Demo: JSON Flattening Performance Comparison

This repository demonstrates JSON flattening performance optimization using **dbt-glue** with **Apache Iceberg** tables on AWS Glue Interactive Sessions.

## Overview

A comprehensive performance comparison of three JSON parsing approaches:
- **Legacy**: Multiple `get_json_object()` calls (50+ extractions)
- **Optimized**: Single `from_json()` with explicit schema
- **Macro-based**: Reusable schema definitions with business logic

All approaches create **Apache Iceberg tables** for ACID transactions, schema evolution, and time travel capabilities.

## Architecture

```
AWS Glue Interactive Sessions (Serverless Spark)
├── dbt-glue 1.10.13 (Trusted Adapter)
├── Apache Iceberg Tables
├── S3 Data Lake Storage
└── JSON Dataset (1,000 transaction records)
```

## Performance Results

| Approach | Execution Time | Table Format | Status |
|----------|---------------|--------------|---------|
| Legacy (get_json_object) | 24 seconds | Iceberg | ✅ |
| Optimized (from_json) | 24 seconds | Iceberg | ✅ |
| Macro-based | 28 seconds | Iceberg | ✅ |

## Repository Structure

```
├── dbt_glue_demo/                    # NYC Taxi data demo
│   ├── models/
│   │   ├── gold/                     # Business metrics
│   │   └── silver/                   # Aggregated data
│   └── profiles/                     # dbt profiles
├── dbt_glue_json/                    # JSON performance comparison
│   ├── models/
│   │   ├── legacy/                   # get_json_object approach
│   │   ├── optimized/                # from_json approaches
│   │   └── warmup/                   # Session initialization
│   ├── macros/                       # Reusable JSON schemas
│   └── run_performance_test.sh       # Automated testing script
├── aws-glue-is-dbt-template.yml      # CloudFormation template
├── generate_json_dataset.py          # Sample data generator
└── README.md
```

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.8+
- AWS Glue Interactive Sessions enabled

### Setup

1. **Deploy AWS Infrastructure**
   ```bash
   aws cloudformation create-stack \
     --stack-name dbt-glue-demo \
     --template-body file://aws-glue-is-dbt-template.yml \
     --capabilities CAPABILITY_IAM
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv dbt-glue-env
   source dbt-glue-env/bin/activate
   pip install dbt-glue==1.10.13
   ```

3. **Generate Sample Data**
   ```bash
   python generate_json_dataset.py
   ```

4. **Run Performance Test**
   ```bash
   cd dbt_glue_json
   ./run_performance_test.sh --skip-warmup
   ```

## Key Features

### JSON Parsing Approaches

#### 1. Legacy Approach (`flattened_transactions.sql`)
- Uses 50+ individual `get_json_object()` calls
- Exact field-by-field extraction
- Baseline performance reference

#### 2. Optimized Approach (`complete_optimized_transactions.sql`)
- Single `from_json()` with comprehensive schema
- Identical output columns to legacy
- Improved maintainability

#### 3. Macro-based Approach (`macro_based_flattened_transactions.sql`)
- Reusable schema definitions via dbt macros
- Business logic transformation
- Enhanced code organization

### Iceberg Table Benefits

- **ACID Transactions**: Consistent reads and writes
- **Schema Evolution**: Add/modify columns without breaking changes
- **Time Travel**: Query historical data snapshots
- **Performance**: Optimized for analytical workloads

### AWS Glue Integration

- **Serverless Compute**: No infrastructure management
- **Session Reuse**: 30-minute idle timeout for cost optimization
- **Interactive Development**: Real-time query execution
- **Automatic Scaling**: Dynamic resource allocation

## Configuration

### dbt Profile (`dbt_glue_json/profiles/profiles.yml`)
```yaml
dbt_glue_json:
  target: dev
  outputs:
    dev:
      type: glue
      role_arn: "{{ env_var('DBT_ROLE_ARN') }}"
      region: us-east-1
      glue_version: "4.0"
      workers: 2
      worker_type: G.1X
      schema: dbt_glue_json_comparison
      database: dbt_glue_json_comparison
      session_provisioning_timeout_in_minutes: 10
      location: "{{ env_var('DBT_S3_LOCATION') }}"
      glue_session_reuse: true
      idle_timeout: 30
```

### Project Configuration (`dbt_glue_json/dbt_project.yml`)
```yaml
models:
  dbt_glue_json:
    legacy:
      +materialized: table
      +table_type: iceberg
    optimized:
      +materialized: table
      +table_type: iceberg
```

## Sample JSON Schema

The test dataset includes complex nested JSON with:
- Transaction metadata (ID, status, type, platform)
- Financial data (amount, currency, pricing)
- Original transaction details (subscription info, dates)
- Extra information (country, experiments, product details)

## Performance Testing

### Automated Test Script
```bash
./run_performance_test.sh [--skip-warmup]
```

Features:
- Automatic S3 cleanup between runs
- Glue table management
- Session warm-up (optional)
- Performance timing and comparison
- Error handling and reporting

### Manual Testing
```bash
# Set environment variables
export DBT_ROLE_ARN="arn:aws:iam::ACCOUNT:role/GlueInteractiveSessionRole"
export DBT_S3_LOCATION="s3://your-bucket/path"

# Run specific model
dbt run --select complete_optimized_transactions --profiles-dir profiles
```

## AWS Glue Compatibility Notes

Functions **NOT** supported in AWS Glue:
- `try()` and `try_cast()` - Use standard `cast()` instead
- `date_parse()` - Keep timestamps as strings or use alternative parsing

## Cost Optimization

- **Session Reuse**: Enabled with 30-minute idle timeout
- **Worker Configuration**: G.1X workers for development
- **Automatic Termination**: Sessions end after idle period
- **Selective Model Runs**: Target specific models to reduce compute

## Monitoring and Debugging

### View Glue Job Logs
```bash
aws logs describe-log-groups --log-group-name-prefix "/aws-glue/interactive-sessions"
```

### Check Table Metadata
```bash
aws glue get-table --database-name dbt_glue_json_comparison --name complete_optimized_transactions
```

### S3 Data Location
```bash
aws s3 ls s3://your-bucket/dbt_glue_json_comparison/ --recursive
```

## Best Practices

1. **Schema Definition**: Use explicit schemas for `from_json()` calls
2. **Session Management**: Enable session reuse for development workflows
3. **Error Handling**: Avoid unsupported Spark functions in AWS Glue
4. **Data Partitioning**: Leverage Iceberg's partitioning for large datasets
5. **Cost Control**: Monitor session usage and configure appropriate timeouts

## Contributing

1. Fork the repository
2. Create a feature branch
3. Test changes with the performance script
4. Submit a pull request

## Resources

- [dbt-glue Documentation](https://docs.getdbt.com/reference/warehouse-setups/glue-setup)
- [AWS Glue Interactive Sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [AWS Glue Spark SQL Reference](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
