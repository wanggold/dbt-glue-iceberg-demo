# Build and manage your modern data stack using dbt and AWS Glue through dbt-glue, the new "trusted" dbt adapter

**Authors:** Noritaka Sekiyama, Akira Ajisaka, Jason Ganz, Kinshuk Pahare, and Benjamin Menuet  
**Published:** November 29, 2023  
**Categories:** Analytics, AWS Glue, Intermediate (200)

## Overview

dbt is an open source, SQL-first templating engine that allows you to write repeatable and extensible data transforms in Python and SQL. dbt focuses on the transform layer of extract, load, transform (ELT) or extract, transform, load (ETL) processes across data warehouses and databases through specific engine adapters to achieve extract and load functionality. It enables data engineers, data scientists, and analytics engineers to define the business logic with SQL select statements and eliminates the need to write boilerplate data manipulation language (DML) and data definition language (DDL) expressions. dbt lets data engineers quickly and collaboratively deploy analytics code following software engineering best practices like modularity, portability, continuous integration and continuous delivery (CI/CD), and documentation.

dbt is predominantly used by data warehouses (such as Amazon Redshift) customers who are looking to keep their data transform logic separate from storage and engine. We have seen a strong customer demand to expand its scope to cloud-based data lakes because data lakes are increasingly the enterprise solution for large-scale data initiatives due to their power and capabilities.

In 2022, AWS published a dbt adapter called dbt-glue—the open source, battle-tested dbt AWS Glue adapter that allows data engineers to use dbt for cloud-based data lakes along with data warehouses and databases, paying for just the compute they need. The dbt-glue adapter democratized access for dbt users to data lakes, and enabled many users to effortlessly run their transformation workloads on the cloud with the serverless data integration capability of AWS Glue. From the launch of the adapter, AWS has continued investing into dbt-glue to cover more requirements.

Today, we are pleased to announce that the dbt-glue adapter is now a **trusted adapter** based on our strategic collaboration with dbt Labs. Trusted adapters are adapters not maintained by dbt Labs, but adaptors that that dbt Lab is comfortable recommending to users for use in production.

## Key Capabilities of dbt-glue

The key capabilities of the dbt-glue adapter are as follows:

- Runs SQL as Spark SQL on AWS Glue interactive sessions
- Manages table definitions on the Amazon SageMaker Lakehouse Catalog with storage on Amazon S3
- Supports open table formats such as Apache Hudi, Delta Lake, and Apache Iceberg
- Supports AWS Lake Formation permissions for fine-grained access control

In addition to those capabilities, the dbt-glue adapter is designed to optimize resource utilization with several techniques on top of AWS Glue interactive sessions.

This post demonstrates how the dbt-glue adapter helps your workload, and how you can build a modern data stack using dbt and AWS Glue using the dbt-glue adapter.

## Common Use Cases

### Use Case 1: Central Analytics Team

One common use case for using dbt-glue is if a central analytics team at a large corporation is responsible for monitoring operational efficiency. They ingest application logs into raw Parquet tables in an Amazon Simple Storage Service (Amazon S3) data lake. Additionally, they extract organized data from operational systems capturing the company's organizational structure and costs of diverse operational components that they stored in the raw zone using Iceberg tables to maintain the original schema, facilitating easy access to the data. The team uses dbt-glue to build a transformed gold model optimized for business intelligence (BI). The gold model joins the technical logs with billing data and organizes the metrics per business unit. The gold model uses Iceberg's ability to support data warehouse-style modeling needed for performant BI analytics in a data lake. The combination of Iceberg and dbt-glue allows the team to efficiently build a data model that's ready to be consumed.

### Use Case 2: GDPR Compliance

Another common use case is when an analytics team in a company that has an S3 data lake creates a new data product in order to enrich its existing data from its data lake with medical data. Let's say that this company is located in Europe and the data product must comply with the GDPR. For this, the company uses Iceberg to meet needs such as the right to be forgotten and the deletion of data. The company uses dbt to model its data product on its existing data lake due to its compatibility with AWS Glue and Iceberg and the simplicity that the dbt-glue adapter brings to the use of this storage format.

## How dbt and dbt-glue Work

### Key dbt Features

The following are key dbt features:

- **Project** – A dbt project enforces a top-level structure on the staging, models, permissions, and adapters. A project can be checked into a GitHub repo for version control.
- **SQL** – dbt relies on SQL select statements for defining data transformation logic. Instead of raw SQL, dbt offers templatized SQL (using Jinja) that allows code modularity. Instead of having to copy/paste SQL in multiple places, data engineers can define modular transforms and call those from other places within the project. Having a modular pipeline helps data engineers collaborate on the same project.
- **Models** – dbt models are primarily written as a SELECT statement and saved as a .sql file. Data engineers define dbt models for their data representations.
- **Materializations** – Materializations are strategies for persisting dbt models in a warehouse. There are five types of materializations built into dbt: table, view, incremental, ephemeral, and materialized view.
- **Data lineage** – dbt tracks data lineage, allowing you to understand the origin of data and how it flows through different transformations. dbt also supports impact analysis, which helps identify the downstream effects of changes.

### High-level Data Flow

The high-level data flow is as follows:

1. Data engineers ingest data from data sources to raw tables and define table definitions for the raw tables.
2. Data engineers write dbt models with templatized SQL.
3. The dbt adapter converts dbt models to SQL statements compatible in a data warehouse.
4. The data warehouse runs the SQL statements to create intermediate tables or final tables, views, or materialized views.

### dbt-glue Workflow

dbt-glue works with the following steps:

1. The dbt-glue adapter converts dbt models to SQL statements compatible in Spark SQL.
2. AWS Glue interactive sessions run the SQL statements to create intermediate tables or final tables, views, or materialized views.
3. dbt-glue supports csv, parquet, hudi, delta, and iceberg as fileformat.
4. On the dbt-glue adapter, table or incremental are commonly used for materializations at the destination. There are three strategies for incremental materialization. The merge strategy requires hudi, delta, or iceberg. With the other two strategies, append and insert_overwrite, you can use csv, parquet, hudi, delta, or iceberg.

## Example Use Case

In this post, we use the data from the New York City Taxi Records dataset. This dataset is available in the Registry of Open Data on AWS (RODA), which is a repository containing public datasets from AWS resources. The raw Parquet table records in this dataset stores trip records.

The objective is to create the following three tables, which contain metrics based on the raw table:

- **silver_avg_metrics** – Basic metrics based on NYC Taxi Open Data for the year 2016
- **gold_passengers_metrics** – Metrics per passenger based on the silver metrics table
- **gold_cost_metrics** – Metrics per cost based on the silver metrics table

The final goal is to create two well-designed gold tables that store already aggregated results in Iceberg format for ad hoc queries through Amazon Athena.

## Prerequisites

The instruction requires following prerequisites:

- An AWS Identity and Access Management (IAM) role with all the mandatory permissions to run an AWS Glue interactive session and the dbt-glue adapter

---

*Note: This is a partial extraction of the blog post. The full article contains additional sections including detailed implementation steps, code examples, and configuration details.*

**Source:** [AWS Big Data Blog](https://aws.amazon.com/blogs/big-data/build-and-manage-your-modern-data-stack-using-dbt-and-aws-glue-through-dbt-glue-the-new-trusted-dbt-adapter/)
