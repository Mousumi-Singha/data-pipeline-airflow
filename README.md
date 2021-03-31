# Sparkify Airflow Data Pipeline

## Overview

This project builds a data pipeline for Sparkify using Apache Airflow that automates and monitors the running of an ETL pipeline.

The ETL loads song and log data in JSON format from S3 and processes the data into analytics tables in a star schema on Reshift. A star schema has been used to allow the Sparkify team to readily run queries to analyze user activity on their app. Airflow schedules this ETL hourly and monitors its success by running a data quality check.

## Structure

* `udac_example_dag.py` contains the tasks and dependencies of the parent DAG. 
* `load_dimension_table_subdag.py` contains the subdag for loading the dimensional tables.
* `create_tables.sql` contains the SQL queries used to create all the required tables in Redshift. It should be placed in the `dags` directory of your Airflow installation.
* `sql_queries.py` contains the SQL queries for creating and loading the tables used in the ETL process. It should be placed in the `plugins/helpers` directory of your Airflow installation.
* `images/` contains the images of the DAG and subDAG in Apache Airflow.
* `README.md` this very file that discusses the specifics and process of the project.

The following operators should be placed in the `plugins/operators` directory of
your Airflow installation:
* `stage_redshift.py` contains `StageToRedshiftOperator`, which copies JSON data from S3 to staging tables in the Redshift data warehouse.
* `load_dimension.py` contains `LoadDimensionOperator`, which loads a dimension table from data in the staging table(s).
* `load_fact.py` contains `LoadFactOperator`, which loads a fact table from data in the staging table(s).
* `data_quality.py` contains `DataQualityOperator`, which runs a data quality check by passing an SQL query and expected result as arguments, failing if the results don't match.

## Configuration

* Make sure to add the following Airflow connections:
    * AWS credentials
    * Connection to Redshift database
    
## How to run the project

* Create a Redshift cluster with the appropriate user,role and security group in the region us-west-2.
* When the DAG updated, run the command `/opt/airflow/start.sh` to start the Airflow webserver.
* Add connections for AWS credentials and Redshift in Airflow UI.
* Start the DAG by toggling OFF to ON,the DAG will be triggered automatically.The progress of the DAG runs can be checked in the graph view and tree view tabs.

### Pipeline

![Airflow DAG](images/Airflow_dag2.png)

![Airflow SubDAG](images/Airflow_subdag.png)