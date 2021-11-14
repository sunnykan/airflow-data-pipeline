# Project Summary
Use Apache-Airflow to schedule tasks to extract data in JSON format from S3 buckets, transform it based on a star schema and load it into a data warehouse on AWS Redshift. Data consists of songs and user activity on a music streaming app. User activity and metadata on songs are recorded in separate JSON files which are kept in S3 buckets. Airflow is used to schedule tasks to: create tables, load data into staging tables, load data from staging table into fact and dimension tables, conducting rudimentary quality checks on the data. The DAG is shown below: 

![DAG](https://github.com/sunnykan/airflow-data-pipeline/blob/main/images/airflow-pipeline.png "DAG")

# Execution
Clone the project and open the folder:

1. Open and run the Jupyter notebook `create_cluster.ipynb` to create a new Redshift cluster and connect to it. Alternatively, run `python create_cluster.py` in the terminal.
2. Cluster creation will take some time. Once it has been created and a connection established, the endpoint (HOST) and the Amazon Resource Name (ARN) will be printed. Copy these and fill out the relevant fields in the config file `dwh.cfg`, namely ARN and DWH_HOST.
3. Ensure that Docker is running and type `docker-compose up` in your terminal.
4. Once the Airflow webserver is running, type `0.0.0.0:8080` in your browser to access the Airflow web UI. 
5. Create connections for accessing S3 and Redshift using the Admin dropdown. These should respectively be called 'aws-credentials' and 'redshift'.
6. Turn on the DAG to initiate the ETL.

# Files
- `create_cluster.ipynb`: Creates a Redshift cluster and connect to it. Alternativel, use `create_cluster.py`.
- `docker-compose.yml`: Builds the required images.
- `dags/etl_dag.py`: Contains the DAG and associated tasks.
- `sql/create_tables.sql`: SQL queries for creating tables.
- `plugins/helpers/sql_queries.py`: Helper class with SQL queries used for inserting data.
- `plugins/operators/`: Contains files with custom operators for moving data from S3 to redshift, loading the fact and dimension tables and conducting data quality checks.
