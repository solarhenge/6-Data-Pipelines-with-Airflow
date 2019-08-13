import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (DataQualityOperator
                              ,HasRowsOperator
                              ,LoadDimensionOperator
                              ,LoadFactOperator
                              ,StageToRedshiftOperator
                              )

# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def stg_subdag(
        parent_dag_name,
        task_id,
        aws_credentials_id,
        postgres_conn_id,
        table,
        create_sql_stmt,
        s3_prefix,
        s3_bucket,
        s3_key,
        s3_jsonpath_file,
        sw_delete_stages,
        partition_year,
        partition_month,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_table = PostgresOperator(
        task_id=f"create_table_{table}",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        sql=create_sql_stmt
    )

    load_table = StageToRedshiftOperator(
        task_id=f"load_table_{table}",
        dag=dag,
        aws_credentials_id=aws_credentials_id,
        postgres_conn_id=postgres_conn_id,
        table=table,
        s3_prefix=s3_prefix,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        s3_jsonpath_file=s3_jsonpath_file,
        sw_delete_stages=sw_delete_stages,
        partition_year=partition_year,
        partition_month=partition_month
    )

    has_rows_table = HasRowsOperator(
        task_id=f"has_rows_table_{table}",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        table=table
    )

    create_table >> load_table >> has_rows_table

    return dag
