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

def dim_subdag(
        parent_dag_name,
        task_id,
        postgres_conn_id,
        table,
        insert_sql_stmt,
        sw_delete_dimensions,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_table = LoadDimensionOperator(
        task_id=f"load_table_{table}",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        table=table,
        sql=insert_sql_stmt,
        sw_delete_dimensions=sw_delete_dimensions,
    )

    has_rows_table = HasRowsOperator(
        task_id=f"has_rows_table_{table}",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        table=table
    )

    load_table >> has_rows_table

    return dag
