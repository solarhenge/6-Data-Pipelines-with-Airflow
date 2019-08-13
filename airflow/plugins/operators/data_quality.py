import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 data_quality_tables="",
                 sw_skip_data_quality_checks=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.data_quality_tables = data_quality_tables
        self.sw_skip_data_quality_checks = sw_skip_data_quality_checks

    def execute(self, context):
        if not self.sw_skip_data_quality_checks:

            conn = PostgresHook(self.postgres_conn_id)

            errors = []
            info = []
            for table in self.data_quality_tables:
                records = conn.get_records(f"select count(*) from {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    errors.append(f"Data quality check failed. {table} returned no results")
                else:
                    num_records = records[0][0]
                    if num_records < 1:
                        errors.append(f"ERROR - Data quality check failed. {table} contains {num_records} records")
                    else:
                        info.append(f"SUCCESS - Data quality check passed. {table} contains {num_records} records")

                if table == "artists":
                    columns = ['latitude','location','longitude']
                    expected = [0,0,0]
                    for idx, column in enumerate(columns):
                        records = conn.get_records(f"select count(*) from {table} where 1 = 1 and {column} is null")
                        actual = records[0][0]
                        if actual != expected[idx]:
                            errors.append(f"ERROR - Data quality check failed. {table}.{column} is null...actual {actual} does not equal expected {expected[idx]}")

            for i in info:
                logging.info(i)
        
            errors_encountered = False
            for e in errors:
                errors_encountered = True
                logging.info(e)
            if errors_encountered:
                raise ValueError("Data quality check failed.")




