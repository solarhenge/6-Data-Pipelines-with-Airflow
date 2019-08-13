from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    template_fields = ("sql",)

    @apply_defaults
    def __init__(self
                ,postgres_conn_id=""
                ,table=""
                ,sw_delete_dimensions=False
                ,sql=""
                ,*args
                ,**kwargs
                ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table=table
        self.sw_delete_dimensions=sw_delete_dimensions
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator begin')

        # connect to local
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # do we want to truncate the dimension?
        if self.sw_delete_dimensions:
            truncate_sql=f"truncate {self.table}"
            conn.run(truncate_sql)

        # render sql statement
        rendered_sql = self.sql.format(**context)
        self.log.info(rendered_sql)

        # run rendered sql statement
        conn.run(rendered_sql)

        if self.table == "artists":
            update_sql=f"update {self.table} set location = null where location = ''"
            conn.run(update_sql)

        self.log.info('LoadDimensionOperator end')
