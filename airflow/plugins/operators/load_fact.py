from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    template_fields = ("sql",)

    @apply_defaults
    def __init__(self
                ,postgres_conn_id=""
                ,table=""
                ,sw_delete_fact=False
                ,sql=""
                ,*args
                ,**kwargs
                ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sw_delete_fact = sw_delete_fact
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadFactOperator begin')

        # connect
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # do we want to truncate the fact?
        if self.sw_delete_fact:
            truncate_sql=f"truncate {self.table}"
            conn.run(truncate_sql)

        # render sql statement
        rendered_sql = self.sql.format(**context)
        self.log.info(rendered_sql)

        # run rendered sql statement
        conn.run(rendered_sql)

        self.log.info('LoadFactOperator end')
