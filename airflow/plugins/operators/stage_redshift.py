from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as JSON '{}'
    """

    template_fields = ("s3_key","partition_year","partition_month",)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 #delimiter=",",
                 #ignore_headers=1,
                 postgres_conn_id="",
                 s3_prefix="",
                 s3_bucket="",
                 s3_key="",
                 s3_jsonpath_file="",
                 table="",
                 sw_delete_stages=False,
                 partition_year="",
                 partition_month="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        #self.delimiter = delimiter
        #self.ignore_headers = ignore_headers
        self.postgres_conn_id = postgres_conn_id
        self.s3_prefix = s3_prefix
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_jsonpath_file = s3_jsonpath_file
        self.table = table
        self.sw_delete_stages=sw_delete_stages
        self.partition_year=partition_year
        self.partition_month=partition_month

    def execute(self, context):
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if self.sw_delete_stages:
            self.log.info("clearing data from destination redshift table")
            conn.run("DELETE FROM {}".format(self.table))

        do_the_copy = False

        if self.table == "staging_events":
            self.log.info(f"get_records select count(*) from {self.table}")
            partition_year = self.partition_year.format(**context)
            partition_month = self.partition_month.format(**context)
            sql_stmt = f"""select count(*) 
                           from {self.table} 
                           where 1 = 1 
                           and extract(year from TIMESTAMP 'epoch' + ts / 1000 * interval '1 second') = {partition_year} 
                           and extract(month from TIMESTAMP 'epoch' + ts / 1000 * interval '1 second') = {partition_month}
                        """
            self.log.info(sql_stmt)
            records = conn.get_records(sql_stmt)
            num_records = records[0][0]
            if num_records < 1:
                do_the_copy = True

        if self.table == "staging_songs":
            self.log.info(f"get_records select count(*) from {self.table}")
            sql_stmt = f"""select count(*)
                           from {self.table}
                           where 1 = 1
                        """ 
            records = conn.get_records(sql_stmt)
            num_records = records[0][0]
            if num_records < 1:
                do_the_copy = True

        if do_the_copy:
            self.log.info("copying data from s3 to redshift")
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            rendered_key = self.s3_key.format(**context)
            s3_path = "{}/{}/{}".format(self.s3_prefix, self.s3_bucket, rendered_key)
            if self.table == "staging_events":
                s3_json_format = "{}/{}/{}".format(self.s3_prefix, self.s3_bucket, self.s3_jsonpath_file)
            if self.table == "staging_songs":
                s3_json_format = self.s3_jsonpath_file
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                s3_json_format
            )
            self.log.info(formatted_sql)
            conn.run(formatted_sql)
