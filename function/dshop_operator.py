from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

class DshopCsvOperator(BaseOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            table_name = None,
            *args, **kwargs):
        super(DshopCsvOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.table_name = table_name

    def execute(self, context):
        hdfs_conn = BaseHook.get_connection('hdfs_conn_id')
        client = InsecureClient(hdfs_conn.host, user=hdfs_conn.login)
        self.log.info("Successfully connect to HDFS")
        file_path = os.path.join("/", "bronze", "postgres", '{{ ds }}', self.table_name)
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Successfully connect to Postgres")
        with client.write(file_path+'.csv') as csv_file:
            cursor.copy_expert(f'COPY {self.sql} TO STDOUT WITH HEADER CSV', csv_file)
        self.log.info(f"Successfully write to {file_path}.csv")
        conn.close()
        self.log.info(f"Successfully close connection to dbase")
