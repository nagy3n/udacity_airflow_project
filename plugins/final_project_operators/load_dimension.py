from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination dimension table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Inserting into dimension table {}'.format(self.table))
        redshift.run('INSERT INTO {} {}'.format(self.table, self.sql))
