from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 table,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination fact table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Inserting into fact table {}'.format(self.table))
        redshift.run('INSERT INTO "{}" {}'.format(self.table, self.sql))
