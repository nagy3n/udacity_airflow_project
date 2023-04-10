from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if dq_checks is None:
            dq_checks = list()
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for dq_check in self.dq_checks:
            records = redshift_hook.get_records(dq_check['check_sql'])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {dq_check['check_sql']} returned no results")
            result = records[0][0]
            if (dq_check['is_not'] and result == dq_check['invalid_result']) or result != dq_check['expected_result']:
                raise ValueError(f"Data quality check failed. {dq_check['check_sql']} failed")
            self.log.info(f"Data quality on table {dq_check['check_sql']} check passed with {records[0][0]}")
