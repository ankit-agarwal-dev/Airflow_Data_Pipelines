import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 column_name="",
                 no_of_null_val="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column_name = column_name
        self.no_of_null_val = no_of_null_val
    
    def execute(self, context):
        self.log.info('Check number of Null values in the table {} and column{},\
                      expected null values are {}'.format(self.table,self.column_name\
                                                          ,self.no_of_null_val))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} where\
        {self.column_name} is null")
        if records > self.no_of_null_val:
            raise ValueError(f"Data quality check failed. {self.table} returned more \
            than allowed null values")
        logging.info(f"Data quality on table {self.table} check passed")