from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql = """
    insert into {}
    ('{}')
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 fact_sql="",
                 append_yn="",
                 *args, **kwargs):
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.fact_sql = fact_sql
        self.append_yn = append_yn
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_yn == "N":
            logging.info(f"Truncating table {self.table}")
            truncate_sql=f"truncate table {self.table}"
            redshift.run(truncate_sql)
        logging.info(f"inserting data into Fact table {self.table}")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.fact_sql
        )
        redshift.run(formatted_sql)
        