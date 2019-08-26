from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        insert into {}
        ('{}')
      """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 dimension_sql="",
                 append_yn="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.dimension_sql = dimension_sql
        self.append_yn = append_yn

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('inserting data into Dimension table {}'.format(self.table))
        if self.append_yn == "N":
            self.log.info('Truncating table '.format(self.table))
            truncate_sql="truncate table {}".format(self.table)
            redshift.run(formatted_sql)
            
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.dimension_sql
            )
        redshift.run(formatted_sql)
