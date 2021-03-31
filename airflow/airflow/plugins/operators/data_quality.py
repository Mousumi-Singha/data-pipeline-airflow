from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL query and expected result
    
    :param postgres_conn_id: Redshift connection ID
    :param test_query: SQL query to run on Redshift data warehouse
    
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 test_query=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.test_query = test_query
        
        

    def execute(self, context):
        self.log.info("Getting credentials")
        postgres = PostgresHook(self.postgres_conn_id)

        self.log.info("Running test")
        for test in self.test_query:
            result = int(postgres.get_first(sql=test['sql'])[0])
            
             # check if equal
            if test['op'] == 'eq':
                if result != test['val']:
                    raise AssertionError(f"Check failed: {result} {test['op']} {test['val']}")
            # check if not equal
            elif stmt['op'] == 'ne':
                if result == test['val']:
                    raise AssertionError(f"Check failed: {result} {test['op']} {test['val']}")
            # check if greater than
            elif test['op'] == 'gt':
                if result <= test['val']:
                    raise AssertionError(f"Check failed: {result} {test['op']} {test['val']}")

            self.log.info(f"Passed check: {result} {test['op']} {test['val']}")
