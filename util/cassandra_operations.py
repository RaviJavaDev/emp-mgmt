from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from logger.db_logger import DBLogger


class CassandraOperations:
    def __init__(self, transaction_id, cluster_type='local'):
        self.cluster_type = cluster_type
        self.logger = DBLogger(transaction_id)
        self.collection_name = 'db_log'
        self.error_collection_name = 'error_log'
        self.session = self.__get_session()

    def __get_session(self):
        try:
            if self.cluster_type == 'cloud':
                cloud_config = {
                    'secure_connect_bundle': 'C:\Ravis\Data Science\Practice\Cassandra\secure-connect-Test.zip'
                }
                auth_provider = PlainTextAuthProvider('ihMLPIrUYsFpBGhDPFPPNTzP',
                                                      'XT9.cqTK4mTAyPwM.mp6-1B1IxJP1sCYW9QPPjhTb_z-K,H0IUHIE7+hO9A2xwJnosBEt9tFBId0dF9qU7xxL+-dF86JLKREjDgik0b5YlIBWq7Ssy-SmHt07igw.stL')
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session_cloud = cluster.connect()
                return session_cloud
            else:
                cluster = Cluster()
                session_local = cluster.connect()
                return session_local
        except Exception as e:
            self.logger.log(f'error occurred while connecting mysql database {e}', self.error_collection_name)
            raise e

    def create_keyspace(self, database_name):
        try:
            query = "CREATE KEYSPACE IF NOT EXISTS " + ' ' + database_name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}"
            self.session.execute(query)
            self.logger.log(f'created keyspace successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating keyspace {e}', self.error_collection_name)
            raise e

    def create_table(self, keyspace_name, table_name, columns):
        try:
            column_query = ''
            for key, value in columns.items():
                column_query += f'{key} {value}, '
            query = f'CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} ({column_query})'
            query = query[:len(query) - 3] + ')'
            print(query)
            self.session.execute(query)
            self.logger.log(f'creating table successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_record(self, keyspace_name, table_name, columns, record):
        try:
            parameter = '%s,' * len(record)
            parameter = parameter[:len(parameter) - 1]
            query = "INSERT INTO {}.{} ({}) VALUES({})".format(keyspace_name, table_name, columns, parameter)
            print(query)
            print(record)
            self.session.execute(query, record)
            self.logger.log(f'inserted record successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while saving record in table {e}', self.error_collection_name)
            raise e

    def get_record(self, keyspace_name, table_name, _id):
        try:
            query = f'SELECT * FROM {keyspace_name}.{table_name} WHERE emp_id = {_id}'
            record = self.session.execute(query)
            self.logger.log(f'fetched record successfully. query: {query}', self.collection_name)

            return record
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e

    def get_all_records(self, keyspace_name, table_name):
        try:
            query = f'SELECT * FROM {keyspace_name}.{table_name}'
            records = self.session.execute(query)
            self.logger.log(f'fetched record successfully. query: {query}', self.collection_name)
            return records
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e


if __name__ == '__main__':
    my_sql_opr = CassandraOperations('1234')
    db_name = 'EMP_MGMT_DB'
    my_sql_opr.create_keyspace(db_name)

    columns = {'EMP_ID': 'int' + ' PRIMARY KEY', 'EMP_NAME': 'text', 'AGE': 'int'}
    table_name = 'EMPLOYEE_HIST'
    my_sql_opr.create_table(db_name, table_name, columns)

    columns = []
    values = []
    emp = {'EMP_ID': 1, 'EMP_NAME': 'Ravi', 'AGE': 32}
    columns = ",".join([key for key in emp.keys()])
    record = tuple([key for key in emp.values()])
    #my_sql_opr.save_record(db_name, table_name, columns, record)

    records = my_sql_opr.get_all_records(db_name, table_name)
    for record in records:
        print(record)
