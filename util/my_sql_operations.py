import mysql.connector as connection

from logger.db_logger import DBLogger


class MySqlOperation:
    def __init__(self, transaction_id):
        host = 'localhost'
        user_name = 'root'
        password = 'root'
        self.conn = connection.connect(host=host, user=user_name, passwd=password, use_pure=True)
        self.logger = DBLogger(transaction_id)
        self.collection_name = 'db_log'
        self.error_collection_name = 'error_log'

    def create_database(self, database_name):
        try:
            cursor = self.conn.cursor()
            query = f'CREATE DATABASE IF NOT EXISTS {database_name}'
            cursor.execute(query)
            self.logger.log(f'created database query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating database {e}', self.error_collection_name)
            raise e

    def create_table(self, database_name, table_name, columns):
        try:
            cursor = self.conn.cursor()
            column_query = ''
            for key, value in columns.items():
                column_query += f'{key} {value}, '
            query = f'CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ({column_query})'
            self.logger.log(f'creating table started query: {query}', self.collection_name)
            query = query[:len(query)-3] + ')'
            cursor.execute(query)
            self.logger.log(f'creating table finished query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_record(self, database_name, table_name, record):
        try:
            cursor = self.conn.cursor()
            query = f'INSERT INTO {database_name}.{table_name} VALUES({record}'
            cursor.execute(query)
            self.logger.log(f'inserted record query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while saving record in table {e}', self.error_collection_name)
            raise  e

    def get_record(self, database_name, table_name, emp_id):
        try:
            cursor = self.conn.cursor()
            query = f'SELECT * FROM {database_name}.{table_name} WHERE emp_id = %s'
            record = cursor.execute(query, emp_id)
            self.logger.log(f'fetched record query: {query}', self.collection_name)
            return record
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e

    def get_all_records(self, database_name, table_name):
        try:
            cursor = self.conn.cursor()
            query = f'SELECT * FROM {database_name}.{table_name}'
            records = cursor.execute(query)
            self.logger.log(f'fetched record query: {query}', self.collection_name)
            return records
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e


if __name__ == '__main__':
    my_sql_opr = MySqlOperation()
    db_name = 'EMP_MGMT_DB'
    my_sql_opr.create_database(db_name)
    columns = {'EMP_ID': 'INT', 'EMP_NAME': 'VARCHAR(40)', 'AGE': 'INT'}
    table_name = 'EMPLOYEE_HIST'
    my_sql_opr.create_table(columns, table_name)
