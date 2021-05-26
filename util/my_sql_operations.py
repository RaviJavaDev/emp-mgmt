import mysql.connector as connection

from logger.db_logger import DBLogger


class MySqlOperation:
    def __init__(self, transaction_id):
        host = 'localhost'
        user_name = 'root'
        password = 'root'
        self.collection_name = 'db_log'
        self.error_collection_name = 'error_log'
        try:
            self.conn = connection.connect(host=host, user=user_name, passwd=password, use_pure=True)
            self.logger = DBLogger(transaction_id)
        except Exception as e:
            self.logger.log(f'error occurred while connecting mysql database {e}', self.error_collection_name)
            raise e


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

    def save_record(self, database_name, table_name,columns, record):
        try:
            cursor = self.conn.cursor()
            parameter = '%s,' * len(record)
            parameter = parameter[:len(parameter) - 1]
            query = "INSERT INTO {}.{} ({}) VALUES({})".format(database_name, table_name, columns, parameter)
            print(query)
            cursor.execute(query, record)
            self.conn.commit()
            self.logger.log(f'inserted record successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while saving record in table {e}', self.error_collection_name)
            raise  e

    def get_record(self, database_name, table_name, _id):
        try:
            cursor = self.conn.cursor()
            query = f'SELECT * FROM {database_name}.{table_name} WHERE emp_id = {_id}'
            cursor.execute(query)
            record = cursor.fetchall()
            self.logger.log(f'fetched record query: {query}', self.collection_name)
            return record
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e

    def get_all_records(self, database_name, table_name):
        try:
            cursor = self.conn.cursor()
            query = f'SELECT * FROM {database_name}.{table_name}'
            cursor.execute(query)
            records = cursor.fetchall()
            self.logger.log(f'fetched record query: {query}', self.collection_name)
            return records
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e


if __name__ == '__main__':
    my_sql_opr = MySqlOperation('123432')
    db_name = 'EMP_MGMT_DB'
    my_sql_opr.create_database(db_name)
    columns = {'EMP_ID': 'INT', 'EMP_NAME': 'VARCHAR(40)', 'AGE': 'INT'}
    table_name = 'EMPLOYEE_HIST'
    my_sql_opr.create_table(db_name,table_name,columns)

    columns = []
    values = []
    emp = {'EMP_ID': 1, 'EMP_NAME': 'Ravi', 'AGE': 32}
    columns = ",".join([key for key in emp.keys()])
    record = tuple([key for key in emp.values()])
    my_sql_opr.save_record(db_name, table_name, columns, record)