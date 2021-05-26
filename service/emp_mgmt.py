from logger.db_logger import DBLogger
from util.cassandra_operations import CassandraOperations
from util.my_sql_operations import MySqlOperation


class EmployeeMgmt:
    def __init__(self, transaction_id, db):
        self.collection_name = ''
        self.error_collection_name = 'error_log'
        self.general_collection_name = 'general_log'
        self.logger = DBLogger(transaction_id)
        self.db = db
        if db == 'MYSQL':
            self.my_sql_db_opr = MySqlOperation(transaction_id)
        else:
            self.cassandra_opr = CassandraOperations(transaction_id)

    def create_database(self, db_name):
        try:
            self.logger.log(f'{db_name} database creation started...', self.general_collection_name)
            if self.db == 'MYSQL':
                self.my_sql_db_opr.create_database(db_name)
            else:
                self.cassandra_opr.create_keyspace(db_name)
            self.logger.log(f'{db_name} database creation completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating {db_name} database {e}', self.error_collection_name)
            raise e

    def create_table(self, database_name, table_name, columns):
        try:
            self.logger.log('Creating table started...', self.general_collection_name)
            # columns = {'emp_id': 'INT', 'emp_name': 'VARCHAR(40)', 'age': 'INT', 'salary': 'INT',
            #           'email': 'VARCHAR(40)', 'phone_no': 'VARCHAR(20)', 'dept': 'VARCHAR(20)'}
            self.my_sql_db_opr.create_table(database_name, table_name, columns)
            self.logger.log('Creating table completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_employee(self, database_name, table_name, dct_obj):
        try:
            self.logger.log('Inserting record started...', self.general_collection_name)
            columns = ",".join([key for key in dct_obj.keys()])
            record = tuple([key for key in dct_obj.values()])
            if self.db == 'MYSQL':
                self.my_sql_db_opr.save_record(database_name=database_name,
                                               table_name=table_name, columns=columns, record=record)
            else:
                self.cassandra_opr.save_record(keyspace_name=database_name,
                                               table_name=table_name, columns=columns, record=record)
            self.logger.log('Inserting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_multiple_employee(self, database_name, table_name, dict_list):
        try:
            self.logger.log('Inserting record started...', self.general_collection_name)
            for dct_obj in dict_list:
                columns = ",".join([key for key in dct_obj.keys()])
                record = tuple([key for key in dct_obj.values()])
                self.my_sql_db_opr.save_record(database_name=database_name, table_name=table_name, columns=columns,
                                               record=record)
            self.logger.log('Inserting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def get_employee_by_id(self, database_name, table_name, _id):
        try:
            self.logger.log('getting record started...', self.general_collection_name)
            records = self.my_sql_db_opr.get_record(database_name=database_name, table_name=table_name, _id=_id)
            response = []
            for record in records:
                response.append(record)
            self.logger.log('getting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while getting record {e}', self.error_collection_name)
            raise e
        return response

    def get_all_employee(self, database_name, table_name):
        try:
            self.logger.log('getting record started...', self.general_collection_name)
            records = self.my_sql_db_opr.get_all_records(database_name=database_name, table_name=table_name)
            response = []
            for record in records:
                response.append(record)
            self.logger.log('getting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while getting record {e}', self.error_collection_name)
            raise e
        return response
