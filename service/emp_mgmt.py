from logger.db_logger import DBLogger
from util.my_sql_operations import MySqlOperation


class EmployeeMgmt:
    def __init__(self,transaction_id):
        self.logger = DBLogger(transaction_id)
        self.my_sql_db_opr = MySqlOperation(transaction_id)
        self.collection_name = ''
        self.error_collection_name = 'error_log'
        self.general_collection_name = 'general_log'

    def create_database(self):
        try:
            self.logger.log('Creating database started...', self.general_collection_name)
            database_name = 'EMP_MGMT_DB'
            self.my_sql_db_opr.create_database(database_name)
            self.logger.log('Creating database completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating database {e}', self.error_collection_name)
            raise e

    def create_table(self):
        try:
            self.logger.log('Creating table started...', self.general_collection_name)
            database_name = 'EMP_MGMT_DB'
            table_name = 'EMPLOYEE'
            columns = {'emp_id': 'INT', 'emp_name': 'VARCHAR(40)', 'age': 'INT', 'salary': 'INT',
                       'email': 'VARCHAR(40)', 'phone_no': 'VARCHAR(20)', 'dept': 'VARCHAR(20)'}
            self.my_sql_db_opr.create_table(database_name, table_name, columns)
            self.logger.log('Creating table completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_employee(self, employee):
        try:
            self.logger.log('Inserting record started...', self.general_collection_name)
            database_name = 'EMP_MGMT_DB'
            table_name = 'EMPLOYEE'
            self.my_sql_db_opr.save_record(database_name=database_name,table_name=table_name,record=employee)
            self.conn.commit()
            self.logger.log('Inserting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_multiple_employee(self, emp_list):
        try:
            self.logger.log('Inserting record started...', self.general_collection_name)
            database_name = 'EMP_MGMT_DB'
            table_name = 'EMPLOYEE'
            for employee in emp_list:
                self.my_sql_db_opr.save_record(database_name=database_name, table_name=table_name, record=employee)
                self.conn.commit()
            self.logger.log('Inserting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def get_employee_by_id(self, emp_id):
        try:
            self.logger.log('getting record started...', self.general_collection_name)
            database_name = 'EMP_MGMT_DB'
            table_name = 'EMPLOYEE'
            record = self.my_sql_db_opr.get_record(database_name=database_name, table_name=table_name, emp_id=emp_id)
            record
            self.logger.log('getting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while getting record {e}', self.error_collection_name)
            raise e

    def get_all_employee(self):
        try:
            self.logger.log('getting record started...', self.general_collection_name)
            database_name = 'EMP_MGMT_DB'
            table_name = 'EMPLOYEE'
            records = self.my_sql_db_opr.get_record(database_name=database_name, table_name=table_name)
            records
            self.logger.log('getting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while getting record {e}', self.error_collection_name)
            raise e
