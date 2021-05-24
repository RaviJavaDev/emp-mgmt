import pymongo
import logging
import os


class MongoOperation:
    """
    This class used to perform operation on MongoDB.
    """

    def __init__(self):
        try:
            os.makedirs('log', exist_ok=True)
            logging.basicConfig(filename=os.path.join(os.getcwd(), 'log', 'db_log.txt'),
                                level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
            logging.debug('connecting to MongoDB Started')
            user_name = 'root'
            password = 'root'
            connection_url = f"mongodb+srv://{user_name}:{password}@projectscluster.2dm5i.mongodb.net/test?retryWrites=true&w=majority"
            self.client = pymongo.MongoClient(connection_url)
            self.db_name = 'emp_mgmt_log_db'
            logging.debug('connecting to MongoDB Finish')
        except Exception as e:
            logging.error('Error occurred while connection Mongo Db.', e)
            raise e

    def get_db(self):
        """
        This method returns database object.

        :return: db object
        """
        logging.info('in get_db()')
        try:
            return self.client[self.db_name]
        except Exception as e:
            logging.error('Error occurred while getting client.', e)
        logging.info('out get_db()')

    def save_single_record(self, record, collection_name):
        """
        This method saves single record.

        :param record:
        :param collection_name:

        :return: saved record id
        """
        logging.info('in save_single_record()')
        try:
            collection = self.get_db()[collection_name]
            record = collection.insert_one(record)
            return record
        except Exception as e:
            logging.error('Error occurred while saving single record.', e)
        logging.info('out save_single_record()')

    def save_multiple_records(self, records, collection_name):
        """
        This method saves multiple record.

        :param records:
        :param collection_name:

        :return: saved record id
        """
        logging.info('in save_multiple_records()')
        try:
            collection = self.get_db()[collection_name]
            record = collection.insert_many(records)
            return record
        except Exception as e:
            logging.error('Error occurred while saving multiple records.', e)
        logging.info('out save_multiple_records()')

    def get_record(self, collection_name, filter):
        """
        This method returns single record.

        :param collection_name:
        :param filer:

        :return: record
        """
        logging.info('in get_record()')
        try:
            collection = self.get_db()[collection_name]
            record = collection.find_one(filter)
            return record
        except Exception as e:
            logging.error('Error occurred while getting records.', e)
        logging.info('in get_record()')

    def get_records(self, collection_name):
        """
        This method return multiple record.

        :param collection_name:

        :return:list of record
        """
        logging.info('in get_records()')
        try:
            collection = self.get_db()[collection_name]
            records = collection.find()
            return records
        except Exception as e:
            logging.error('Error occurred while getting record.', e)
        logging.info('in get_records()')


if __name__ == '__main__':
    mongo_operation = MongoOperation()
    record = {'log_message': 'record saved', 'transaction_id': 12345}
    # mongo_operation.save_single_record(record, 'transaction_log')
    records = mongo_operation.get_records('transaction_log')
    for record in records:
        print(record)
