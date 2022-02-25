from pymongo.errors import OperationFailure

from InitializeMongo import returnCollection
from Constant import FIND_DETAIL_LOG_FILE, INFO, ERROR, UPLOAD_DATA_LOG_FILE, UPDATE_DETAIL_LOG_FILE, DELETE_LOG_FILE, FILTERED_RESULT_LOG_FILE
import csv
from Logger import log


class database_operation:

    def __init__(self, csv_file, collection_name):
        self.csv_file = csv_file
        self.collection_name = collection_name

    def upload_data_to_collection(self, deli):
        """
        This method add multiple documents to given collection by fetching it from the give csv file
        :param deli: delimiter in the data set
        """
        collection = returnCollection()
        logger = log(UPLOAD_DATA_LOG_FILE, INFO)
        logger.info(f"Adding documents to collection: {self.collection_name}")

        with open(self.csv_file, 'r') as f:
            datas = csv.reader(f, delimiter=deli)
            data_list = []
            i = 0
            for data in datas:
                if i > 0:
                    chiral_indice_n = int(data[0])
                    chiral_indice_m = int(data[1])
                    initial_atomic_coordinate_u = data[2]
                    initial_atomic_coordinate_v = data[3]
                    initial_atomic_coordinate_w = data[4]
                    calculated_atomic_coordinates_u_transpose = data[5]
                    calculated_atomic_coordinates_v_transpose = data[6]
                    calculated_atomic_coordinates_w_transpose = data[7]
                    dictionary = {"Chiral indice n": chiral_indice_n, "Chiral indice m": chiral_indice_m,
                                  "Initial atomic coordinate u": initial_atomic_coordinate_u,
                                  "Initial atomic coordinate v": initial_atomic_coordinate_v,
                                  "Initial atomic coordinate w": initial_atomic_coordinate_w,
                                  "Calculated atomic coordinates u'": calculated_atomic_coordinates_u_transpose,
                                  "Calculated atomic coordinates v'": calculated_atomic_coordinates_v_transpose,
                                  "Calculated atomic coordinates w'": calculated_atomic_coordinates_w_transpose}
                    data_list.append(dictionary)
                i += 1
        collection.insert_many(data_list)

    def update_details(self, key_to_find, value_to_find, key_to_update, value_to_update):
        """
        This method update key and value in a document which is filtered with key_to_find and value_to_find
        :param key_to_find: key for which data is filtered
        :param value_to_find: value for which data is filtered
        :param key_to_update: key for which details is to be updated
        :param value_to_update: value with which key is updated
        """
        logger = log(UPDATE_DETAIL_LOG_FILE, INFO)
        returnCollection().update_many({key_to_find: value_to_find}, {"$set": {key_to_update: value_to_update}})
        logger.info(
            f"Document with key:{key_to_find} and value:{value_to_find} is updated with key:{key_to_update} and value:{value_to_update}")

    def find_details(self, key, number_of_data):
        """
        This method fetch details of the mentioned key in the document
        :param key: key for which details is to be fetched
        :param number_of_data: number of records to be fetched
        """
        info_logger = log(FIND_DETAIL_LOG_FILE, INFO)
        error_logger = log(FIND_DETAIL_LOG_FILE, ERROR)
        try:
            for i in returnCollection().find().limit(number_of_data):
                info_logger.info(f"The value of: {key} is: {i[key]}")
        except KeyError as e:
            error_logger.error(f"This is no such keys as: {e} in collection")

    def delete_documents_with_filter(self, key, value):
        """
        This method deletes all the documents containing entered key and value
        :param key: key in document for which delete operation is to be performed
        :param value: value for key in document for which delete operation is to be performed
        """
        logger = log(DELETE_LOG_FILE, INFO)
        logger.info(f"Deleting all the document containing key: {key} and value: {value}")
        returnCollection().delete_many({key: value})

    def delete_all_documents_in_collection(self):
        """
        This method deletes all the documents in the given collection
        """
        logger = log(DELETE_LOG_FILE, INFO)
        returnCollection().delete_many({})
        logger.info(f"Deleted all the document in the collection: {self.collection_name}")

    def filter_data(self, key, operator, value):
        """
        It returns the filtered data as per the key, operator and value entered above
        :param key: key on basis of which data is filtered
        :param operator: operation that user wants to perform on collection
        :param value: value of key on basis of which data is filtered
        """
        info_logger = log(FILTERED_RESULT_LOG_FILE, INFO)
        error_logger = log(FILTERED_RESULT_LOG_FILE, ERROR)
        try:
            info_logger.info(f"Value for {key} with operator: {operator} and value: {value}")
            for i in returnCollection().find({key: {operator: value}}):
                info_logger.info(i)
        except OperationFailure as e:
            error_logger.error(f"This {operator} is an unknown operator which throws {e}")


