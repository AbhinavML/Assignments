from DatabaseOperation import database_operation
from Constant import FILE_ADDRESS, COLLECTION_NAME

data = database_operation(FILE_ADDRESS, COLLECTION_NAME)

# To upload data to given collection
data.upload_data_to_collection(";")

# To filter out data according to key and given operation
data.filter_data("Chiral indice n", "$gt", 11)

# To find details for particular key
data.find_details("Chiral indice n", 7)

# To delete all the documents in the collection
data.delete_all_documents_in_collection()

# To update details in the collection
data.update_details("Chiral indice n", 2, "Chiral indice m", 55)

# To delete selected documents as per the condition
data.delete_documents_with_filter("Chiral indice m", 55)