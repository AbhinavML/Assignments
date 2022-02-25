import pymongo
from Constant import USERNAME, PASSWORD, COLLECTION_NAME


def returnCollection():
    uri = "mongodb+srv://" + USERNAME + ":" + PASSWORD + "@cluster1.5dp3d.mongodb.net/myFirstDatabase?retryWrites" \
                                                         "=true&w=majority"
    client = pymongo.MongoClient(uri)
    database = client.ineuron_database
    collection = database[COLLECTION_NAME]

    return collection
