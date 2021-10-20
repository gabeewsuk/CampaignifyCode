from pymongo import MongoClient
import pymongo
import certifi
import os
import ssl
from decouple import config


#connects to mongodb
def connect(x):
    client = pymongo.MongoClient(config('MONGO_URI'))

    db = client[x]
    print(db.list_collection_names())

    print("connected to mongoDB!")
    return db
    

