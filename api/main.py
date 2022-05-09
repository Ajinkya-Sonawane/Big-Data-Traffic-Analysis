from typing import Optional
from fastapi import FastAPI
from pymongo import MongoClient
import os

MONGO_URI = os.environ['MONGO_URI']
DB_NAME = os.environ['DB_NAME']

app = FastAPI()

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

@app.get("/")
def root():
    response = {}
    response["status"] = 200
    response["data"] = "This is Big Data CS GY 6513 Proejct API Endpoint"
    return response

@app.get("/chicago/crash")
def fetch_chicago_crash(query = "summary", type=None,month=None,year=None):
    response = {}
    try:
        search = {}
        if query == "summary":
            collection = db['chicago_crash_summary']
            response["data"] = [document for document in collection.find({},{"_id":0})]
        elif query == "count":
            collection = db['chicago_monthly_crash_count']            
            if month:
                search["month"] = month
            if year:
                search["year"] = year
        elif query == "cause":
            collection = db['chicago_prime_contributory_cause']            
            if month:
                search["month"] = month
            if year:
                search["year"] = year
        response["data"] = [document for document in collection.find(search,{"_id":0})]
        response["status"] = 200
    except Exception as ex:
        response["status"] = 400
        response["data"] = str(e)
    return response


