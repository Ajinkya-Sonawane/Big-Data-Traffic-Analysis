from typing import Optional
from fastapi import FastAPI
from pymongo import MongoClient
from fastapi.middleware.cors import CORSMiddleware
import os
import calendar

MONGO_URI = os.environ['MONGO_URI']
DB_NAME = os.environ['DB_NAME']

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

@app.get("/")
def root():
    response = {}
    response["status"] = 200
    response["data"] = "This is Big Data CS GY 6513 Proejct API Endpoint"
    return response

@app.get("/chicago/crash")
def fetch_chicago_crash(query = "summary", category=None,month=None,year=None):
    response = {}    
    try:
        search = {}
        collection = None
        if query.lower() == "summary":
            collection = db['chicago_crash_summary']
        elif query.lower() == "count":
            collection = db['chicago_monthly_crash_count']            
        elif query.lower() == "damage":
            collection = db['chicago_total_damage']            
        elif query.lower() == "weather":
            collection = db['chicago_weather_conditions_during_accident']
        elif query.lower() == "violation" and category.lower() == "light":
            collection = db['chicago_red_light_violations_summary']
        elif query.lower() == "violation" and category.lower() == "speed":
            collection = db['chicago_speed_violations_summary']
        elif query.lower() == "cause":
            collection = db['chicago_prime_contributory_cause']
        if month:
            search["month"] = month
        if year:
            search["year"] = year
        else:
            raise Exception("Invalid year")
        if collection  is not None:
            if query.lower() == "cause":
                data = collection.aggregate(
                    [
                    { "$match": { "year": year} },
                    {
                    "$group" : 
                        {"_id" : "$PRIM_CONTRIBUTORY_CAUSE", 
                        "count" : {"$sum" : "$prim_contributory_cause_count"}
                        }}
                    ])
                response["data"] = [document for document in data]
            else:
                data = [document for document in collection.find(search,{"_id":0})]
                temp = []
                for document in data:
                    document["month"] = calendar.month_name[int(document["month"])][:3]
                    temp.append(document)
                response["data"] = temp
            response["status"] = 200
        else:
            raise Exception("Invalid Data Source")
    except Exception as ex:
        response["status"] = 400
        response["data"] = str(ex)
    return response


@app.get("/nyc/crash")
def fetch_nyc_crash(query = "summary", category=None,month=None,year=None):
    response = {}
    try:
        search = {}
        collection = None
        if query.lower() == "summary":
            collection = db['newyork_crash_summary']
        elif query.lower() == "count":
            collection = db['newyork_monthly_crash_count']            
        elif query.lower() == "cause":
            collection = db['newyork_prime_contributory_cause']
        elif query.lower() == "vehicle":
            collection = db['newyork_vehicle_type_crash_cause']
        if month:
            search["month"] = month
        if year:
            search["year"] = year
        if year:
            search["year"] = year
        else:
            raise Exception("Invalid year")        
        if collection is not None:
            if query.lower() == "cause":
                response["data"] = [document for document in collection.aggregate(
                [
                { "$match": { "year": year} },
                {
                "$group" : 
                    {"_id" : "$contributing_factor_vehicle_1", 
                    "count" : {"$sum" : "$prim_contributory_cause_count"}
                    }}
                ])]
            else:
                data = [document for document in collection.find(search,{"_id":0})]
                temp = []
                for document in data:
                    document["month"] = calendar.month_name[int(document["month"])][:3]
                    temp.append(document)
                response["data"] = temp
            response["status"] = 200
        else:
            raise Exception("Invalid Data Source")
    except Exception as ex:
        response["status"] = 400
        response["data"] = str(ex)
    return response


@app.get("/dropdown")
def fetch_dropdown(dataset = "chicago"):
    response = {}
    try:
        search = {}
        collection = None
        years = set()
        if dataset.lower() == "chicago" or dataset.lower() == "violation":
            collection = db['chicago_crash_summary']
            for year in collection.distinct("year"):
                years.add(year)
        elif dataset.lower() == "newyork":
            collection = db['newyork_crash_summary']
            for year in collection.distinct("year"):
                years.add(year) 
        if collection is not None:
            print(years)
            response["data"] =  list(sorted(years))
            response["status"] = 200
        else:
            response["status"] = 400
            response["data"] = "Invalid data source"
    except Exception as ex:
        response["status"] = 400
        response["data"] = str(ex)
    return response