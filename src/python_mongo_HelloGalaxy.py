##
# Python Sample Application: Connection to Informix using Mongo
##

# Topics
# 1 Data Structures
# 1.1 Create collection
# 1.2 Create table
# 2 Inserts
# 2.1 Insert a single document into a collection 
# 2.2 Insert multiple documents into a collection 
# 3 Queries
# 3.1 Find one document in a collection 
# 3.2 Find documents in a collection 
# 3.3 Find all documents in a collection 
# 3.4 Count documents in a collection 
# 3.5 Order documents in a collection 
# 3.6 Find distinct fields in a collection 
# 3.7 Joins
# 3.7a Collection-Collection join
# 3.7b Table-Collection join
# 3.7c Table-Table join 
# 3.8 Modifying batch size 
# 3.9 Find with projection clause 
# 4 Update documents in a collection 
# 5 Delete documents in a collection 
# 6 SQL passthrough 
# 7 Transactions
# 8 Commands
# 8.1 Count  
# 8.2 Distinct 
# 8.3 CollStats 
# 8.4 DBStats 
# 9 Drop a collection

from pymongo.mongo_client import MongoClient
import json
import logging
import os
from flask import Flask, render_template

app = Flask(__name__)

# To run locally, set URL and DATABASE information.
# Otherwise, url and database information will be parsed from 
# the Bluemix VCAP_SERVICES.
URL = ""
DATABASE = ""

USE_SSL = False     # Set to True to use SSL url from VCAP_SERVICES
SERVICE_NAME = os.getenv('SERVICE_NAME', 'timeseriesdatabase')
port = int(os.getenv('VCAP_APP_PORT', 8080))

class City:
    def __init__(self, name, population, longitude, latitude, countryCode):
        self.name = name
        self.population = population
        self.longitude = longitude
        self.latitude = latitude
        self.countryCode = countryCode
    def toJSON(self):
        return json.loads("{\"name\" : \"%s\" , \"population\" : %d , \"longitude\" : %.4f , \"latitude\" : %.4f , \"countryCode\" : %d}"
                      %(self.name, self.population, self.longitude, self.latitude, self.countryCode))
        
kansasCity = City("Kansas City", 467007, 39.0997, 94.5783, 1)
seattle = City("Seattle", 652405, 47.6097, 122.3331, 1)
newYork = City("New York", 8406000, 40.7127, 74.0059, 1)
london = City("London", 8308000, 51.5072, 0.1275, 44)
tokyo = City("Tokyo", 13350000, 35.6833, -139.6833, 81)
madrid = City("Madrid", 3165000, 40.4000, 3.7167, 34)
melbourne = City("Melbourne", 4087000, -37.8136, -144.9631, 61)
sydney = City("Sydney", 4293000, -33.8650, -151.2094, 61)

def getDatabaseInfo():
    """
    Get database and url information
    
    :returns: (database, url)
    """
    
    # Use defaults
    if URL and DATABASE:
        return DATABASE, URL
    
    # Parse database info from VCAP_SERVICES
    if (os.getenv('VCAP_SERVICES') is None):
        raise Exception("VCAP_SERVICES not set in the environment")
    vcap_services = json.loads(os.environ['VCAP_SERVICES'])
    try:
        tsdb = vcap_services[SERVICE_NAME][0]
        credentials = tsdb['credentials']
        database = credentials['db']
        if USE_SSL:
            url = credentials['mongodb_url_ssl']
        else:
            url = credentials['mongodb_url']
        return database, url
    except KeyError as e:
        logging.error(e)
        raise Exception("Error parsing VCAP_SERVICES. Key " + str(e) + " not found.")
        

def doEverything():
    # Get database connectivity information
    database, url = getDatabaseInfo()

    # Run test
    try:
        client = MongoClient(url)
        db = client[database]
        
        output = []
        collectionName = "pythonMongoGalaxy"
        joinCollectionName = "pyJoin"
        cityTableName = "cityTable"
        codeTableName = "codeTable"
        
        output.append("# 1 Data Structures")
        output.append("# 1.1 Create a collection")
        output.append("Creating collection " + collectionName + " " + joinCollectionName)
        collection = db[collectionName]
        joinCollection = db[joinCollectionName]
        
        output.append("# 1.2 Create a table")
        output.append("Creating tables " + codeTableName + " " + cityTableName)
         
        db.command({"create" : codeTableName, "columns":[{"name":"countryCode","type":"int"},
                                                                    {"name": "countryName", "type": "varchar(50)"}]})
                  
        db.command({"create" : cityTableName, "columns":[{"name":"name","type":"varchar(50)"},
                                                                    {"name": "population", "type": "int"}, {"name": "longitude", "type": "decimal(8,4)"},
                                                                    {"name": "latitude", "type": "decimal(8,4)"}, {"name": "countryCode", "type": "int"}]})
        
        
        #insert 1
        output.append("# 1 Inserts")
        output.append("# 1.1 Insert a single document to a collection")
        collection.insert(kansasCity.toJSON())
        output.append("Inserted" )
        output.append(kansasCity.toJSON())
        
        #insert many
        output.append("#1.2 Inserting multiple entries into collection")
        multiPost = [seattle.toJSON(), newYork.toJSON(), london.toJSON(), tokyo.toJSON(), madrid.toJSON()] 
        collection.insert(multiPost)
        output.append("Inserted \n%s \n%s \n%s \n%s \n%s" % (seattle.toJSON(), newYork.toJSON(), london.toJSON(), tokyo.toJSON(), madrid.toJSON()))
        
        # # Find 
        output.append("\n#2 Queries")
        output.append("#2.1 Find one that matches a query condition")
        output.append(collection.find_one({"name": kansasCity.name}))
         
        # Find all 
        output.append("#2.2 Find all that match a query condition")
        for doc in collection.find({"longitude": {"$gt" : "40.0"}}):
            output.append(doc)
         
        # Display all documents
        output.append("#2.3 Find all documents in collection")
        for doc in collection.find():
            output.append(doc)
            
        #Count     
        output.append("#2.4 Count documents in collection")
        num = collection.find({"population": {"$lt" : 8000000}}).count()
        output.append("There are %d documents with a population less than 8 million" % num)
        
        #Order 
        output.append("#2.5 Order documents in collection")
        for doc in collection.find().sort("population", -1):
            output.append(doc)    
         
        # Distinct
        output.append("#2.6 Find distinct codes in collection")
        for doc in collection.distinct("countryCode"):
            output.append(doc)
            
        #Joins
        output.append("#2.7 Joins")
        sys = db["system.join"]
         
        joinCollection.insert({"countryCode": 1, "countryName": "United States of America" })
        joinCollection.insert({"countryCode": 44, "countryName": "United Kingdom" })
        joinCollection.insert({"countryCode": 81, "countryName": "Japan" })
        joinCollection.insert({"countryCode": 34, "countryName": "Spain" })
        joinCollection.insert({"countryCode": 61, "countryName": "Australia" })
         
        codeTable = db[codeTableName]
        codeTable.insert({"countryCode": 1}, {"countryName": "United State of America"})
        codeTable.insert({"countryCode": 44 }, {"countryName": "United Kingdom"})
        codeTable.insert({"countryCode": 81 }, {"countryName": "Japan"})
        codeTable.insert({"countryCode": 34 }, {"countryName": "Spain"})
        codeTable.insert({"countryCode": 61 }, {"countryName": "Australia"})
         
        codeTable = db[cityTableName]
        codeTable.insert(kansasCity.toJSON())
        codeTable.insert(multiPost)
         
        output.append("#2.7a Join collection-collection")
        joinCollectionCollection = { "$collections" : { collectionName : { "$project" : { "name" : 1 , "population" : 1 , "longitude" : 1 , "latitude" : 1}} , 
                                                       joinCollectionName : { "$project" : { "countryCode" : 1 , "countryName" : 1}}} , 
                                    "$condition" : { "pythonMongoGalaxy.countryCode": "pyJoin.countryCode"}}
        for doc in sys.find(joinCollectionCollection):
            output.append(doc)
              
        output.append("#2.7b Join table-collection")
        joinTableCollection = { "$collections" : { cityTableName : { "$project" : { "name" : 1 , "population" : 1 , "longitude" : 1 , "latitude" : 1}} , 
                                                  joinCollectionName : { "$project" : { "countryCode" : 1 , "countryName" : 1}}} , 
                               "$condition" : { "cityTable.countryCode": "pyJoin.countryCode"}}
        for doc in sys.find(joinTableCollection):
            output.append(doc)
              
        output.append("#2.7c Join table-table")
        joinTableTable= { "$collections" : { cityTableName : { "$project" : { "name" : 1 , "population" : 1 , "longitude" : 1 , "latitude" : 1}} ,
                                                   codeTableName : { "$project" : { "countryCode" : 1 , "countryName" : 1}}} , 
                               "$condition" : { "cityTable.countryCode": "codeTable.countryCode"}}
          
        for doc in sys.find(joinTableTable):
            output.append(doc)
         
        
        output.append("#2.8 Changed Batch Size")
        # docs = collection.find().batch_size(2)
        # for doc in docs:
        #     output.append(doc)
        
        output.append("#2.9 Projection clause")
        output.append("Displaying results without longitude and latitude:")
        for doc in collection.find({"countryCode" : 1}, {"longitude":0, "latitude" : 0}):
            output.append(doc)
        
        # update document
        output.append("\n#3 Update Documents")
        collection.update({"name": seattle.name}, {"$set": { "countryCode": 999}})
        output.append("Updated %s with countryCode 999" % seattle.name)
        
        # delete document
        output.append("\n#4 Delete Documents")
        collection.remove({"name": tokyo.name})  
        output.append("Deleted all with name %s" % tokyo.name)
        
        # Display all collection names
        output.append("\n#5 Get a list of all of the collections")
        output.append( db.collection_names())
        
        #SQL Passthrough
        output.append("\n#6 SQL passthrough")
        sql = db["system.sql"]
        query = {"$sql": "create table town (name varchar(255), countryCode int)"}
        for doc in sql.find(query):
            output.append(doc)
        
        query = {"$sql": "insert into town values ('Lawrence', 1)"}
        for doc in sql.find(query):
            output.append(doc)
        
        query = {"$sql": "drop table town"}
        for doc in sql.find(query):
            output.append(doc)
        
        #Transactions
        output.append("\n#7 Transactions")
        db.command({"transaction": "enable"})
        collection.insert(sydney.toJSON())
        db.command({"transaction": "commit"})
          
        collection.insert(melbourne.toJSON())
        db.command({"transaction": "rollback"})
        db.command({"transaction": "disable"})
        
        for doc in collection.find():
            output.append(doc)
        
        output.append("\n#8 output")
        
        output.append("#8.1 Count")
        count = db.command("count", collectionName)
        output.append("There are %d documents in the collection" % count['n'])
        
        output.append("#8.2 Distinct")
        distinct = db.command("distinct", collectionName, key="countryCode")
        output.append("The distinct country codes are %s" % distinct['values'])
        
        
        output.append("#8.3 collection names ")
        output.append(db.collection_names())
        
        
        output.append("#8.3 Database stats")
        output.append(db.command("dbstats"))
        
        output.append("#8.4 Collection stats")
        output.append(db.command("collstats", collectionName))
        
        
        output.append("\n#9 Drop a collection")
        db.drop_collection(collectionName)
        db.drop_collection(joinCollectionName)
        db.drop_collection(cityTableName)
        db.drop_collection(codeTableName)
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))
    finally:
        if client is not None:
            client.close()
            output.append("Connection to database has been closed")

    return output

@app.route("/")
def displayPage():
    return render_template('index.html')

@app.route("/databasetest")
def runSample():
    output = []
    try:
        output = doEverything()
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))
    return render_template('tests.html', output=output)

if (__name__ == "__main__"):
    app.run(host='0.0.0.0', port=port)
