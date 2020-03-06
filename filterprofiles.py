import pymongo
import psycopg2
import json

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["huwebshop"]
mycol = mydb["visitors"]
hostname = 'localhost'
username = 'postgres'
password = 'Floris09'
database = 'huwebshop'


all_products = mycol.find()

suc = 0
fail = 0


myConnection = psycopg2.connect(host=hostname, user=username, password=password, database=database)

allvars = []

for x in all_products:
    try:
        print(x["buids"])
    except:
        continue
    # try:
    #     var = x["properties"]["variant"]
    #     if var in allvars:
    #         allvars[var] = allvars[var] + 1
    #     else:
    #         allvars[var] = 1
    # except KeyError:
    #     continue
    # _id = x["_id"]
    # name = x["name"]
    # price = x["price"]["selling_price"]
    # if doQuery(myConnection, _id, name, price):
    #     suc += 1
    # else:
    #     fail += 1

# print(f"{suc} / {len(all_products)} gelukt")
# print(f"{fail} / {len(all_products)} gefaald")