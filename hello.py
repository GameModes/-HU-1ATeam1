import pymongo
import psycopg2
import json

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["huwebshop"]
mycol = mydb["products"]
hostname = 'localhost'
username = 'postgres'
password = 'Floris09'
database = 'huwebshop'


all_products = mycol.find()

suc = 0
fail = 0

def doQuery(conn, _id, name, price):
    cur = conn.cursor()
    try:
        cur.execute(f"""INSERT INTO public.products (_id, product_name, price)
                        VALUES ('{json.dumps(_id)}', '{json.dumps(name)}', {json.dumps(price)});""")
        conn.commit()
        return True
    except:
        print(f"{name}")
        return False


myConnection = psycopg2.connect(host=hostname, user=username, password=password, database=database)


for x in all_products:
    _id = x["_id"]
    name = x["name"]
    price = x["price"]["selling_price"]
    if doQuery(myConnection, _id, name, price):
        suc += 1
    else:
        fail += 1

print(f"{suc} / {len(all_products)} gelukt")
print(f"{fail} / {len(all_products)} gefaald")