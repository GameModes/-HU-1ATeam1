import pymongo, psycopg2, csv

def remove_commas(entry):
    return str(entry.replace(",", ""))

client = pymongo.MongoClient("mongodb://localhost:27017/")
mongoDB = client["huwebshop"]

hostname = 'localhost'
username = 'postgres'
password = 'Floris09'
database = 'huwebshop'
conn = psycopg2.connect(host=hostname, user=username, password=password, database=database)
cur = conn.cursor()

with open('products.csv', 'w', newline='') as prods, open('categories.csv', 'w', newline='') as cats, open('genders.csv', 'w', newline='') as gends, open('doelgroepen.csv', 'w', newline='') as cats:
    prods_fieldnames = ['id', 'category_id']
    categories_fieldnames = ['id', 'category']
    prod_writer = csv.DictWriter(prods, fieldnames=prods_fieldnames)
    categorie_writer = csv.DictWriter(cats, fieldnames=categories_fieldnames)
    prod_writer.writeheader()
    categorie_writer.writeheader()
    c = 0
    cats_dict = {}
    for product in mongoDB.products.find():
        prod_id = remove_commas(str(product["_id"]))
        cat = remove_commas(str(product.get("category", None)))
        if cat not in cats_dict:
            if len(cats_dict) > 0:
                cats_dict[cat] = max(cats_dict.values())+1
            else:
                cats_dict[cat] = 1
            categorie_writer.writerow(
                {
                    'id': cats_dict[cat],
                    'category': cat
                }
            )
        cat_id = cats_dict[cat]
        prod_writer.writerow(
            {
                'id': prod_id,
                'category_id': cat_id
            }
        )
        c += 1
        if c % 10000 == 0:
            print("{} product records written...".format(c))
    print("Finished creating the product database contents.")



# with open('products.csv', 'w', newline='') as csvout:
#     fieldnames = ['id', 'category', 'subcategory', 'subsubcategory']
#     writer = csv.DictWriter(csvout, fieldnames=fieldnames)
#     writer.writeheader()
#     c = 0
#     for product in mongoDB.products.find():
#         writer.writerow({'id': product["_id"],
#                          'category': remove_commas(str(product.get("category", None))),
#                          'subcategory': remove_commas(str(product.get("sub_category", None))),
#                          'subsubcategory': remove_commas(str(product.get("sub_sub_category", None)))
#                          })
#         c += 1
#         if c % 10000 == 0:
#             print("{} product records written...".format(c))
# print("Finished creating the product database contents.")
#
with open('products.csv', 'r') as prods, open('categories.csv', 'r') as cats:
    next(prods)
    cur.copy_from(prods, 'products', sep=',')
    next(cats)
    cur.copy_from(cats, 'categories', sep=',')
    conn.commit()
print("Check postgres")

