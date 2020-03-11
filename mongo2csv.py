import pymongo, psycopg2, csv
from playsound import playsound

def remove_commas(entry):
    return str(entry.replace(",", ""))

client = pymongo.MongoClient("mongodb://localhost:27017/")
mongoDB = client["huwebshop"]

hostname = 'localhost'
username = 'postgres'
password = 'Floris09'
database = 'huws'
conn = psycopg2.connect(host=hostname, user=username, password=password, database=database)
cur = conn.cursor()

cur.execute('''
        --Er wordt gebruik maken drop ... CASCADE , want er zitten constraints van de tabellen die afhankelijk is aan andere tabellen 
        drop table if exists products CASCADE ;
        drop table if exists categories;
        drop table if exists brands;
        drop table if exists doelgroepen;
        
        drop table if exists profiles CASCADE ;
  
        
        drop table if exists sessions CASCADE ;
        drop table if exists orders;-- produc_sessions
        ''')
conn.commit()
print("tables dropped")
cur.execute('''
       --Products
        CREATE TABLE products(
            id varchar,
            name varchar,
            price int,
            categoriesID int,
            doelgroepenID int,
            brandsID int,
            variants int,
	        discount int
        );
        CREATE TABLE categories(
            id int,
            category varchar,
            sub_category varchar,
            sub_sub_category varchar
        );
        CREATE TABLE brands(
            id int,
            brand varchar
        );
        CREATE TABLE doelgroepen(
            id int,
            doelgroep varchar,
            genders   varchar 
        );
        
        --Profiles
       CREATE TABLE profiles(
            id varchar,
            buids text[],
            segment varchar,
            similars text[]
        );
        
        
       --Sessions
        CREATE TABLE sessions(
            id varchar,
            profilesID varchar,
            session_start date,
            session_end date,
            buid varchar,
            has_sale int 
        );
        
        --Koppeltabel van products en sessions
        CREATE TABLE orders(
            productsID varchar,
            sessionsID varchar,
            count int 
        );
        
        --Define primary keys (6x)
        
        ALTER Table products
        ADD PRIMARY KEY (id);
        
        ALTER TABLE categories
        ADD PRIMARY KEY (id);
        
        ALTER TABLE brands
        ADD PRIMARY KEY (id);
        
        ALTER TABLE doelgroepen
        ADD PRIMARY KEY (id);
        
        ALTER TABLE profiles
        ADD PRIMARY KEY (id);
        
        ALTER TABLE sessions
        ADD PRIMARY KEY (id);
        
        -- Define foreign keys
        
        -- products
        ALTER TABLE products
        ADD FOREIGN KEY (categoriesID) REFERENCES categories(id);
        
        ALTER TABLE products
        ADD FOREIGN KEY (doelgroepenID) REFERENCES doelgroepen(id);
        
        ALTER TABLE products
        ADD FOREIGN KEY (brandsID) REFERENCES brands(id);
        
       
        
        --sessions
        ALTER TABLE  sessions
        ADD FOREIGN KEY (profilesID) REFERENCES  profiles(id);
        
        --orders
        ALTER TABLE orders
        ADD CONSTRAINT FK_Products_Sessions
        FOREIGN KEY (productsID) REFERENCES products(id);
        
        ALTER TABLE orders
        ADD CONSTRAINT FK_Sessions_Products
        FOREIGN KEY (sessionsID) REFERENCES sessions(id);
        ''')
conn.commit()
print("tables made")

# Profiles
with open('profiles.csv', 'w', newline='') as profs:
    profs_fieldnames = ['id', 'buids', 'previously_recommended', 'segment', 'viewed_before', 'similars']
    prof_writer = csv.DictWriter(profs, fieldnames=profs_fieldnames, quotechar="'", delimiter=";")
    prof_writer.writeheader()
    c = 0
    for profile in mongoDB.visitors.find():
        try:
            buids_list = profile["buids"]
            buids = ','.join(buids_list)
            if ";" not in buids:
                buids = "{" + buids + "}"
                previously_recommended_list = profile.get("previously_recommended", None)
                previously_recommended = ','.join(previously_recommended_list)
                previously_recommended = "{" + previously_recommended + "}"
                viewed_before_list = profile["recommendations"].get("viewed_before", None)
                viewed_before = ','.join(viewed_before_list)
                viewed_before = "{" + viewed_before + "}"
                similars_list = profile["recommendations"].get("similars", None)
                similars = ','.join(similars_list)
                similars = "{" + similars + "}"
                prof_writer.writerow(
                    {
                        'id': profile["_id"],
                        'buids': buids,
                        'previously_recommended': previously_recommended,
                        'segment': profile["recommendations"]["segment"],
                        'viewed_before': viewed_before,
                        'similars': similars
                    }
                )
                c += 1
        except:
            continue
        if c % 100000 == 0:
            print("{} profiles records written...".format(c))
    print(f"Finished creating the product database contents. {c} profiles loaded.")

# Sessions
with open('sessions.csv', 'w', newline='') as sess:
    sess_fieldnames = ['id', 'session_start', 'session_end', 'buid', 'has_sale']
    sess_writer = csv.DictWriter(sess, fieldnames=sess_fieldnames)
    sess_writer.writeheader()
    c = 0
    for session in mongoDB.sessions.find():
        try:
            sess_writer.writerow(
                {
                    'id': session["_id"],
                    'session_start': session["session_start"],
                    'session_end': session["session_end"],
                    'has_sale': session["has_sale"],
                    'buid': session["buid"][0]
                }
            )
            c += 1
            if c % 100000 == 0:
                print("{} session records written...".format(c))
        except:
            continue
    print(f"Finished creating the product database contents. {c} sessions loaded.")


# Products
with open('products.csv', 'w', newline='') as prods, open('categories.csv', 'w', newline='') as cats, open('discounts.csv', 'w', newline='') as discs, open('brands.csv', 'w', newline='') as brands, open('variants.csv', 'w', newline='') as vars, open('doelgroepen.csv', 'w', newline='') as doels, open('genders.csv', 'w', newline='') as gends :
    prods_fieldnames = ['id', 'name', 'price', 'category_id', 'discount_id', 'brand_id', 'variant_id', 'doelgroep_id', 'gender_id']
    cats_fieldnames = ['id', 'category', 'sub_category', 'sub_sub_category']
    discs_fieldnames = ['id', 'discount']
    brands_fieldnames = ['id', 'brand']
    vars_fieldnames = ['id', 'variant']
    doels_fieldnames = ['id', 'doelgroep']
    gends_fieldnames = ['id', 'gender']
    prod_writer = csv.DictWriter(prods, fieldnames=prods_fieldnames)
    cats_writer = csv.DictWriter(cats, fieldnames=cats_fieldnames)
    discs_writer = csv.DictWriter(discs, fieldnames=discs_fieldnames)
    brand_writer = csv.DictWriter(brands, fieldnames=brands_fieldnames)
    var_writer = csv.DictWriter(vars, fieldnames=vars_fieldnames)
    doel_writer = csv.DictWriter(doels, fieldnames=doels_fieldnames)
    gend_writer = csv.DictWriter(gends, fieldnames=gends_fieldnames)
    prod_writer.writeheader()
    cats_writer.writeheader()
    discs_writer.writeheader()
    brand_writer.writeheader()
    var_writer.writeheader()
    doel_writer.writeheader()
    gend_writer.writeheader()
    c = 0
    cats_dict = {}
    disc_dict = {}
    brands_dict = {}
    vars_dict = {}
    doels_dict = {}
    gends_dict = {}
    for product in mongoDB.products.find():
        try:
            prod_id = remove_commas(str(product["_id"]))
            cat = remove_commas(str(product.get("category", None)))
            sub_cat = remove_commas(str(product.get("sub_category", None)))
            sub_sub_cat = remove_commas(str(product.get("sub_sub_category", None)))
            disc = remove_commas(str(product["properties"].get("discount", None)))
            brand = remove_commas(str(product.get("brand", None)))
            var = remove_commas(str(product["properties"].get("variant", None)))
            doel = remove_commas(str(product["properties"].get("doelgroep", None)))
            gend = remove_commas(str(product.get("gender", None)))
            cat_search = cat + sub_cat + sub_sub_cat
            if cat_search not in cats_dict:
                if len(cats_dict) > 0:
                    cats_dict[cat_search] = max(cats_dict.values())+1
                else:
                    cats_dict[cat_search] = 1
                cats_writer.writerow(
                    {
                        'id': cats_dict[cat_search],
                        'category': cat,
                        'sub_category': sub_cat,
                        'sub_sub_category': sub_sub_cat
                    }
                )
            cat_id = cats_dict[cat_search]

            if disc not in disc_dict:
                if len(disc_dict) > 0:
                    disc_dict[disc] = max(disc_dict.values()) + 1
                else:
                    disc_dict[disc] = 1
                discs_writer.writerow(
                    {
                        'id': disc_dict[disc],
                        'discount': disc
                    }
                )
            disc_id = disc_dict[disc]

            if brand not in brands_dict:
                if len(brands_dict) > 0:
                    brands_dict[brand] = max(brands_dict.values()) + 1
                else:
                    brands_dict[brand] = 1
                brand_writer.writerow(
                    {
                        'id': brands_dict[brand],
                        'brand': brand
                    }
                )
            brand_id = brands_dict[brand]

            if var not in vars_dict:
                if len(vars_dict) > 0:
                    vars_dict[var] = max(vars_dict.values()) + 1
                else:
                    vars_dict[var] = 1
                var_writer.writerow(
                    {
                        'id': vars_dict[var],
                        'variant': var
                    }
                )
            var_id = vars_dict[var]

            if doel not in doels_dict:
                if len(doels_dict) > 0:
                    doels_dict[doel] = max(doels_dict.values()) + 1
                else:
                    doels_dict[doel] = 1
                doel_writer.writerow(
                    {
                        'id': doels_dict[doel],
                        'doelgroep': doel
                    }
                )
            doel_id = doels_dict[doel]

            if gend not in gends_dict:
                if len(gends_dict) > 0:
                    gends_dict[gend] = max(gends_dict.values()) + 1
                else:
                    gends_dict[gend] = 1
                gend_writer.writerow(
                    {
                        'id': gends_dict[gend],
                        'gender': gend
                    }
                )
            gend_id = gends_dict[gend]

            if "." not in str(product["price"]["selling_price"]):
                try:
                    prod_writer.writerow(
                        {
                            'id': prod_id,
                            'name': remove_commas(str(product.get("name", None))),
                            'price': product["price"]["selling_price"],
                            'category_id': cat_id,
                            'discount_id': disc_id,
                            'brand_id': brand_id,
                            'variant_id': var_id,
                            'doelgroep_id': doel_id,
                            'gender_id': gend_id
                        }
                    )
                    c += 1
                except:
                    continue
            if c % 10000 == 0:
                print("{} product records written...".format(c))
        except:
            print(prod_id)
    print(f"Finished creating the product database contents. {c} rows loaded.")



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

# Profiles
with open('profiles.csv', 'r') as profs:
    next(profs)
    cur.copy_from(profs, 'profiles', sep=';')
    conn.commit()
print("Profiles copied!")

# Sessions
with open('sessions.csv', 'r') as sess:
    next(sess)
    cur.copy_from(sess, 'sessions', sep=',')
    conn.commit()
print("Sessions copied!")


# Products
with open('products.csv', 'r') as prods, open('categories.csv', 'r') as cats, open('discounts.csv', 'r') as discs, open('brands.csv', 'r') as brands, open('variants.csv', 'r') as vars, open('doelgroepen.csv', 'r') as doels, open('genders.csv', 'r') as gends:
    next(prods)
    cur.copy_from(prods, 'products', sep=',')
    next(cats)
    cur.copy_from(cats, 'categories', sep=',')
    next(discs)
    cur.copy_from(discs, 'discounts', sep=',')
    next(brands)
    cur.copy_from(brands, 'brands', sep=',')
    next(vars)
    cur.copy_from(vars, 'variants', sep=',')
    next(doels)
    cur.copy_from(doels, 'doelgroepen', sep=',')
    next(gends)
    cur.copy_from(gends, 'genders', sep=',')
    conn.commit()
print("Products copied!")

# cur.execute("""INSERT INTO prosess
# (profile_id, session_id, buid)
# SELECT profiles.id, sessions.id, sessions.buid
# FROM sessions
# INNER JOIN profiles ON sessions.buid= ANY(profiles.buids);""")
# conn.commit()
conn.close()

print("Check postgres")
playsound('C:/Users/Floris Videler/Downloads/Ding Sound Effect.mp3')

