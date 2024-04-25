import sqlite3
import time
from colorama import Fore
import pymongo
from progress.bar import Bar






def add_index(mongodb_name : str = "mongo-db", collection : str = "") -> None:
    """#### Adds index for a specified collection to navigate easily through data
    ---------------------
    
    ##### Note : 
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    mongodb_name : The name of the mongodb database relative to this script
    collection : The name of the collection that needs to be indexed

    ---------------------
    #### @returns

    None
    """

    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client[mongodb_name]

    keys = list(db[collection].find_one({}, {"_id": 0}).keys())
    index_keys = list(map(lambda a: pymongo.IndexModel(a), keys))
    print(f"Adding index for {collection}...")
    db[collection].create_indexes(index_keys)


def remplir_collection(dbpath : str = "database.db", nomtable : str = "", batch_size : int = 1000) -> None:
    """#### Puts all data from a database table in a collection of a mongodb database of the same name
    ---------------------
    
    ##### Note : 
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    dbpath : The name of the database file relative to this script
    nomtable : The name of the table that needs to be converted in collection
    batch_size : The size of the batch for continuous requests to prevent memory overflow

    ---------------------
    #### @returns

    None
    """

    # pymongo connection
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    dbmongo = client["mongo-db"]
    dbmongo.drop_collection(nomtable)   # Erase collection with the same name if it already exists and rewrite over it

    # sqlite connection
    consqlite = sqlite3.connect(dbpath)
    csqlite = consqlite.cursor()


    # Gets all the informations of one table including its attributes which for every line is its 2nd index (1st one being its own index in the info table)
    csqlite.execute(f"PRAGMA table_info({nomtable})")
    caracs = csqlite.fetchall()
    champs = []

    # Getting all the attributes names of the selected table
    for line in caracs:
       champs.append(line[1])
   
    

    # Get the number of lines for current table
    csqlite.execute(f"SELECT COUNT(*) FROM {nomtable}")
    nb_lines = csqlite.fetchone()[0]


    # Mongodb collection
    collection = dbmongo[nomtable]

    
    batch = []  # List of the data for the current batch, its size should not go beyond the "batch_size" variable
    
    request = csqlite.execute(f"SELECT * FROM {nomtable}")

    #Progress bar when treating the request
    print(Fore.YELLOW) #Display console in yellow
    with Bar(f'Processing {nomtable} with a batch of {batch_size}...', max=nb_lines) as bar:
        # Go through the table line by line 
        for curr_row_i,row in enumerate(request):
            row_dict = {}
            for (i, value) in enumerate(row):   # Go through every attribute in the current row
                row_dict[champs[i]] = value # Get in a dict every value of a table row
            batch.append(row_dict)   #Update the current batch by adding the row

            if (curr_row_i + 1) % batch_size == 0: #If batch size reached, push and start another one
                collection.insert_many(batch)
                batch.clear()
            
            bar.next()
                    
        # Insert remaining values at the end of for loop
        if batch:
            collection.insert_many(batch)
            batch.clear()
        bar.finish()

    csqlite.close()
    consqlite.close()
    client.close()


def remplir_bdd(dbpath : str = "database.db", batch_size : int = 1000) -> None:
   """#### Transfers data from a database file to a mongodb database
    ---------------------
    
    ##### Note : 
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    dbpath : The name of the database file relative to this script
    batch_size : The size of the batch for continuous requests to prevent memory overflow

    ---------------------
    #### @returns

    None
    """
   consqlite = sqlite3.connect(dbpath)
   csqlite = consqlite.cursor()

   #Gets the name of the tables of the db one by one
   csqlite.execute("SELECT name FROM sqlite_master WHERE type='table';")
   tables = csqlite.fetchall()
   
   for table in tables:
       # table[0] is the name of the current table being treated
       remplir_collection(dbpath, table[0], batch_size)
       add_index("mongo-db",table[0])



start_time = time.time()

remplir_bdd()
print("Task took "+ str(time.time() - start_time) +" s")