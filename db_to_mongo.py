import sqlite3
import time
from colorama import Fore
import pymongo
from progress.bar import Bar








def remplir_collection(dbpath : str = "database.db", mongodb_name : str = "mongo-db", nomtable : str = "", batch_size : int = 1000) -> None:
    """ Fills a mongodb collection with a given database file of the same table name
    ---------------------
    
    Note : The client is created with the default localhost/port usage for mongodb

    ---------------------
    @params
    --

    mongodb_name : The name of the mongodb database to connect to
    dbpath : The path of the database relative to the script
    nomtable : The name of the table that the data needs to be read from
    batch_size : The size of the batch for continuous requests to prevent memory overflow

    ---------------------
    @returns
    --

    None
    """
    
    # pymongo connection
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    dbmongo = client[mongodb_name]

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
        for row_index,row in enumerate(request):
            row_dict = {}
            for (i, value) in enumerate(row):   # Go through every attribute in the current row
                row_dict[champs[i]] = value # Get in a dict every value of a table row
            batch.append(row_dict)   #Update the current batch by adding the row

            if (row_index + 1) % batch_size == 0: #If batch size reached, push and start another one
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


def remplir_bdd(dbpath : str = "database.db", mongodb_name : str = "mongo-db", batch_size : int = 1000) -> None:
   """ Fills a mongodb database with a given database file
    ---------------------
    
    Note : The client is created with the default localhost/port usage for mongodb

    ---------------------
    @params
    --

    mongodb_name : The name of the mongodb database to connect to
    dbpath : The path of the database relative to the script
    batch_size : The size of the batch for continuous requests to prevent memory overflow

    ---------------------
    @returns
    --

    None
   
   """
   consqlite = sqlite3.connect(dbpath)
   csqlite = consqlite.cursor()

   #Gets the name of the tables of the db one by one
   csqlite.execute("SELECT name FROM sqlite_master WHERE type='table';")
   tables = csqlite.fetchall()
   
   for table in tables:
       # table[0] is the name of the current table being treated
       remplir_collection(dbpath, mongodb_name ,table[0], batch_size)



start_time = time.time()

remplir_bdd("database.db","mongo-db",1000)
print("Task took "+ time.time() - start_time +" s")