import pandas as pd
import pymongo
import sqlite3
from progress.bar import Bar
import csv
from colorama import Style

#connexion sqlite
consqlite = sqlite3.connect('database.db')
csqlite = consqlite.cursor()

#connexion pymongo à mongoDB
client = pymongo.MongoClient('127.0.0.1', 27017)
db = client["mongo-db"]

def file_len(fname):
    with open(fname , encoding='utf-8') as f:
        for i, l in enumerate(f, 1):
            pass
    return i


# Liste des noms de tables
table_names = [
    'Movies',
    'Persons',
    'Characters',
    'Directors',
    'Genres',
    'Knownformovies',
    'Principals',
    'Professions',
    'Ratings',
    'Titles',
    'Writers'
]

batch_size = 1000

for table_name in table_names:
    collection = db[table_name]

    with open("imdb-medium/"+str(table_name).lower()+".csv", encoding='utf-8') as csvfile:
        r = csv.reader(csvfile, delimiter=',')
        batch = []
        with Bar(f'Processing {table_name} with a batch of {batch_size}...', max=(file_len("imdb-medium/"+str(table_name).lower()+".csv")/batch_size)+1) as bar:
        # Itération à travers le fichier CSV par lots
            for chunk in pd.read_csv(csvfile, chunksize=batch_size):
                # Conversion des données en dictionnaires pour insertion dans MongoDB
                donnees_a_inserer = chunk.to_dict(orient='records')
                # Insertion des données dans la collection MongoDB
                collection.insert_many(donnees_a_inserer)
                bar.next()    
        
    print(Style.RESET_ALL)



client.close()