import sqlite3
import pymongo

#connexion sqlite
consqlite = sqlite3.connect('../database.db')
csqlite = consqlite.cursor()

#connexion pymongo
con = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
dbmongo = con



def remplir_collection(nomtable):

   # récupération des champs de la table sqlite

   csqlite.execute(f"PRAGMA table_info({nomtable})")
   table = csqlite.fetchall()
   listechamp = []

   for ligne in table:
       listechamp.append(ligne[1])
   
   nbchamp = len(listechamp)

   #création d'une collection mongodb
   
   collection = dbmongo[nomtable]

   #curseur sur les données de la table
   csqlite.execute(f"SELECT * FROM {nomtable}")

   while True:
       
       resultats = csqlite.fetchmany(100)

       if not resultats:
           break


       for resultat in resultats:
           i = 0
           Infochar = {}
           for champ in listechamp:
               Infochar[champ] = resultat[i]
               i = i+1
           
           
           collection.insert_one(Infochar)

def remplir_bdd():
   csqlite.execute("SELECT name FROM sqlite_master WHERE type='table';")
   tables = csqlite.fetchall()
   
   for table in tables:
       remplir_collection(table[0])


remplir_bdd()

consqlite.close()
con.close()

