import sqlite3
import csv
from progress.bar import Bar
import time
from colorama import Fore, Style
import sys

"""
def rm(nom_fichier):
    try:
        os.remove(nom_fichier)
        print(f"Le fichier {nom_fichier} a été supprimé avec succès.")
    except FileNotFoundError:
        print(f"Le fichier {nom_fichier} n'existe pas.")
    except Exception as e:
        print(f"Une erreur s'est produite lors de la suppression du fichier {nom_fichier}: {e}")
if(str(sys.argv[1]) != 'sql'):
    rm('database.db')
"""
def file_len(fname):
    with open(fname , encoding='utf-8') as f:
        for i, l in enumerate(f, 1):
            pass
    return i




start_time = time.time()


# Liste des tables
listoftable = [
    "CREATE TABLE Movies (mid TEXT, titleType TEXT, primaryTitle TEXT, orginialTitle TEXT, isAdult INT, startYear INT, endYear INT, runtimeMinutes INT, PRIMARY KEY(mid))",
    "CREATE TABLE Persons (pid TEXT, primaryName TEXT, birthYear INT, deathYear INT)",
    "CREATE TABLE Genres (mid TEXT, genre TEXT, PRIMARY KEY(mid, genre))",
    "CREATE TABLE Directors (mid TEXT, pid TEXT, PRIMARY KEY(mid, pid))",
    "CREATE TABLE Knownformovies (pid TEXT, mid TEXT, PRIMARY KEY(pid, mid))",
    "CREATE TABLE Principals (mid TEXT, ordering INT, pid TEXT, category TEXT, job TEXT, PRIMARY KEY(mid, ordering, pid))",
    "CREATE TABLE Professions (pid TEXT, jobName TEXT, PRIMARY KEY(pid, jobName))",
    "CREATE TABLE Ratings (mid TEXT, averageRating TEXT, numVotes INT, PRIMARY KEY(mid))",
    "CREATE TABLE Titles (mid TEXT, ordering INT, title TEXT, region TEXT, language TEXT, types TEXT, attributes TEXT, isOriginalTitle INT, PRIMARY KEY(mid, ordering))",
    "CREATE TABLE Writers (mid TEXT, pid TEXT, PRIMARY KEY(mid, pid))",
    "CREATE TABLE Characters (mid TEXT, pid TEXT, name TEXT)"
]

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

#Creation des tables dans la db
def create_tables(db_path):
    conn = sqlite3.connect(db_path)
    
    if(len(sys.argv[1])!=0):
        if(str(sys.argv[1]) != 'sql'):
            # Génération des tables
            for table in listoftable:
                conn.execute(table)
        
    conn.commit()
    conn.close()

#Insérer les données dans la table
def insert_table(db_path, attributes, path, batch_size, table_name):
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print(Fore.YELLOW)
    with open(path, encoding='utf-8') as csvfile:
        r = csv.reader(csvfile, delimiter=',')
        next(r)  # Skip header
        batch = []
        with Bar(f'Processing {table_name} with a batch of {batch_size}...', max=(file_len(path)/batch_size)+1) as bar:
            for i, row in enumerate(r):
                batch.append(row)
                if (i + 1) % batch_size == 0: #If batch size reached, do another batch after pushing
                    cur.executemany(f"INSERT INTO {table_name} VALUES ({attributes})", batch)
                    batch.clear()
                    bar.next()
            # insertion de la fin des valeurs 
            if batch:
                cur.executemany(f"INSERT INTO {table_name} VALUES ({attributes})", batch)
                batch.clear()
                bar.next()


    print(Style.RESET_ALL)
    conn.commit()
    conn.close()

#Fonction pour avoir le nb de colonnes d'une table
def getTableColumns(db_size,table_name):
    attribute = ""

    with open(str(db_size) + "/" + table_name.lower() + ".csv", "r", newline = "") as file:
        reader = csv.reader(file)
        l = len(next(reader))
        file.close()

    for i in range(l):
        if(i == l-1):
            attribute += "?"
        else:
            attribute += "?,"

    return attribute


#Création des tables
create_tables("database.db")


#Vérification lors du lancement du script s'il y a le batch_size, sinon met 1000 par défaut
if (sys.argv[1] != 0 and str(sys.argv[1]) !='sql'):
    batch_size = int(sys.argv[1])
elif(len(sys.argv[1]) == 0):
    batch_size = 1000


# Appel à la fonction insert_table pour chaque fichier
for table_name in table_names:
    insert_table("database.db", getTableColumns("imdb-medium",table_name), 'imdb-medium/'+table_name.lower()+'.csv', batch_size, table_name)


print("Task took : " , time.time() - start_time ,"s")

"""
#REQUETES SQL (question 1 à 5)

def sql_request(db_path,request):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(request)

    conn.commit()
    conn.close()

print("Requête question 1 : ")
sql_request(cur,"SELECT Movies.primaryTitle FROM Movies JOIN Principals ON Movies.mid = Principals.mid JOIN Persons ON Principals.pid = Persons.pid WHERE Persons.primaryName = 'Jean Reno';")
print("Requête question 2 : ")
sql_request(cur, "SELECT primaryTitle, averageRating FROM Movies JOIN Genres ON Movies.mid = Genres.mid JOIN Ratings ON Movies.mid = Ratings.mid WHERE startYear = 2000 AND startYear <= 2010 AND titleType = 'movie' AND genre = 'Horror' ORDER BY averageRating DESC limit 3;")
print("Requête question 3 : ")
sql_request(cur, "SELECT DISTINCT Persons.primaryName FROM Persons JOIN Writers ON Persons.pid = Writers.pid JOIN Titles ON Writers.mid = Titles.mid WHERE Titles.region != 'ES';")
print("Requête question 4 : ")
sql_request(cur, "SELECT Persons.primaryName, COUNT(*) AS nombreDeRoles FROM Persons JOIN Principals ON Persons.pid = Principals.pid GROUP BY Persons.pid ORDER BY nombreDeRoles DESC LIMIT 1;")
print("Requête question 5 : ")
sql_request(cur, "")
"""




