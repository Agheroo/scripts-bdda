import sqlite3
import csv
from progress.bar import Bar
import time
from colorama import Fore, Style
import sys


def create_tables(db_name: str = "database.db", tables: list =  [""]):
    """Creates the database
    ---------------------
    
    Note : The file needs to be non-existant before running this script, it will not rewrite over a file with the same name

    -----------------------

    @params

    db_name : The path of the database relative to the script
    tables : The "CREATE TABLE" list of all the tables of the concerned database
    """

    #Connection to the DB with sqlite
    conn = sqlite3.connect(db_name)

    if(len(sys.argv[1])!=0):
        if(str(sys.argv[1]) != 'sql'):
            # Génération des tables avec la liste définie avant
            for table in tables:
                conn.execute(table)
        
    conn.commit()
    conn.close()

def insert_table(db_path: str = "database.db", attributes : str = "", csv_path : str = "", batch_size : int = 1000, table_name : str = ""):
    """Reads a csv file to insert its data into a database table with the same name
    ---------------------
    
    Note : The attributes are using the "?, ?, ?, ... , ?" string format for SQL 'executemany' method

    ---------------------
    @params

    db_path : The path of the database relative to the script
    attributes : The number of attributes of a table (using the "?, ?, ?, ... , ?" string format for SQL 'executemany' method)
    csv_path : The path of the csv file that needs to be written in the database
    batch_size : The size of the batch for continuous requests to prevent memory overflow
    table_name : The name of the table that needs to be read

    ---------------------
    @returns

    None
    """
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    
    #Pre read the file once to get the number of lines without the header
    with open(csv_path, 'r', errors = "ignore") as csvfile: 
        nb_lines = len(csvfile.readlines())
    nb_lines -= 1

    with open(csv_path, encoding='utf-8', errors = "ignore") as csvfile:
        r = csv.reader(csvfile, delimiter=',')
        next(r)  # Skip header of the file (names of columns etc)

        batch = []  # List of the data for the current batch, its size should not go beyond the "batch_size" variable

        #Progress bar when treating the request
        print(Fore.YELLOW) #Display console in yellow
        with Bar(f'Processing {table_name} with a batch of {batch_size}...', max=nb_lines) as bar:
            for (i, row) in enumerate(r):
                batch.append(row)   #Update the current batch by adding the row
                
                if (i + 1) % batch_size == 0: #If batch size reached, push and start another one
                    cur.executemany(f"INSERT INTO {table_name} VALUES ({attributes})", batch)
                    batch.clear()
                
                bar.next()
                
            # Insert remaining values at the end of for loop
            if batch:
                cur.executemany(f"INSERT INTO {table_name} VALUES ({attributes})", batch)
                batch.clear()
            bar.finish()
        csvfile.close()

    print(Style.RESET_ALL)
    conn.commit()
    conn.close()

def get_attributes(db_folder: str = "imdb-tiny",table_name: str = "") -> str:
    """Gets all the attributes of a table in a csv file
    ---------------------
    
    Note : The attributes are using the "?, ?, ?, ... , ?" string format for SQL 'executemany' method

    All the CSV files should be in a folder and this script should be out of this folder i.e.  \n

    ... \n
    | \n
    create_db.py \n
    db_folder \n
        |   table1.csv\n
        |   table2.csv \n
        |   ...

    
    ---------------------
    @params

    db_folder : The folder in which all the CSV files are located
    table_name : The name of the current CSV file to get its attributes (the files all need to be in lowercase characters)
    """ 

    attribute = ""
    with open(str(db_folder) + "/" + table_name.lower() + ".csv", "r", newline = "", errors="ignore") as file:
        reader = csv.reader(file)
        l = len(next(reader))
        file.close()

    for i in range(l):
        if(i == l-1):
            attribute += "?"
        else:
            attribute += "?,"

    return attribute

# Création des index pour le comparatif des performances
conn = sqlite3.connect('database.db')
cursor = conn.cursor()
cursor.execute("CREATE INDEX IF NOT EXISTS idx_mid ON Movies(mid)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_pid ON Persons(pid)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_mid_genre ON Genres(mid, genre)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_mid_pid ON Directors(mid, pid)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Knownformovies(pid, mid)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Principals(mid, ordering, pid)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Professions(pid, jobName)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Ratings(mid)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Titles(mid, ordering)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Writers(mid, pid)")
cursor.execute("CREATE INDEX IF NOT EXISTS index ON Characters(mid, pid)")
conn.commit()
conn.close()

# Création des tables par SQL (dans une liste python pour faciliter la lecture)
list_sql_creates = [
    "CREATE TABLE Movies (mid TEXT, titleType TEXT, primaryTitle TEXT, orginialTitle TEXT, isAdult INT, startYear INT, endYear INT, runtimeMinutes INT, PRIMARY KEY(mid))",
    "CREATE TABLE Persons (pid TEXT, primaryName TEXT, birthYear INT, deathYear INT, PRIMARY KEY(pid))",
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

# Liste des noms de tables nommées précédemment
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

# Ce script se lance avec un argument dans la console : c'est la taille du batch
# Vérification lors du lancement du script s'il y a le batch_size, sinon met 1000 par défaut
if (sys.argv[1] != 0 and str(sys.argv[1]) !='sql'):
    batch_size = int(sys.argv[1])
elif(len(sys.argv[1]) == 0):
    batch_size = 1000



start_time = time.time()

# Création des tables
create_tables("database.db", list_sql_creates)

# Insertion des données dans les tables créées pour la DB 
# Appel à la fonction insert_table pour chaque fichier
for table_name in table_names:
    insert_table("database.db", get_attributes("imdb-tiny",table_name), 'imdb-tiny/'+table_name.lower()+'.csv', batch_size, table_name)


# Affichage du temps pris pour écrire la DB entière
print("Task took : " , time.time() - start_time ,"s")


