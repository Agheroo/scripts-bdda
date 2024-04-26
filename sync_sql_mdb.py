from ctypes import *
from threading import Lock, Thread
import sqlite3
import pymongo
from colorama import Fore
# Define some symbols
SQLITE_DELETE =  9
SQLITE_INSERT = 18
SQLITE_UPDATE = 23


# Function that maps proper ids of tables with collections
apply_keys = lambda k, v : dict(map(lambda i, j : (i, j), k, v))

def delete_row(table_name: str, row_data: list[str], columns: list[str], sem: Lock, mongodb_name: str = "mongo-db") -> None:
    """#### Deletes a row in the mongodb
    ---------------------
    
    ##### Note : 
    - The table should have the same name as the collection used
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    table_name : The table to remove the row from
    row_data : The row to delete
    columns : The names of the columns of the table
    sem : The semaphore used for CLI sync
    mongodb_name : The MongoDB database file name

    ---------------------
    #### @returns

    None
    """
    #DEBUG PURPOSES
    print(Fore.BLUE)

    print("table_name : " + str(table_name))
    print("row_data : "+str(row_data))
    print("columns : " + str(columns))
    print("sem : " + str(sem))
    print("mdb_name : " + str(mongodb_name))
    print("operation : DELETE (9)")

    print(Fore.YELLOW)

    table_name = table_name.decode()
    client = pymongo.MongoClient("127.0.0.1",27017)
    mdb = client[mongodb_name]
    mdb[table_name].delete_one(apply_keys(columns, row_data))
    
    print(Fore.GREEN)
    print(f"Successfully deleted row in MongoDB '{table_name}' collection")
    print(Fore.RESET)
    client.close()
    sem.release()
    

def insert_row(table_name: str, row_id: int, sem: Lock, db_name : str, mongodb_name: str = "mongo-db") -> None:
    """#### Inserts a given row in the mongodb with its row_id
    ---------------------
    
    ##### Note : 
    - The table should have the same name as the collection used
    - The client is created with the default localhost/port usage for mongodb
    - The database path is relative to this script
    ---------------------
    #### @params

    table_name : The table to add the row in
    row_id : The new row id
    columns : The names of the columns of the table
    sem : The semaphore used for CLI sync
    db_name : The database file name
    mongodb_name : The MongoDB database file name

    ---------------------
    #### @returns

    None
    """
    client = pymongo.MongoClient("127.0.0.1",27017)


    #DEBUG PURPOSES
    print(Fore.BLUE)

    print("table_name : " + str(table_name))
    print("row_id : "+str(row_id))
    print("sem : " + str(sem))
    print("db_name :" + str(db_name))
    print("mdb_name : " + str(mongodb_name))
    print("operation : INSERT (18)")

    print(Fore.YELLOW)

    table_name = table_name.decode()

    mdb = client[mongodb_name]

    db = sqlite3.connect(db_name).cursor()

    # Search in SQLITE table the corresponding row
    cursor = db.execute(f"SELECT * FROM {table_name} WHERE rowid={row_id}")
    columns = [row[0] for row in cursor.description]
    row = cursor.fetchone()

    # Map the new row in the collecion
    mdb[table_name].insert_one(apply_keys(columns, row))

    print(Fore.GREEN)
    print(f"Successfully added row in MongoDB '{table_name}' collection")
    print(Fore.RESET)
    client.close()
    sem.release()

def update_row(table_name: str, row_id: int, row_data_before: list[str], columns: list[str], sem: Lock, db_name : str = "database.db", mongodb_name: str = "mongo-db") -> None:
    """#### Updates a row in the mongodb database
    ---------------------
    
    ##### Note : 
    - The table should have the same name as the collection used
    - The client is created with the default localhost/port usage for mongodb
    - The database path is relative to this script
    ---------------------
    #### @params
    
    table_name : The table in which to update its row
    row_id : The new row id to modify
    row_data_before : The row before being updated
    columns : The columns of the table
    sem : The semaphore used for CLI sync
    db_name : The database file name
    mongodb_name : The MongoDB database file name
    ---------------------
    #### @returns

    None
    """
    client = pymongo.MongoClient("127.0.0.1",27017)
    mdb = client[mongodb_name]

    #DEBUG PURPOSES
    print(Fore.BLUE)

    print("table_name : " + str(table_name))
    print("row_id : "+str(row_id))
    print("row_data_before : " + str(row_data_before))
    print("columns :" + str(columns))
    print("db_name : " + str(db_name))
    print("mongodb_name : " + str(mongodb_name))
    print("operation : UPDATE (23)")

    print(Fore.YELLOW)

    table_name = table_name.decode()
    db = sqlite3.connect(db_name).cursor()
    cursor = db.execute(f"select * from {table_name} where rowid={row_id}")
    row = cursor.fetchone()

    Fore.LIGHTBLUE_EX
    for e in row_data_before:
        print(e)
    for e in row:
        print(e)
    Fore.YELLOW

    mdb[table_name].find_one_and_replace(apply_keys(columns, row_data_before), apply_keys(columns, row))

    print(Fore.GREEN)
    print(f"Successfully update row in MongoDB '{table_name}' collection")
    print(Fore.RESET)
    client.close()
    sem.release()
    



def callback(user_data, operation, db_name, table_name, row_id) -> None:
    """#### Define our callback function

    ---------------------
    #### @params
    
    user_data : Third param passed to sqlite3_update_hook
    operation : SQLITE_DELETE, SQLITE_INSERT, or SQLITE_UPDATE
    db name : Name of the affected database
    table_name : Name of the affected table
    row_id : ID of the affected row

    ---------------------
    
    #### @returns

    None
    """
    db_name = "database.db"
    if operation == SQLITE_DELETE:
        print(f"Triggered DELETE on {row_id} in {table_name} table")

        cursor = sqlite3.connect("database.db").execute(f"SELECT * FROM {table_name.decode()} WHERE rowid={row_id}")
        columns = [row[0] for row in cursor.description]
        row = cursor.fetchone()

        # Starting a thread to make prevent data writing/reading errors
        Thread(target=delete_row, args=(table_name, row, columns, sem, "mongo-db")).start()


    elif operation == SQLITE_INSERT:
        print(f"Triggered INSERT on {row_id} in {table_name} table")

        # Starting a thread to make prevent data writing/reading errors
        Thread(target=insert_row, args=(table_name, row_id, sem, db_name ,"mongo-db")).start()        

    elif operation == SQLITE_UPDATE:
        print(f"Triggered UPDATE on {row_id} in {table_name} table")

        cursor = sqlite3.connect("database.db").execute(f"SELECT * FROM {table_name.decode()} WHERE rowid={row_id}")
        columns = [row[0] for row in cursor.description]
        row = cursor.fetchone()

        # Starting a thread to make prevent data writing/reading errors
        Thread(target=update_row, args=(table_name, row_id, row, columns, sem, db_name ,"mongo-db")).start()        

    else:
        print("Unknown operation : use SQLITE_UPDATE, SQLITE_INSERT, or SQLITE_DELETE")
        


# Load sqlite3
dll = CDLL('libsqlite3.so.0.8.6')

# Holds a pointer to the database connection
db = c_void_p()

# Open a connection to 'database.db'
dll.sqlite3_open('database.db'.encode(), byref(db))


# Translate into a ctypes callback
c_callback = CFUNCTYPE(c_void_p, c_void_p, c_int, c_char_p, c_char_p, c_int64)(callback)
# Register callback
dll.sqlite3_update_hook(db, c_callback, None)




# Create a variable to hold error messages
err = c_char_p()

sem = Lock()
print("Type 'q', 'quit', 'stop' or 'exit' to quit CLI")
while True:
    sem.acquire()

    print(Fore.YELLOW)
    request = input(f"SQLite > ")
    if request in ["exit", "q", "quit", "stop"]:
        print("Quitting instance")
        break
    if not (request.startswith("delete") or request.startswith("insert") or request.startswith("update")):
        print("Unknown command, only 'insert', 'delete' and 'update' are available")
        sem.release()
        continue
    

    byte_string = request.encode()
    dll.sqlite3_exec(db, byte_string, None, None, byref(err))
    if err:
        print(Fore.RED)
        print(err.value.decode())
        sem.release()
    
