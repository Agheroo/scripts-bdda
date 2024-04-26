from joins import *
import pymongo
from progress.bar import Bar
from colorama import Fore, Style





def create_movie_info(collection_name : str = "Movie_info", batch_size : int = 1000) -> None:
    """#### Creates a collection for all the useful informations about a movie
    ---------------------
    
    ##### Note :
    - The client is created with the default localhost/port usage for mongodb
    - The collections are all in the same mongodb database

    ---------------------
    #### @params

    collection_name : The name wanted for the new collection storing all informations about a movie
    batch_size : The size of the batch for continuous requests to prevent memory overflow

    ---------------------
    #### @returns

    None
    """
    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client["mongo-db"]

    db.drop_collection(collection_name) # Rewrite new collection for movies

    movies = db["Movies"].find({},{"_id":0})
    nb_movies = db["Movies"].count_documents({})    # Get all rows for movies

    batch = []
    i=0
    print(Fore.YELLOW)
    with Bar(f'Processing with a batch of {batch_size}...', max=nb_movies) as bar:  # Progress bar to track progress of data insertion
        for movie in movies:
            batch.append(join_movie(movie))
            i+=1
            if((i+1) % batch_size == 0):
                db[collection_name].insert_many(batch)
                batch.clear()
            bar.next()
        
        if(batch):  # If there is remaining data but not enough to complete the batch, insert them
            db[collection_name].insert_many(batch)
            bar.finish()
    
    # Adding index for better performance
    print("Adding index for movie_info...")
    keys = ["mid","primaryTitle" ,"type", "startYear","runtimeMinutes", "averageRating", "numVotes"]
    index_keys = list(map(lambda a: pymongo.IndexModel(a), keys))
    db[collection_name].create_indexes(index_keys)


    print(Style.RESET_ALL)
    client.close()

  
create_movie_info("Movie_info",1000)
