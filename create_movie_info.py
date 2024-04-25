import time
import pymongo
from progress.bar import Bar
from colorama import Fore, Style

client = pymongo.MongoClient('127.0.0.1', 27017)
db = client["mongo-db"]

def join_ratings_movie(movie : dict = {}) -> dict:
    """#### Join the ratings of a given movie to the dict of the movie
    ---------------------
    
    ##### Note :
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    mongodb_name : The name of the mongodb database relative to this script
    movie : The movie dict that needs to be joined to its rating

    ---------------------
    #### @returns

    movie : The new movie dict that's been modified with new attributes from ratings
    """

    ratings = db["Ratings"].find_one({"mid":movie["mid"]})  # Only find_one since every mid is primary key in Ratings and Movies table

    movie["averageRating"] = ratings["averageRating"]
    movie["numVotes"] = ratings["numVotes"]

    return movie

def join_genres_movie(movie : dict = {}) -> dict:
    """#### Adds the genres list of a given movie to the dict of the movie
    ---------------------
    
    ##### Note : 
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    mongodb_name : The name of the mongodb database relative to this script
    movie : The movie dict that needs to be joined to its genres

    ---------------------
    #### @returns

    movie : The new movie dict that's been modified with new attributes from genres
    """

    genres = db["Genres"].find(
        {"mid" : movie["mid"]},
        {"_id" : 0, "genre" : 1} # Make the _id hidden and genre visible
    )

    movie["genres"] = [elem["genre"] for elem in genres]    # Gets a list of all the different genres found for a movie
    return movie
    
def join_casting_movie(movie : dict = {}) -> dict:
    """#### Adds the casting list (actors) and principals (directors / producers...) of a given movie
    ---------------------
    
    ##### Note :
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    movie : The movie dict that needs to be joined to its principal casting

    ---------------------
    #### @returns

    movie : The new movie dict that's been modified with new attributes (actors and directors)
    """

    principals = db["Principals"].find(
        {"mid" : movie["mid"]},
        {"_id" : 0, "mid" : 0} # Register all principals of the movie in a list
    )

    principals_pid = list(map(lambda row: row["pid"],principals.clone()))


    persons = list(db["Persons"].find(
        {"pid": {"$in" : principals_pid}},
        {"_id":0,"pid":1,"primaryName":1}
    ))


    # Join principals, persons and movies
    movie["principals"] = [
        {
            "pid": principal["pid"],
            "name": pers["primaryName"],
            "category": principal["category"],
            "job": principal["job"]
        }
        for principal in principals
        for pers in persons 
        if pers["pid"] == principal["pid"]
    ]

    return movie

def join_movie(movie : dict = {}) -> dict:
    """#### Does all joins of a given movie adding key informations
    ---------------------
    
    ##### Note : 
    - Joins "Ratings", "Genres" and "Principals / Persons"
    - Adds for a movie its averageRating, numVotes, actors, directors, and genres
    ---------------------
    #### @params

    movie : The movie dict that needs its new informations to be added

    ---------------------
    #### @returns

    movie : The new movie dict that's been modified with all new attributes
    """

    movie = {
        "mid": movie["mid"],
        "type": movie["titleType"],
        "startYear": movie["startYear"],
        "runtimeMinutes": movie["runtimeMinutes"],
        "primaryTitle": movie["primaryTitle"]
    }
    movie = join_ratings_movie(movie)
    movie = join_genres_movie(movie)
    movie = join_casting_movie(movie)


    return movie


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
    print(Style.RESET_ALL)

  
create_movie_info("Movie_info",1000)
client.close()