import pymongo


def join_ratings_movie(mongodb_name : str = "mongo-db", movie : dict = {}) -> dict:
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

    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client[mongodb_name]
    ratings = db["Ratings"].find_one({"mid":movie["mid"]})  # Only find_one since every mid is primary key in Ratings and Movies table

    movie["averageRating"] = ratings["averageRating"]
    movie["numVotes"] = ratings["numVotes"]

    client.close()
    return movie

def join_genres_movie(mongodb_name : str = "mongo-db", movie : dict = {}) -> dict:
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
    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client[mongodb_name]

    genres = db["Genres"].find(
        {"mid" : movie["mid"]},
        {"_id" : 0, "genre" : 1} # Make the _id hidden and genre visible
    )

    movie["genres"] = [elem["genre"] for elem in genres]    # Gets a list of all the different genres found for a movie

    client.close()
    return movie
    
def join_casting_movie(mongodb_name : str = "mongo-db", movie : dict = {}) -> dict:
    """#### Adds the casting list (actors) and principals (directors / producers...) of a given movie
    ---------------------
    
    ##### Note :
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    mongodb_name : The name of the mongodb database relative to this script
    movie : The movie dict that needs to be joined to its principal casting

    ---------------------
    #### @returns

    movie : The new movie dict that's been modified with new attributes (actors and directors)
    """

    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client[mongodb_name]

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

    client.close()
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
    movie = join_ratings_movie("mongo-db",movie)
    movie = join_genres_movie("mongo-db",movie)
    movie = join_casting_movie("mongo-db",movie)

    return movie
