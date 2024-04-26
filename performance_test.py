import pymongo
import time
from joins import *
from progress.bar import Bar
from colorama import Fore, Style




def compare_performances(mongodb_name : str = "mongo-db", sample_size : int = 2000) -> None:
    """#### Tests and prints performances of joining movies with selecting with the json movie_info collection
    ---------------------
    
    ##### Note :
    - The client is created with the default localhost/port usage for mongodb

    ---------------------
    #### @params

    mongodb_name : The name of the mongodb database relative to this script
    sample_size : The number of movies to take in the sample to compare performances

    ---------------------
    #### @returns

    None
    """

    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client[mongodb_name]

    # Get some movies to use to compare the performances
    movies = db["Movies"].aggregate([{"$sample": {"size": sample_size}}])

    movies = list(map(lambda row: row["mid"],movies))


    print(Fore.YELLOW)
    start = time.time()
    with Bar(f'Processing joins task...', max=sample_size) as bar:  # Progress bar to track progress 
        for movie_id in movies:
            # Testing performance when we join individually every movie

            movie = db["Movies"].find_one({ "mid": {"$eq": movie_id} })
            movie = join_movie(movie)
            bar.next()
        bar.finish()
    print("Task with joins took : " + str(time.time() - start))

    with Bar(f'Processing JSON find task...', max=sample_size) as bar:  # Progress bar to track progress 
        for movie_id in movies:
            
            # Testing performance when we select a movie with the JSON file
            db["Movie_info"].find_one({ "mid": {"$eq": movie_id} })
            bar.next()
        bar.finish()
    print("Task with JSON collection took : " + str(time.time() - start))



    start = time.time()


    client.close()


compare_performances("mongo-db",1000)