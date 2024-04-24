import pymongo


def create_movie_info(mongodb_name : str = "mongo-db", collection_name : str = "Movie_info", pipeline : list =  [{}], batch_size : int = 1000) -> None:
    """Creates a collection for all the useful informations about a movie
    ---------------------
    
    Note : The client is created with the default localhost/port usage for mongodb
    
    The pipeline is doing several junctions, this operation might take a while to complete.

    The collections are all in the same mongodb database

    ---------------------
    @params
    --

    mongodb_name : The name of the mongodb database to connect to
    collection_name : The name wanted for the new collection storing all informations about a movie
    pipeline : The pipeline command to use the 'aggregate()' method on
    batch_size : The size of the batch for continuous requests to prevent memory overflow

    ---------------------
    @returns
    --

    None
    """

    # Connexion pymongo à mongoDB
    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client[mongodb_name]
    movie_info = db[collection_name]

    movie = db["Movies"]
    
    batch_full = True   # Boolean indicating if we should keep doing other batches
    i=0 # Index for doing batches
    while(batch_full):
        pipeline_pagination = pipeline + [  # Add to the current pipeline a way to skip already written data (respectfully to the batch_size)
            {"$skip": batch_size*i},
            {"$limit":batch_size}
        ]

        result = movie.aggregate(pipeline_pagination)  # Read the 1000 (or less) lines to insert its data
        batch = list(result)
        if(batch != []):
            movie_info.insert_many(batch)   # Write previously read informations in the collection

        if(len(batch) < batch_size):    # Checking if the batch was to its full size, if not then it must be less so we don't need to keep looping
            batch_full = False
        
        print("batch "+ str(i))
        i+=1    # Incrementing this index to skip many through data with the batch_size

    # Terminating client connection
    client.close()
    


# Definition de la pipeline utilisée (avec jointures et même structure qu'éxpliqué dans le compte-rendu)
pipeline = [
   
    # Jointure de movies et principals (mid)
    {
        "$lookup":
        {
            "from": "Principals",
            "localField": "mid",
            "foreignField": "mid",
            "as": "principals"
        }
    },
    {
        "$unwind": "$principals"
    },
    # Recherche des job "actor" et "director" lors de la jointure
    {
        "$match":
        {
            "principals.category": { "$in": ["actor", "director"] }
        }
    },
    
    # Jointure entre principals et persons (pid)
    {
        "$lookup":
        {
            "from": "Persons",
            "localField": "principals.pid",
            "foreignField": "pid",
            "as": "casting"
        }
    },
    {
        "$unwind": "$casting"
    },
    
    # Jointure de movies et ratings
    {
        "$lookup":
        {
            "from": "Ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }
    },
    {
        "$unwind": "$rating"
    },

    # Jointure de movies et genres (mid)
    {
        "$lookup":
        {
            "from": "Genres",
            "localField": "mid",
            "foreignField": "mid",
            "as": "genre"
        }
    },
    {
        "$unwind": "$genre"
    },




    
    {
        "$group":{
            "_id": "$movie.mid",
            "mid" : {"$last" : "$movie.mid"},
            "primaryTitle": { "$last" : "$movie.primaryTitle" },
            "runtimeMinutes" : {"$last": "$movies.runtimeMinutes"},
            "startYear": { "$last" : "$movie.startYear" },
            "averageRating": { "$last" : "$rating.averageRating" },
            "numVotes":{"$last" : "$rating.numVotes"},
            "genre": { "$addToSet": "$genre.genre" },
            "actors":{ "$addToSet": {    # Rajout des acteurs dans la liste du casting
                    "$cond": [{ "$eq": [ "$principals.category", "actor" ] }, "$casting.primaryName", None] # Vérification que le "job" est bien "actor"
                }
            }, 
            "directors": { "$addToSet": { # Rajout des directeurs dans la liste du casting
                    "$cond": [{ "$eq": [ "$principals.category", "director" ] }, "$casting.primaryName", None] # Vérification que le "job" est bien "director"
                }
            }

        }
    },
    
    # Création de l'objet à charger en JSON
    {
        "$project": {
            "_id": 0,    # 0 Signifie que _id ne sera pas dans le résultat de la requête
            "mid" : 1,
            "primaryTitle":1,
            "runtimeMinutes":1,
            "startYear":1,
            "averageRating":1,
            "numVotes":1,
            "genre":1,
            "actors": {
                "$filter": {
                    "input": "$actors",
                    "as": "actor",
                    "cond": { "$ne": [ "$$actor", None ] }
                }
            },
            "directors": {
                "$filter": {
                    "input": "$directors",
                    "as": "director",
                    "cond": { "$ne": [ "$$director", None ] }
                }
            }
        }
    }


]


create_movie_info("mongo-db", "Movie_info",pipeline,1000)