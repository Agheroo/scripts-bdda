import json
import pymongo
from bson.json_util import dumps


#connexion pymongo à mongoDB
client = pymongo.MongoClient('127.0.0.1', 27017)
db = client["mongo-db"]

movies = db["Movies"]


pipeline = [
    
    #Chargement de movies pour les jointures
    {
        "$lookup": {
            "from": "Movies",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie"
        }
    },
    {
        "$unwind": "$movie"
    },
    
    
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
            "_id": "$movie._id",
            "mid" : {"$last" : "$movie.mid"},
            "primaryTitle": { "$last" : "$movie.primaryTitle" },
            "startYear": { "$last" : "$movie.startYear" },
            "averageRating": { "$last" : "$rating.averageRating" },
            "numVotes":{"$last" : "$rating.numVotes"},
            "genre": { "$addToSet": "$genre.genre" },
            "actors":{ "$addToSet": {    #Rajout des acteurs dans la liste du casting
                    "$cond": [{ "$eq": [ "$principals.category", "actor" ] }, "$casting.primaryName", None]
                }
            }, 
            "directors": { "$addToSet": { #Rajout des directeurs dans la liste du casting
                    "$cond": [{ "$eq": [ "$principals.category", "director" ] }, "$casting.primaryName", None]
                }
            }

        }
    },
    
    #Création de l'objet à charger en JSON
    {
        "$project": {
            "_id":0,    # 0 Signifie que _id ne sera pas dans le résultat de la requête
            "mid" : 1,
            "primaryTitle":1,
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


# Exécuter l'agrégation
try:
    # Exécuter l'agrégation
    result = movies.aggregate(pipeline)
    # Afficher les résultats ou effectuer d'autres opérations ici
except Exception as e:
    print("Une erreur s'est produite lors de l'exécution de l'agrégation :", e)

# Convertir le résultat en JSON
json_result = dumps(result)


# Écrire le résultat JSON dans un fichier
with open('movie_info.json', 'w') as f:
    json.dump(json.loads(json_result), f, indent=4)
 