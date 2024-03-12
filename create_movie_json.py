import json
import pymongo
from bson.json_util import dumps


#connexion pymongo à mongoDB
client = pymongo.MongoClient('127.0.0.1', 27017)
db = client["mongo-db"]

movies = db['movies']
ratings = db['ratings']
genres = db['genres']
principals = db['principals']
persons = db['persons']

pipeline = [
    
    #Jointure de movies et ratings
    {
        "$lookup":
        {
            "from": "ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }
    },
    {
        "$unwind": "$rating"
    },
    
    #Jointure de movies et genres
    {
        "$lookup":
        {
            "from": "genres",
            "localField": "mid",
            "foreignField": "mid",
            "as": "genre"
        }
    },
    {
        "$unwind": "$genre"
    },
    
    #Jointure de movies et principals
    {
        "$lookup":
        {
            "from": "principals",
            "localField": "mid",
            "foreignField": "mid",
            "as": "principals"
        }
    },
    {
        "$unwind": "$principals"
    },

    
    
    #Jointure entre principals et persons (pid)
    {
        "$lookup":
        {
            "from": "persons",
            "localField": "principals.pid",
            "foreignField": "pid",
            "as": "actors"
        }
    },
    {
        "$unwind": "$actors"
    },
    #Recherche des job "actor" lors de la liaison
    {
        "$match":
        {
            "principals.job": { "$in": ["actor", "director"] }
        }
    },
    
    {
        "$facet": {
            "actors": [
                {
                    "$unwind": "$principals"
                },
                {
                    "$match": {
                        "principals.job": "actor"
                    }
                },
                {
                    "$lookup":
                    {
                        "from": "persons",
                        "localField": "principals.personId",
                        "foreignField": "pid",
                        "as": "person"
                    }
                },
                {
                    "$unwind": "$person"
                },
                {
                    "$project": {
                        "title": "$originalTitle",
                        "name": "$person.primaryName"
                    }
                },
                {
                    "$group": {
                        "_id": "$title",
                        "actors": { "$push": "$name" }
                    }
                }
            ],
            "directors": [
                {
                    "$unwind": "$principals"
                },
                {
                    "$match": {
                        "principals.job": "director"
                    }
                },
                {
                    "$lookup":
                    {
                        "from": "persons",
                        "localField": "principals.personId",
                        "foreignField": "pid",
                        "as": "person"
                    }
                },
                {
                    "$unwind": "$person"
                },
                {
                    "$project": {
                        "title": "$originalTitle",
                        "name": "$person.primaryName"
                    }
                },
                {
                    "$group": {
                        "_id": "$title",
                        "directors": { "$push": "$name" }
                    }
                }
            ]
        }
    },
    
    #Création du JSON
    {
        "$project": {
            "title": "$movies.originalTitle",
            "startYear":"$movies.startYear",
            "averageRating": "$rating.averageRating",
            "numVotes": "$rating.numVotes",
            "genre" : "$genre.genre",
            "actorNames" : "$persons.primaryName",
            "directorName" : "$persons.primaryName"
        }
    }
]

# Exécuter l'agrégation
result = movies.aggregate(pipeline)

# Convertir le résultat en JSON
json_result = dumps(result)

# Écrire le résultat JSON dans un fichier
with open('output.json', 'w') as f:
    json.dump(json.loads(json_result), f, indent=4)