from pymongo import MongoClient
import json

# Connecter à la base de données MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['mongo-db']
movie_info = db["movie_info"]

# Ouvrir le fichier JSON en mode lecture
with open('movie_info.json', 'r') as f:
    # Charger le contenu du fichier JSON dans une liste de dictionnaires
    movies_list = json.load(f)

# Insérer les documents dans la collection "movies"
result = movie_info.insert_many(movies_list)


