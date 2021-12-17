from pymongo import MongoClient
# import itertools
import csv

# connexion à la bdd
client = MongoClient('localhost', 27017)

# connexion à la database
database = client['tp-imdb']

# creation des collections
collection_principals = database['principals']

# récuperation colletion
principals_db = database.get_collection("principals")

# supprime tout au lancement du script
principals_db.delete_many({})

# on ouvre title basics
with open('csv/name-basics/name-basics.tsv', 'r', encoding="utf8") as file:
    reader = csv.reader(file, delimiter="\t")
    # on lit les lignes 1000 par 1000
    for row in reader:
        
        # on split aux genres pour les creer en liste ex : "aaa,bbb" -> ["aaa", "bbb"]
        knownForTitles = row[5].split(",")

        # on construit notre json(document) qu'on va envoyer dans mongo
        data = {
            "nconst": row[0], 
            "primaryName": row[1], 
            "birthYear": row[2], 
            "deathYear": row[3], 
            "primaryProfession": row[4],
            "knownForTitles": knownForTitles
        }

        # on insere dans mongo
#         principals_db.insert_one(data)

# print(principals_db.count_documents({}))
