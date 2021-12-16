from pymongo import MongoClient
import itertools
import csv

# connexion à la bdd
client = MongoClient('localhost', 27017)

# connexion à la database
database = client['tp-imdb']

# creation des collections
collection_production = database['production']

# récuperation colletion
prod_db = database.get_collection("production")

# supprime tout au lancement du script
prod_db.delete_many({})

# on ouvre title basics
with open('csv/title-basics/title-basics.tsv', 'r', encoding="utf8") as file:
    reader = csv.reader(file, delimiter="\t")
    # on lit les lignes 1000 par 1000
    for row in itertools.islice(reader, 1000):
        
        # on split aux genres pour les creer en liste ex : "aaa,bbb" -> ["aaa", "bbb"]
        genres = row[8].split(",")

        # on construit notre json(document) qu'on va envoyer dans mongo
        data = {
            "tconst": row[0], 
            "titleType": row[1], 
            "primaryTitle": row[2], 
            "originalTitle": row[3], 
            "isAdult": row[4],
            "startYear": row[5],
            "endYear": row[6],
            "runtimeMinutes": row[7],
            "genres": genres
        }

        # on insere dans mongo
        prod_db.insert_one(data)

# print(prod_db.count_documents({}))

# for a in prod_db.find():
#     print(a)
        