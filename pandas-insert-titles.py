from pandas.core.arrays import string_
from pymongo import MongoClient
import pandas as pd
import csv
import time

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

count = 1
prod_df = pd.read_csv('csv/title-basics/title-basics.tsv',sep='\t', skiprows=count, nrows=1000)

while not prod_df.empty:
    prod_df = pd.read_csv('csv/title-basics/title-basics.tsv',sep='\t', skiprows=count + 1, nrows=1000000, header = None)
    prod_df.columns = ["tconst", "titleType", "primaryTitle", "originalTitle","isAdult","startYear", "endYear", "runtimeMinutes","genres"]
    prod_df['genres'] = prod_df['genres'].apply(lambda x : x.split(','))
    prod_df_json = prod_df.to_dict(orient='records')
    prod_db.insert_many(prod_df_json)
    count += 1000000