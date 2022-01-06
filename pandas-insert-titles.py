from pandas.core.arrays import string_
from pymongo import MongoClient
import pandas as pd
import db_config


# creation des collections
collection_production = db_config.database['production']

# récuperation colletion
prod_db = db_config.database.get_collection("production")

# supprime tout au lancement du script
prod_db.delete_many({})

count = 0
prod_df = pd.read_csv('csv/title-basics/title-basics.tsv',sep='\t', skiprows=count, nrows=db_config.nrows)

while not prod_df.empty:
    prod_df = pd.read_csv('csv/title-basics/title-basics.tsv',sep='\t', skiprows=count + 1, nrows=db_config.nrows, header = None)
    prod_df.columns = ["tconst", "titleType", "primaryTitle", "originalTitle","isAdult","startYear", "endYear", "runtimeMinutes","genres"]
    prod_df['genres'] = prod_df['genres'].apply(lambda x : x.split(','))
    prod_df_json = prod_df.to_dict(orient='records')
    prod_db.insert_many(prod_df_json)
    count += db_config.nrows