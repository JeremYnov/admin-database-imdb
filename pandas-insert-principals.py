from pandas.core.arrays import string_
from pymongo import MongoClient
import pandas as pd

# connexion à la bdd
client = MongoClient('localhost', 27017)

# connexion à la database
database = client['tp-imdb']

# creation des collections
collection_principals = database['principals']

# récuperation collection
principals_db = database.get_collection("principals")

# supprime tout au lancement du script
principals_db.delete_many({})

count = 1
principals_df = pd.read_csv('csv/name-basics/name-basics.tsv',sep='\t', skiprows=count, nrows=1000)

while not principals_df.empty:
    principals_df = pd.read_csv('csv/name-basics/name-basics.tsv',sep='\t', skiprows=count + 1, nrows=100000, header = None)
    principals_df.columns = ["nconst", "primaryName", "birthYear", "deathYear","primaryProfession","knownForTitles"]
    principals_df = principals_df.fillna('no employment')
    principals_df['primaryProfession'] = principals_df['primaryProfession'].apply(lambda x : x.split(','))
    principals_df['knownForTitles'] = principals_df['knownForTitles'].apply(lambda x : x.split(','))
    principals_df_json = principals_df.to_dict(orient='records')
    principals_db.insert_many(principals_df_json)
    count += 100000