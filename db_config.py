from pymongo import MongoClient

# connexion à la bdd
client = MongoClient('localhost', 27017)

# connexion à la database
database = client['tp-imdb']

nrows = 100000