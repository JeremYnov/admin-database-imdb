import db_config


# creation des collections
collection_production = db_config.database['production']

# récuperation colletion
prod_db = db_config.database.get_collection("production")


# DECOMMENTER POUR CREER L'INDEXE
prod_db.create_index('tconst')
print(prod_db.index_information())