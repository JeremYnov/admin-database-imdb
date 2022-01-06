import db_config
import csv
from pymongo import UpdateOne, bulk
from alive_progress import alive_bar
import time

prod_db = db_config.database.get_collection("production")
requests = []
with open('csv/title-episode/title-episode.tsv') as title_episode:
    nbLines=sum(1 for _ in open('csv/title-episode/title-episode.tsv', newline=''))-1
    title_episode.seek(0)
    csv_reader = csv.reader(title_episode, delimiter='\t')
    
    with alive_bar(nbLines) as bar:
        for count, row in enumerate(csv_reader): 
            if count != 0:
                if row[2] and row[3] != '\\N': 
                    requests.append(UpdateOne({'tconst' : row[1]}, {'$set' : {'seasonNumber':int(row[2]), 'episodeNumber':int(row[3])}}))
                if count % 100000 == 0 or nbLines - count < 100000 :
                    bulk = prod_db.bulk_write(requests)
                    requests = []
            bar()            