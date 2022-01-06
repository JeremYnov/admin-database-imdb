import db_config
import csv
from pymongo import UpdateOne, bulk
from alive_progress import alive_bar
import time

prod_db = db_config.database.get_collection("production")
requests = []
with open('csv/title-ratings/title-ratings.tsv') as title_ratings:
    nbLines=sum(1 for _ in open('csv/title-ratings/title-ratings.tsv', newline=''))-1
    title_ratings.seek(0)
    csv_reader = csv.reader(title_ratings, delimiter='\t')
    
    with alive_bar(nbLines) as bar:
        for count, row in enumerate(csv_reader): 
            if count != 0:
                print(count)
                requests.append(UpdateOne({'tconst' : row[0]}, {'$set' : {'averageRating':float(row[1]), 'numVotes':int(row[2])}}))
                if count % 10000 == 0:
                    print("Len of REQUESTS :" + str(len(requests)))
                    print("============================================TEST 1 ===============================")
                    bulk = prod_db.bulk_write(requests)
                    print("============================================TEST 2 ===============================")
                    requests = []
                    print(bulk.bulk_api_result)
            bar()             