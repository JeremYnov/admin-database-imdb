import pandas as pd
import os

# path => Chemin du csv / size => nombre de ligne par csv
def divise_csv(path, size):
    # read DataFrame
    data = pd.read_csv(path + '.tsv',sep='\t')
        
    # k -> nombre de csv créé
    k = (len(data) // 10000) + 1

    # creation csv 
    for i in range(k):
        df = data[size*i:size*(i+1)]
        df.to_csv(f'{path}_{i+1}.csv', index=False)



my_list = os.listdir('csv')
for name in my_list: 
    # my_path = "csv/" + name + '/'+ name + '.tsv'
    my_path = "csv/" + name + '/'+ name
    print(my_path)
    divise_csv(my_path, 10000)
    # data =pd.read_csv(my_path, sep='\t')
    
