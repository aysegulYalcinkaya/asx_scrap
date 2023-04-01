import pandas as pd
import functions

from concurrent.futures import ThreadPoolExecutor

data = pd.read_csv("asx-codes.csv", header=None)
con = functions.connect()

i = 0
full_asx = []
for index, asx_row in data.iterrows():
    full_asx.append(asx_row[0])

while i < len(full_asx):
    if i + 50 < len(full_asx):
        stop = i + 50
        asx_codes = full_asx[i:stop]
        print("Getting " + asx_codes[0] + "-" + asx_codes[stop-1] + "\n")
    else:
        asx_codes = full_asx[i:]
        print("Getting " + asx_codes[0] + "-" + asx_codes[-1] + "\n")
    results = functions.get_multi_page(asx_codes, "W")

    index = 0
    for result in results:
        data = functions.get_data(asx_codes[index], result)
        index += 1
        if data:
            functions.insert_data(con, data)

    i += 50
