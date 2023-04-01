import pandas as pd
import functions

from concurrent.futures import ThreadPoolExecutor

data = pd.read_csv("asx-codes.csv", header=None)
con = functions.connect()

for index, asx_row in data.iterrows():
    print(asx_row[0])
    # Use a ThreadPoolExecutor to fetch all the URLs in parallel "
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit a task for each URL to the executor and collect the results "
        urls = []
        for year in range(1998, 2023):
            urls.append('https://www.asx.com.au/asx/v2/statistics/announcements.do?by=asxCode&asxCode=' + str(
                asx_row[0]) + '&timeframe=Y&year=' + str(year))
        results = list(executor.map(functions.fetch_url, urls))

    for result in results:
        data = functions.get_data(asx_row[0], result)
        if data:
            functions.insert_data(con, data)
con.close()
