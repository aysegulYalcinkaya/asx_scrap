def connect():
    import mysql.connector
    con = mysql.connector.connect(user='root', password='',
                                  host='localhost',
                                  database='patrick')
    return con


# Define a function to fetch a URL and return its response "
def fetch_url(url):
    import requests
    response = requests.get(url)
    return response.text


def get_data(asx_code, contents):
    from bs4 import BeautifulSoup
    from lxml import etree
    import re

    # Create a BeautifulSoup object from the HTML content
    soup = BeautifulSoup(contents, 'lxml')
    # Create an XPath object from the BeautifulSoup object
    xpath_soup = etree.HTML(str(soup))

    tables = xpath_soup.xpath("//announcement_data/table/tbody")
    data = []
    if len(tables) > 0:
        trs = tables[0].cssselect("tr")
        for tr in trs:
            tds = tr.cssselect("td")

            timeNode = tr.xpath(".//span[@class='dates-time']")
            time = timeNode[0].text.strip()
            date = tds[0].text.replace(time, "".lower()).strip()
            dateParts = date.split("/")

            date = dateParts[2] + "-" + dateParts[1] + "-" + dateParts[0]
            timeParts = time.split(":")

            if time.find("PM") > 0:
                time = str((int(timeParts[0]) + 12) % 24) + ":" + timeParts[1].replace("PM", "").strip()
            else:
                time = str(int(timeParts[0])) + ":" + timeParts[1].replace("AM", "").strip()
            linkNode = tds[2].cssselect("a")
            if len(linkNode) > 0:
                link = "https://www.asx.com.au" + linkNode[0].get("href")
            else:
                link = ""

            pageNode = tds[2].xpath(".//span[@class='page']")
            if len(pageNode) > 0:
                page = pageNode[0].text.replace("pages", "").replace('page', '').strip()
            else:
                page = ""

            sizeNode = tds[2].xpath(".//span[@class='filesize']")
            if len(sizeNode) > 0:
                size = sizeNode[0].text.strip()
            else:
                size = ""

            # replacers = {sizeNode[0].text: '', pageNode[0].text: '', 'pages': '', 'page': ''}
            title = ''.join(tds[2].itertext()).replace(sizeNode[0].text, '').replace(pageNode[0].text, ''). \
                replace('pages', '').replace('page', '').strip()
            data.append([asx_code, date, time, title, page, size, link])
    return data


def insert_data(con, data):
    for row in data:
        print(row)
        add_data = (
            "insert into asx_table (asx_code,announcement_date,announcement_time,announcement_title,page_count,file_size,link) values (%s, %s, %s, %s, %s, %s, %s) ")
        cursor = con.cursor()
        cursor.execute(add_data, tuple(row))
        con.commit()


def get_multi_page(asx_codes, period):
    from concurrent.futures import ThreadPoolExecutor
    urls = []
    for asx_row in asx_codes:
        urls.append('https://www.asx.com.au/asx/v2/statistics/announcements.do?by=asxCode&asxCode=' + asx_row +
                    '&timeframe=D&period=' + period)

        # Use a ThreadPoolExecutor to fetch all the URLs in parallel "
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit a task for each URL to the executor and collect the results "
            results = list(executor.map(fetch_url, urls))
    return results
