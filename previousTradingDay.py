import functions


def getpage_prevbusday():
    import requests
    url = 'https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do'
    response = requests.get(url)
    return response.text


def get_page_data(contents):
    from bs4 import BeautifulSoup
    from lxml import etree
    import re

    # Create a BeautifulSoup object from the HTML content
    soup = BeautifulSoup(contents, 'lxml')
    # Create an XPath object from the BeautifulSoup object
    xpath_soup = etree.HTML(str(soup))

    tables = xpath_soup.xpath("//announcement_data/table")
    data = []
    if len(tables) > 0:
        trs = tables[0].cssselect("tr")
        for tr in trs:
            tds = tr.cssselect("td")
            if len(tds) > 0:
                asx_code = tds[0].text
                timeNode = tr.xpath(".//span[@class='dates-time']")
                time = timeNode[0].text.strip()
                date = tds[1].text.replace(time, "".lower()).strip()
                dateParts = date.split("/")

                date = dateParts[2] + "-" + dateParts[1] + "-" + dateParts[0]
                timeParts = time.split(":")

                if time.find("PM") > 0:
                    time = str((int(timeParts[0]) + 12) % 24) + ":" + timeParts[1].replace("PM", "").strip()
                else:
                    time = str(int(timeParts[0])) + ":" + timeParts[1].replace("AM", "").strip()
                linkNode = tds[3].cssselect("a")
                if len(linkNode) > 0:
                    link = "https://www.asx.com.au" + linkNode[0].get("href")
                else:
                    link = ""

                pageNode = tds[3].xpath(".//span[@class='page']")
                if len(pageNode) > 0:
                    page = pageNode[0].text.replace("pages", "").replace('page', '').strip()
                else:
                    page = ""

                sizeNode = tds[3].xpath(".//span[@class='filesize']")
                if len(sizeNode) > 0:
                    size = sizeNode[0].text.strip()
                else:
                    size = ""

                # replacers = {sizeNode[0].text: '', pageNode[0].text: '', 'pages': '', 'page': ''}
                title = ''.join(tds[3].itertext()).replace(sizeNode[0].text, '').replace(pageNode[0].text, ''). \
                    replace('pages', '').replace('page', '').strip()
                data.append([asx_code, date, time, title, page, size, link])
    return data


con = functions.connect()
result = getpage_prevbusday()
data = get_page_data(result)
functions.insert_data(con, data)
