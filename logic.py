# https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html

import pandas as pd
import requests
import unicodecsv as csv
import sqlite3 as db
from lxml import html
from sqlalchemy import create_engine


url = 'https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html'

# Opens a csv file in write mode
# out = csv.writer(open('stocks.csv','wb',))
# out.writerow(('company', 'link', 'marketcap'))

# Connect to SQLite DB 
engine = create_engine('sqlite:///C:\\Users\\GLP-118\\stocks.db')

page = requests.get(url)
tree = html.fromstring(page.content)

company = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/b/text()')
link = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/@href')
marketcap = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[6]/text()')

df = pd.DataFrame({'company':company, 'link':link, 'marketcap':marketcap})

# Stores data in SQLite DB 
df.to_sql('companies', con=engine, index=False, if_exists='append')
df1 = engine.execute("SELECT count(*) FROM companies").fetchall()
print df1


# Writes df to csv file
# for i, meta in df.iterrows():	
# 	out.writerow((meta['company'], meta['link'], meta['marketcap']))



