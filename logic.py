# https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html

import pandas as pd
import requests
from lxml import html
from sqlalchemy import create_engine

url = 'https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html'

# Connect to SQLite DB 
engine = create_engine('sqlite:///C:\\Users\\GLP-118\\stocks.db')

page = requests.get(url)
tree = html.fromstring(page.content)

company = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/b/text()')
link = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/@href')
marketcap = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[6]/text()')

df = pd.DataFrame({'company':company, 'link':link, 'marketcap':marketcap})

# Unique list of companies from dataframe
df_uniq_companies = df.company.unique()

# Extract Unique list of companies from SQLite DB
table_uniq_companies = engine.execute("SELECT distinct company FROM companies").fetchall()
# Converts 1-element tuples into a list
table_uniq_companies = [r[0] for r in table_uniq_companies] 

# List of companies which need to be deleted from database to avoid duplicates
intersection_list = list(set(df_uniq_companies) & set(table_uniq_companies))
intersection_list = [str(r) for r in intersection_list]

# Deletion script
# Ensures in subsequent execution of script no duplicate records should be inserted and new data overrides old data.
delete_sql = 'delete FROM companies where company in ' + str(tuple(intersection_list))
engine.execute(delete_sql)

# Stores latest scraped data in SQLite DB 
df.to_sql('companies', con=engine, index=False, if_exists='append')





