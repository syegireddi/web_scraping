# https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html

import pandas as pd
from lxml import html
import requests
import unicodecsv as csv

url = 'https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html'

out = csv.writer(open('stocks.csv','wb',))
out.writerow(('company', 'link', 'marketcap'))

page = requests.get(url)
tree = html.fromstring(page.content)

company = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/b/text()')
link = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/@href')
marketcap = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[6]/text()')

df = pd.DataFrame({'company':company, 'link':link, 'marketcap':marketcap})

for i, meta in df.iterrows():	
	out.writerow((meta['company'], meta['link'], meta['marketcap']))



