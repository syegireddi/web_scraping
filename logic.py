# https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html

import pandas as pd
import requests
import string
from lxml import html
from sqlalchemy import create_engine

base_url = 'https://www.moneycontrol.com'
base_companies = ['Carborundum', 'NALCO', 'Century Extr' , 'PG Foils']


def connect_db():
	""" Connect to SQLite DB """ 
	engine = create_engine('sqlite:///C:\\Users\\GLP-118\\stocks.db')
	return engine

def scrape_sectors():
	""" Scrapes sector links"""
	url = 'https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html'

	page = requests.get(url)
	tree = html.fromstring(page.content)

	sector = tree.xpath('//div[3]/div[1]/div[7]/div[1]/div[2]/ul/li/a/text()')
	sector_link = tree.xpath('//div[3]/div[1]/div[7]/div[1]/div[2]/ul/li/a/@href')

	df = pd.DataFrame({'sector':sector, 'sector_link':sector_link})
	sectors_df = df[df.sector!='Top 100']

	return sectors_df

def scrape_companies(sectors_df):
	""" Scrapes company data fom web and stores in a python dataframe"""

	sectors_df = sectors_df.head(2)
	main_df_list = []

	for i, meta in sectors_df.iterrows():
		url = base_url + meta['sector_link']
		page = requests.get(url)
		tree = html.fromstring(page.content)

		company = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/b/text()')
		link = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[1]/a/@href')
		marketcap = tree.xpath('//table[@class="tbldata14 bdrtpg"]/tr/td[6]/text()')

		df = pd.DataFrame({'company':company, 'link':link, 'sector':meta['sector'], 'marketcap':marketcap})
		df = df[df['company'].isin (base_companies)]
		main_df_list.append(df)

	main_df = pd.concat(main_df_list)
	return main_df

def scrape_metrics(company_df):
	""" Scrapes relevant metrics for companies """

	main_df_list = []

	for i, meta in company_df.iterrows():
		url = base_url + meta['link']
		page = requests.get(url)
		tree = html.fromstring(page.content)

		pe_ratio = tree.xpath('//*[@id="mktdet_1"]/div[1]/div[2]/div[2]/text()')	

		df = pd.DataFrame({'company':meta['company'], 'pe_ratio': pe_ratio })
		main_df_list.append(df)

	main_df = pd.concat(main_df_list)
	return main_df


def remove_duplicates_db(df):
	""" Removes Duplicate records from DB"""
	engine = connect_db()

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
	if len(intersection_list) !=0:
		if len(intersection_list) == 1:
			delete_sql = 'delete FROM companies where company in ' + string.replace(str(tuple(intersection_list)), ',', '')
		else :
			delete_sql = 'delete FROM companies where company in ' + str(tuple(intersection_list))
		engine.execute(delete_sql)

def insert_to_db(df):
	""" Inserts latest scraped data in SQLite DB """
	engine = connect_db()
	df.to_sql('companies', con=engine, index=False, if_exists='append')

def main():
	sectors_df = scrape_sectors()
	company_df = scrape_companies(sectors_df)
	metric_df = scrape_metrics(company_df)
	
	result_df = pd.merge(company_df, metric_df, how='right', on='company')[['company', 'sector', 'marketcap', 'pe_ratio']]

	remove_duplicates_db(result_df)
	insert_to_db(result_df)
	


if __name__ == "__main__":
    main()



