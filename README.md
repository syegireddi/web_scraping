# web_scraping
Scrapes the Money control site 

**logic.py file**

* Script for web scraping, and data base insertions is written here

**Database**

* SQLite db is used
* The `connect_db()` function in `logic.py` contains the connection string
* `engine = create_engine('sqlite:///C:\\Users\\GLP-118\\stocks.db')`

* A table named `companies` is created in database named `stocks` in SQLite with the following schema:

`CREATE TABLE companies (company TEXT, sector TEXT, marketcap TEXT, pe_ratio TEXT)`

* Connection to the db is achieved using **sqlalchemy create engine**

**Scraping**
`
* The following companies have been shortlisted for extraction
`base_companies = ['Carborundum', 'Grindwell Norto', 'Wendt', 'Orient Abrasive', 'NALCO', 'Century Extr' , 'PG Foils','Hindalco', 'Manaksia Alumin']`
* `lxml module` in python has been used for scraping
* These are stored in a global variable `base_companies` in logic.py file

**Web Scraping steps:**

* The various sectors and their href links are initially extracted from [https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html](https://www.moneycontrol.com/stocks/marketinfo/marketcap/bse/index.html)

* Every sector link is iterated through and respective companies from the table are extracted along with the following info:

a\) company specific href link b\) market cap value

* It is to be noted that `hrefs` and `market cap` is extracted for only those companies specified in the `base_companies`
* Now the company specific pages are iterated through and `P/E metric` is extracted for all listed companies

```
sectors_df = scrape_sectors() // extracts sector and sector href links
company_df = scrape_companies(sectors_df) // extracts sector wise companies, href links, market cap metric
metric_df = scrape_metrics(company_df) // extracts PE/ metric for every listed company
```

**Prevention of insertion of duplicate records**

* `remove_duplicates_db()` function takes care of the following :
* In subsequent execution of script no duplicate records are inserted and
new data override old data.

**Persisting extracted data in SQLite DB**

* `insert_to_db()`is responsible for inserting scraped data into the db

**Cleansing and metric calculation**

* The data is then cleansed to remove commas and string formatting and the following metrics are computed in pandas
* * 3rd and 4th highest market cap companies sector wise.
* Bucket P/E ratios in interval of 5, 11-15,16-20,21-25,...,66-70



