## Import libraries
import pandas as pd
import datetime
import sqlite3
import csv
import datetime
import requests
from sqlalchemy import create_engine

current_year = datetime.date.today().year
#current_quarter = ((datetime.date.today().month - 1) // 3) + 1

start_year = 2011
years = (range(start_year, 2015))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']

history = [(y, q) for y in years for q in quarters] # a list of (year, quarter) tuples
#for i in range(1, current_quarter + 1): ## Append the current year and current quarter to history
#history.append((current_year, 'QTR%d' % i))

# Create a list of the url names for the years and quarters of interest
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.idx' % (x[0], x[1]) for x in history]
urls.sort()


#Creating and connecting to sqlite database file
con = sqlite3.connect('/Users/oladipoositelu/edgar_idx.db')
cur = con.cursor()

#Create a new table with the following columns:
#1. cik==, 2. conm == company name, 3. type == type of form,
#4. date==date of filing, 5. path == path for downloading the form

cur.execute('DROP TABLE IF EXISTS idx') ## if table already exists, drop it
cur.execute('CREATE TABLE idx (cik TEXT, conm TEXT, type TEXT, date TEXT, path TEXT)')

for url in urls:
    lines = requests.get(url).text.splitlines() #
    records = [tuple(line.split('|')) for line in lines[11:]]
    cur.executemany('INSERT INTO idx VALUES (?, ?, ?, ?, ?)', records)
    print(url, 'downloaded and written to SQLite')

#commit then close the connection
con.commit()
con.close()

engine = create_engine('sqlite:///edgar_idx.db')
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_table('idx', conn)
    data[data['type']=='S-1'].to_csv('/Users/oladipoositelu/Form_S1.csv',index=False) ##Save only the form S-1s
