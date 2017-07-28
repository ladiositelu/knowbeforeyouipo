## Import libraries
import pandas as pd
import datetime
import sqlite3
from sqlalchemy import create_engine

start_year = 2004
years = (range(start_year, 2017))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']

history = [(y, q) for y in years for q in quarters] # a list of (year, quarter) tuples

# Create a list of the url names for the years and quarters of interest
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.idx' % (x[0], x[1]) for x in history]
urls.sort()


#Creating and connecting to sqlite database file
con = sqlite3.connect('/.../edgar_idx.db')
cur = con.cursor()

print "connected to sqlite database"

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

print "connection closed"


