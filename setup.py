#!/usr/bin/python
import logging
import MySQLdb
from config import crawl_database as db

# Connect to the db and create the tables if they don't already exist
connection = MySQLdb.connect(db['host'], db['user'], db['passwd'], db['db'])
cursor = connection.cursor()

# crawl_index: holds all the information of the urls that have been crawled
cursor.execute(
    'CREATE TABLE IF NOT EXISTS crawl_index (id INTEGER NOT NULL AUTO_INCREMENT,       \
                                             url VARCHAR(20000),      \
                                             parent VARCHAR(20000),   \
                                             status INTEGER,        \
                                             PRIMARY KEY (id))'
     )

# queue: this should be obvious
cursor.execute(
    'CREATE TABLE IF NOT EXISTS queue (id INTEGER NOT NULL AUTO_INCREMENT, \
                                       depth INTEGER,                      \
                                       url VARCHAR(20000),                   \
                                       parent VARCHAR(20000),                \
                                       PRIMARY KEY (id))'
    )

# status: Contains a record of when crawling was started and stopped.
# Mostly in place for a future application to watch the crawl interactively.
cursor.execute('CREATE TABLE IF NOT EXISTS status (s INTEGER, t TEXT)')

connection.commit()

cursor.execute("INSERT INTO queue (url, depth, parent) VALUES('http://ndep.webdevdc.com/', 0, NULL)")
connection.commit()
connection.close()
