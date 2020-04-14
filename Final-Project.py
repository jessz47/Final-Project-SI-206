import unittest
import sqlite3
import json
import os
import requests

#Names: Jessica Zhang 

#sets up database 
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def setUpTable1(data, cur, conn):

    cur.execute("DROP TABLE IF EXISTS __________")
    cur.execute("CREATE TABLE _________ (id INTEGER PRIMARY KEY, title TEXT)")
    conn.commit()

    pass 
