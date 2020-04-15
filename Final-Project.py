import unittest
import sqlite3
import json
import os
import requests

#Names: Jessica Zhang 
#Project Topic: How the Coronavirus is affecting CO2 Levels in the Atmosphere in Delhi, India (using AQI) and Int'l Flights 
API_AQ = '851a8ae7-0eac-4f3a-8065-2e76906bc7f6'

#sets up database 
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def create_request_url_AQ(city):
    #http://api.airvisual.com/v2/nearest_city?key=your_key
    request_url = 'http://api.airvisual.com/v2/' + city + '?key=' + API_AQ 
    return request_url


def setUpTable1(data, cur, conn):

    cur.execute("DROP TABLE IF EXISTS __________")
    cur.execute("CREATE TABLE _________ (id INTEGER PRIMARY KEY, title TEXT)")
    conn.commit()

    pass 
