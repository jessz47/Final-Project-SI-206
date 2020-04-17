import unittest
import sqlite3
import json
import os
import requests

#Names: Jessica Zhang 
#Project Topic: How the Coronavirus is affecting CO2 Levels in the Atmosphere in Delhi, India (using AQI) and Int'l Flights 
API_AQ = 'berryram97'

#sets up database 
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

#dates are in YYYYMMDD format
#state = 2 digit state FIPS
#county = 3 digit state FIPS code for the county
#email is the requester's
def create_request_url_AQ(email, param, bdate, edate, state_num, county_num):
    #https://aqs.epa.gov/data/api/sampleData/byCounty?email=test@aqs.api&key=test&param=88101&bdate=20160101&edate=20160228&state=37&county=183    request_url = 'https://api.breezometer.com/air-quality/v2/historical/hourly?lat=' + latitude + '&lon=' + longitude + '&key=' API_AQ + '&start_datetime=' + start_date + '&end_datetime=' + end_date
    request_url = 'https://aqs.epa.gov/data/api/sampleData/byCounty?email=' + email + '&key=' + API_AQ + '&param=' + str(param) + '&bdate=' + str(bdate) + '&edate=' + str(edate) + '&state=' + str(state_num) + '&county=' + str(county_num)
    return request_url
    
def get_AQ_data(email, param, bdate, edate, state_num, county_num):
    request_url = create_request_url_AQ(email, param, bdate, edate, state_num, county_num)

    results = requests.get(request_url)
    r = json.loads(results.text)
    print(json.dumps(r))
    
                  


# def setUpTableAQ(data, cur, conn):

#     cur.execute("DROP TABLE IF EXISTS Air Quality")
#     cur.execute('''
#         CREATE TABLE IF NOT EXISTS Air Quality 
#             ( TEXT PRIMARY KEY, 
#             name TEXT, 
#             address TEXT, 
#             zip TEXT, 
#             category_id INTEGER, 
#     ''')
#     conn.commit()

    pass 



def main():
    get_AQ_data('jessz@umich.edu', 88101, 20200101, 20200413, 36, '061')



if __name__ == "__main__":
    main()
