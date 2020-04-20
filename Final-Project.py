import unittest
import sqlite3
import json
import os
import requests
import pdb

#Names: Jessica Zhang 
#Project Topic: How the Coronavirus is affecting CO2 Levels in the Atmosphere in Delhi, India (using AQI) and Int'l Flights 
API_AQ = 'bolebird88'


#sets up database 
def setUpDatabase(db_file):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + db_file)
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

#countries are written in all lowercase
#caseType must be one of : confirmed, recovered, deaths 
def create_request_url_COVID(country, caseType):
    url = 'https://api.covid19api.com/dayone/country/' + country + "/status/" + caseType
    return url 

    
def get_AQ_data(email, param, bdate, edate, state_num, county_num):
    request_url = create_request_url_AQ(email, param, bdate, edate, state_num, county_num)

    results = requests.get(request_url)
    r = json.loads(results.text)
    #print(json.dumps(r))
    return json.dumps(r)


def get_COVID_data(country, caseType):
    requests_url = create_request_url_COVID(country, caseType)

    results = requests.get(requests_url)
    c = json.loads(results.text)
   #print(json.dumps(c))
    
    return json.dumps(c)


def setUpReadingsTable(email, param, bdate, edate, state_num, county_num, cur, conn):

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Readings 
            (reading_id INTEGER PRIMARY KEY,
             date_local TEXT, 
             reading TEXT, 
             unit TEXT,
             county_id TEXT,
             state_id TEXT, 
             parameter TEXT) 

    ''')
    conn.commit()
    reading_data = get_AQ_data(email, param, bdate, edate, state_num, county_num)
    data = json.loads(reading_data)
    for index, reading in enumerate(data['Data']): 
        cur.execute('''INSERT OR IGNORE INTO Readings (reading_id, date_local, reading, unit, county_id, state_id, parameter) 
                VALUES (?,?, ?, ?, ?, ?, ?)''', (index, reading['date_local'], reading['sample_measurement'], reading['units_of_measure'], reading['county_code'], reading['state_code'], reading['parameter']))
    conn.commit()
    

def setUpTableState(email, param, bdate, edate, state_num, county_num, cur,conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS State 
            (state_id TEXT PRIMARY KEY, 
             state_name TEXT) 

    ''')
    conn.commit()
    state_data = get_AQ_data(email, param, bdate, edate, state_num, county_num)
    data = json.loads(state_data)
    for state in data['Data']: 
        cur.execute('''INSERT OR IGNORE INTO State (state_id, state_name) 
                VALUES (?,?)''', (state['state_code'], state['state']))
    conn.commit()

def setUpTableCounty(email, param, bdate, edate, state_num, county_num, cur,conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS County 
           (county_id TEXT PRIMARY KEY, 
            county_name TEXT, 
            state_id TEXT)
    ''')  
    conn.commit() 
    county_data = get_AQ_data(email, param, bdate, edate, state_num, county_num)
    data = json.loads(county_data)
    for county in data['Data']:
        cur.execute('''INSERT or IGNORE INTO County (county_id, county_name, state_id) 
                VALUES (?,?,?)''', (county['county_code'], county['county'], county['state_code']))
    conn.commit()


def setUpC19Country(country, caseType, cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS Covid (Country TEXT PRIMARY KEY, State TEXT, Status TEXT, Cases INTEGER, Date TEXT)')
    conn.commit()
    rows_added = 0 
    country_data = get_COVID_data(country, caseType)
    data_covid = json.loads(country_data)
    
    for data in data_covid:
        cur.execute('SELECT * FROM Covid WHERE State = ? AND Date = ?', (data['Province'], data['Date']))
        row_count = cur.rowcount 
        print(row_count)
        if(row_count == 0 and rows_added < 20):
            cur.execute('INSERT OR IGNORE INTO Covid (Country, State, Status, Cases, Date) VALUES (?, ?, ?, ?, ?)', (data['Country'], data['Province'], data['Status'], data['Cases'], data['Date']))
            rows_added += 1 
        elif(rows_added > 20):
            break
        elif(row_count > 0):
            continue
    conn.commit() 

def get_readings(cur, conn):
    cur.execute("SELECT reading_id, date_local, reading, unit, county_id, state_id, parameter FROM Readings")
    results1 = cur.fetchall()
    conn.commit()
    print(results1) 

def get_state(cur, conn):
    cur.execute("SELECT state_id, state_name FROM State")
    results2 = cur.fetchall()
    conn.commit()
    print(results2) 
    
def get_county(cur, conn):
    cur.execute("SELECT county_id, county_name, state_id FROM County")
    results3 = cur.fetchall()
    conn.commit()
    print(results3) 

def get_COVID_country(cur, conn):
    cur.execute("SELECT * FROM Covid")
    results4 = cur.fetchall()
    conn.commit()
    print(results4)








# class TestAllMethods(unittest.TestCase):

    # def setUp(self):
    #     path = os.path.dirname(os.path.abspath(__file__))
    #     self.conn = sqlite3.connect(path+'/'+__file__)
    #     self.cur = self.conn.cursor()

  


    # def test_table_state(self):
    #     cur.execute('SELECT * state_id, state_name FROM State')
    #     list_state = cur.fetchall()
    #     conn.commit()

    #     self.assertEqual(list_state[0][0], "36")

def main():
    cur, conn = setUpDatabase('Readings.db')
    setUpTableCounty('jessz@umich.edu', '88101', '20200101', '20200415', '06', '067', cur, conn)
    setUpReadingsTable('jessz@umich.edu', '88101', '20200101', '20200415', '06', '067', cur, conn)
    setUpTableState('jessz@umich.edu', '88101', '20200101', '20200415', '06', '067', cur, conn)
    setUpC19Country('united-states', 'confirmed', cur, conn)

    get_readings(cur, conn)
    get_state(cur, conn)
    get_county(cur, conn) 
    get_COVID_country(cur, conn)


    #get_COVID_data("United States", "confirmed")




if __name__ == "__main__":
    main()
    #unittest.main(verbosity = 2)

