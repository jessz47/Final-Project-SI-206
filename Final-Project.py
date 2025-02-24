import unittest
import sqlite3
import json
import os
import requests
import pandas 
import matplotlib
import matplotlib.pyplot as plt
from scipy.stats import pearsonr 
#Names: Jessica Zhang, Darian Chang
#Project Topic: How the Coronavirus is affecting CO2 Levels in the Atmosphere in Delhi, India (using AQI) and Int'l Flights 
API_AQ = 'bolebird88'


#sets up database 
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+ db_name)
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
    cur.execute('DROP TABLE IF EXISTS Readings')
    cur.execute('''CREATE TABLE Readings 
             (date_local TEXT, 
             reading TEXT, 
             unit TEXT,
             county_id TEXT,
             state_id TEXT, 
             parameter TEXT) 

    ''')
    conn.commit()

def insertIntoReadingsTable(email, param, bdate, edate, state_num, county_num, cur, conn):
    rows_added = 0
    reading_data = get_AQ_data(email, param, bdate, edate, state_num, county_num)
    data_reading = json.loads(reading_data)

    for reading in data_reading['Data']:
        cur.execute('SELECT * FROM Readings WHERE date_local = ? AND reading = ? AND unit = ?', (reading['date_local'], reading['sample_measurement'], reading['units_of_measure']))
        result = cur.fetchone()

        if(result):
            continue 
        elif(rows_added < 20):
            cur.execute('''INSERT INTO Readings (date_local, reading, unit, county_id, state_id, parameter) 
                VALUES (?, ?, ?, ?, ?, ?)''', (reading['date_local'], reading['sample_measurement'], reading['units_of_measure'], reading['county_code'], reading['state_code'], reading['parameter']))
            rows_added += 1 
        elif(rows_added > 20):
            break

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
            county_name TEXT UNIQUE, 
            state_id TEXT)
    ''')  

    conn.commit() 
    county_data = get_AQ_data(email, param, bdate, edate, state_num, county_num)
    data = json.loads(county_data)
    for county in data['Data']:
        cur.execute('''INSERT or IGNORE INTO County (county_id, county_name, state_id) 
                VALUES (?,?,?)''', (county['county_code'], county['county'], county['state_code']))
    conn.commit()

def createC19Table(country, caseType, cur, conn):
    cur.execute('DROP TABLE IF EXISTS Covid')
    cur.execute('CREATE TABLE Covid (Country TEXT, State TEXT, Status TEXT, Cases INTEGER, Date TEXT)')
    conn.commit()

def insertIntoC19Table(country, caseType, cur, conn):
    rows_added = 0
    country_data = get_COVID_data(country, caseType)
    data_covid = json.loads(country_data)
    

    for data in data_covid:
        cur.execute('SELECT * FROM Covid WHERE Country = ? AND State = ? AND Cases = ? AND Date = ?', (data['Country'], data['Province'], data['Cases'], data['Date']))
        result = cur.fetchone()

        if(result):
            continue 
        elif(rows_added < 20):
            cur.execute('INSERT INTO Covid (Country, State, Status, Cases, Date) VALUES (?, ?, ?, ?, ?)', (data['Country'], data['Province'], data['Status'], data['Cases'], data['Date']))
            rows_added += 1 
        elif(rows_added > 20):
            break

    conn.commit() 

def get_AQI(cur, conn):
    cur.execute("SELECT reading FROM Readings")
    results = cur.fetchmany(size = 75)
    return results 

def get_cases(cur, conn):
    cur.execute("SELECT Cases FROM Cov  id")
    results = cur.fetchmany(size = 75)
    return results

def get_pearsonCorrelation(cur, conn):
    results1 = get_AQI(cur, conn)
    results2 = get_cases(cur, conn)
    new_list1 = [float(x[0]) for x in results1 if type(x[0]) is str]
    new_list2 = [x[0] for x in results2 if type(x[0]) is int] 
    if(len(new_list1) > len(new_list2)):
        new_list1 = new_list1[:len(new_list2)]
    else:
        new_list2 = new_list2[:len(new_list1)]
    corr, _ = pearsonr(new_list1, new_list2)
    return corr 

def writeData(filename, data):
    fpath = "/Users/Darian/Desktop/Sophomore Year/Winter 2020/SI 206/Final-Project-SI-206/" + filename
    with open(fpath, "w+") as fp:
        fp.write("Pearson Coefficient Between AQI and Number of Covid19 Cases: " + str(data))
    fp.close()
    
            
def get_readings(cur, conn):
    cur.execute("SELECT date_local, reading, unit, county_id, state_id, parameter FROM Readings")
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


#CALCULATIONS
def jointables(cur, conn):
    cur.execute('''
                SELECT Readings.date_local, Readings.reading, Readings.county_id
                FROM Readings
                JOIN County  
                WHERE Readings.county_id = County.county_id
                 '''
    )
    conn.commit()

    results = cur.fetchall()
    return results 




#VISUALIZATIONS
def get_AQ_dates(cur,conn):
    cur.execute( '''
        SELECT date_local, reading
        FROM Readings
        WHERE date_local >= "2020-01-01"
        AND date_local < "2020-04-20" 
    ''')
    conn.commit()
    dates_data = cur.fetchall()
    list_dates = []
    for i in dates_data:
        yearData = i[0]
        list_dates.append(yearData)
    return list_dates

def get_AQ_readings(cur, conn):
    cur.execute( '''
        SELECT reading
        FROM Readings
    ''')
    conn.commit()
    reading_data = cur.fetchall()
    list_readings = []
    for i in reading_data:
        if i[0] == None :
            list_readings.append(0)
        else :
            list_readings.append(float(i[0]))
    return list_readings

def get_covid_dates(cur, conn):
    cur.execute( '''
    SELECT Date
    FROM Covid
    ''')
    conn.commit()
    case_dates = cur.fetchall()
    list_date_time = []
    list_date = []
    for i in case_dates:
        list_date_time.append(i[0])
    for i in list_date_time:
        list_date.append(i[:10])
    return list_date

def get_covid_cases(cur, conn):
    cur.execute( '''
    SELECT Cases
    FROM Covid
    ''')
    conn.commit()
    cases = cur.fetchall()
    list_cases = []
    for i in cases:
        list_cases.append(float(i[0]))
    return list_cases


def createscatterplotsAQ(cur, conn):
    x_values_unsorted = get_AQ_dates(cur,conn)
    y_values_unsorted = get_AQ_readings(cur, conn)

    list_sorted = []
    for i in range(len(x_values_unsorted)):
        list_sorted.append([x_values_unsorted[i], y_values_unsorted[i]])
    list_sorted = sorted(list_sorted, key = lambda x:x[0])

    x_val = []
    y_val = []
    for item in list_sorted:
        x_val.append(item[0])
        y_val.append(item[1])

    second_x_unsorted = get_covid_dates(cur,conn)
    second_y_unsorted = get_covid_cases(cur,conn)
    list_sorted_two = []
    for i in range(len(x_values_unsorted)):
        list_sorted_two.append([second_x_unsorted[i], second_y_unsorted[i]])
    list_sorted_two = sorted(list_sorted_two, key = lambda x:x[0])
    x = []
    y = []
    for item in list_sorted_two:
        x.append(item[0])
        y.append(item[1])


    ax = plt.gca()
    plt.scatter(x_val, y_val)
    plt.scatter(x, y)

def create_area_plot(cur,conn):
    x_values_unsorted = get_AQ_dates(cur,conn)
    y_values_unsorted = get_AQ_readings(cur, conn)

    list_sorted = []
    for i in range(len(x_values_unsorted)):
        list_sorted.append([x_values_unsorted[i], y_values_unsorted[i]])
    list_sorted = sorted(list_sorted, key = lambda x:x[0])

    x_val = []
    y_val = []
    for item in list_sorted:
        x_val.append(item[0])
        y_val.append(item[1])

    second_x_unsorted = get_covid_dates(cur,conn)
    second_y_unsorted = get_covid_cases(cur,conn)
    list_sorted_two = []
    for i in range(len(x_values_unsorted)):
        list_sorted_two.append([second_x_unsorted[i], second_y_unsorted[i]])
    list_sorted_two = sorted(list_sorted_two, key = lambda x:x[0])
    x = []
    y = []
    for item in list_sorted_two:
        x.append(item[0])
        y.append(item[1])
    
    
    plt.scatter(x_val, y_val, c='purple', alpha=0.2)
    plt.plot(x,y)
    plt.fill_between(x,y, color="skyblue", alpha = 0.3)
    plt.title("CoronaVirus Cases")
    

    plt.show()
    #plt.savefig("plt.png")
     




def main():
    cur, conn = setUpDatabase('Readings.db')
    setUpTableCounty('jessz@umich.edu', '88101', '20200101', '20200415', '17', '031', cur, conn)
    setUpReadingsTable('jessz@umich.edu', '88101', '20200101', '20200415', '17', '031', cur, conn)
    setUpTableState('jessz@umich.edu', '88101', '20200101', '20200415', '17', '031', cur, conn)

    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '17', '031', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '17', '031', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '17', '031', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '17', '031', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '17', '031', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '36', '061', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '36', '061', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '36', '061', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '36', '061', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '26', '163', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '26', '163', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '26', '163', cur, conn)
    insertIntoReadingsTable('jessz@umich.edu', '88101', '20200101', '20200419', '26', '163', cur, conn)



    createC19Table('Canada', 'confirmed', cur, conn)
    
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Canada', 'confirmed', cur, conn)
    insertIntoC19Table('Italy', 'confirmed', cur, conn)
    insertIntoC19Table('Italy', 'confirmed', cur, conn)
    insertIntoC19Table('Italy', 'confirmed', cur, conn)


    get_readings(cur, conn)
    get_state(cur, conn)
    get_county(cur, conn) 
    get_COVID_country(cur, conn)
    

    data = get_pearsonCorrelation(cur, conn)
    writeData("test.txt", data)
    createscatterplotsAQ(cur, conn)
    create_area_plot(cur, conn)

    



if __name__ == "__main__":
    main()
    #unittest.main(verbosity = 2)

