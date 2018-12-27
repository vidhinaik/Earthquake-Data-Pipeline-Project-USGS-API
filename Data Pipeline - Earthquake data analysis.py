# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 01:52:14 2018

@author: Vidhi Naik
"""
import os
import json
import requests
import pandas as pd
from datetime import datetime
import sqlite3

os.chdir("C://Users/u357994/Documents/Python version/")
f = open("Logfile.txt", "a")
#Function to read data into json file and save data 
def json_to_database(start_time,end_time,connection_object):
        status = True
        r = requests.get("http://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson", params=dict(
            starttime = start_time,
            endtime = end_time
        ))
        
        #status code 200 indicates successful execution of requests.get function
        if(r.status_code == 200):
            data = r.content.decode('utf8')
            json_temp = json.loads(data)
        else:
            status = False
            print("Received an error while getting the response from USGS API: Error code - " , str(r.status_code))
            f.write("\nReceived an error while getting the response from USGS API: Error code - " + str(r.status_code))
        rowNumber = 0
        sql = '''INSERT INTO EarthquakeData01(Event_id,Date,Magnitude,Details) VALUES("%s","%s","%.2f","%s");'''
        for event in json_temp['features']:
                event_id = event['id']
                magnitude = event['properties']['mag']
                time_occurred = datetime.utcfromtimestamp(event['properties']['time']/1000)
                tstr = time_occurred.strftime('%Y-%m-%d %H:%M:%S')
                place = event['properties']['place']           
                rowNumber += 1
                try:
                    cursor_object = connection_object.cursor()
                    row = (event_id,tstr,float(magnitude),place)
                    #print(row)
                    cursor_object.execute(sql %row)
                except TypeError:
                     print("Error in values: Omitted row number -", str(rowNumber))
                     f.write("\nError in values: Omitted row number -"+ str(rowNumber))
                except Exception as e:
                     print(e) 
                     f.write("\n"+str(e))
        return status
    
def create_connection(database):
    try:
        conn = sqlite3.connect(database)
        f.write("\nSuccessfully connected to "+database+" database.")
        return conn
    except Exception as e:
        print(e)
        f.write("\n"+str(e))
    return None
 
def create_table(conn, create_table_str):
    try:
        c=conn.cursor()
        c.execute(create_table_str)
        f.write("\nCreate table string: "+create_table_str)
        f.write("\nSuccessfully created the table.")        
    except Exception as e:
        print(e)
        f.write("\n"+str(e))

def execute_query(conn, query):
    try:
        c=conn.cursor()
        c.execute(query)
        rows = c.fetchall()
        f.write("\nQuery: "+query)
        f.write("\nSuccessfully executed the query.") 
        return rows
    except Exception as e:
        print(e)
        f.write("\n"+str(e))
        return "Failed"
        
def fetch_data_dataframe(conn, query):
    try:
        dataframe = pd.read_sql_query(query,conn)
        f.write("\nQuery: "+query)
        f.write("\nSuccessfully executed the query.") 
        return dataframe
    except Exception as e:
        print(e)
        f.write("\n"+str(e))
        return "Failed"
        
def buckets(x):
    #Create categories based on magnitude
    if x['Magnitude'] < 1:
        return '0-1'
    elif x['Magnitude'] < 2:
        return '1-2'
    elif x['Magnitude'] < 3:
        return '2-3'
    elif x['Magnitude'] < 4:
        return '3-4'
    elif x['Magnitude'] < 5:
        return '4-5'
    elif x['Magnitude'] < 6:
        return '5-6'
    else:
        return '>6'
        
def get_analysis_biggest_earthquake(event_id):
    r = requests.get("http://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson", params=dict(
            eventid = "us2000ahv0"
        ))
    if(r.status_code == 200):
            data = r.content.decode('utf8')
            json_temp = json.loads(data)
    else:
            status = False
            print("Received an error while getting the response from USGS API: Error code - " , str(r.status_code))
            f.write("\nReceived an error while getting the response from USGS API: Error code - " + str(r.status_code))
    
    longitude = json_temp['geometry']['coordinates'][0]
    print(longitude)
    latitude = json_temp['geometry']['coordinates'][1]
    print(latitude)
    depth = json_temp['geometry']['coordinates'][2]
    print(depth)
    magnitude_type = json_temp['properties']['magType']
    print(magnitude_type)   
    return longitude,latitude,depth,magnitude_type
    
def main():
    #Create table string
    create_table_str = ''' CREATE TABLE IF NOT EXISTS EarthquakeData01 (
                                            Event_id text PRIMARY KEY,
                                            Date text,
                                            Magnitude real,
                                            Details text
                                        ); '''
    database = "C:\\Sqlite\QuizDatabase.db"
    
    #Create database connection
    connection_object = create_connection(database)
    
    #Create table if not exists
    if connection_object is not None:
        create_table(connection_object, create_table_str)
    else:
         print("Error in creating connection with the Earthquake database!")
         f.write("\nError in creating connection with the Earthquake database!")
        
    #Dividing 2017 into months because the USGS API can handle limited number of rows in each request
    date={'startdate':['2017-01-01','2017-02-01','2017-03-01','2017-04-01','2017-05-01','2017-06-01',
                        '2017-07-01','2017-08-01','2017-09-01','2017-10-01','2017-11-01','2017-12-01'],
            'enddate':['2017-01-31','2017-02-28','2017-03-31','2017-04-30','2017-05-31','2017-06-30',
                     '2017-07-31','2017-08-31','2017-09-30','2017-10-31','2017-11-30','2017-12-31'],
            'month':['January','February','March','April','May','June',
                     'July','August','September','October','November','December']}
    date_df=pd.DataFrame(data=date)
    for i in range(12):
        print(date_df['startdate'].iloc[i]+" "+date_df['enddate'].iloc[i])
        Status=json_to_database(date_df['startdate'].iloc[i],date_df['enddate'].iloc[i],connection_object)
        if(Status == True):
             print("Successfully inserted "+ date_df['month'].iloc[i] +" data.")
             f.write("\nSuccessfully inserted "+ date_df['month'].iloc[i] +" data.")
        else:
             print("Error in inserting "+ date_df['month'].iloc[i] +" data.")  
             f.write("\nError in inserting "+ date_df['month'].iloc[i] +" data.")  
    connection_object.commit()
    
    #Query to find the earthquake with maximum magnitude(Biggest earthquake in 2017)
    query = '''select Event_id, Date, Magnitude, Details from EarthquakeData01 where Magnitude in (select max(Magnitude) from EarthquakeData01);'''
    rows = execute_query(connection_object,query)
    #if multiple earthquakes had same magnitude
    if len(rows)==1:
        longitude,latitude,depth,magnitude_type=get_analysis_biggest_earthquake(rows[0][0])
        print(rows[0][0])
        print("The biggest earthquake in 2017 occurred at "+rows[0][3]+" on "+rows[0][1]+" with magnitude of "+str(rows[0][2])+". The decimal degrees longitude is "+str(longitude)+", decimal degrees latitude is "+str(latitude)+ " and depth is "+str(depth)+" kms. The method used to calculate the preferred magnitude for the event was "+magnitude_type+"." )
        f.write("The biggest earthquake in 2017 occurred at "+rows[0][3]+" on "+rows[0][1]+" with a magnitude of "+str(rows[0][2])+". The decimal degrees longitude is "+str(longitude)+", decimal degrees latitude is "+str(latitude)+ " and depth is "+str(depth)+" km. The method used to calculate the preferred magnitude for the event was "+magnitude_type+"." )
    else:
        print("The biggest earthquakes in 2017 occurred at "+str(len(rows))+" locations with magnitude of "+str(rows[0][2])+".")
    
    #Fetch data from database and save in dataframe to export in excel files
    query01 = '''select * from EarthquakeData01;'''
    dataframe01 = pd.read_sql_query(query01,connection_object)
    dataframe01.head()
    connection_object.close()
    
    #Create category based on Magnitude.
    #An earthquake with negative magnitudes is very small earthquake that is not felt by humans. 
    #Included those earthquakes in 0-1 bucket.
    dataframe01['Range_of_Magnitude'] = dataframe01.apply (lambda row: buckets (row),axis=1)
    f.write("\nSuccessfully added the category column.")
    dataframe01.to_csv("FinalEarthquakeData01.csv")
    dataframe01.to_excel("FinalEarthquakeData01.xlsx")
    f.write("\nExported data for visualization.")
    f.close()
    

if __name__=="__main__":
    main()