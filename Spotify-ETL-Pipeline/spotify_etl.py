import pandas as pd
import sqlalchemy
import requests
import json
from datetime import datetime
import datetime as dt
import time
import pyodbc

#data validation functions
def check_valid_data(df: pd.DataFrame) -> bool:
    #check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    #check if primary key field is unique
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key field contains duplicate values.")

    if df.isnull().values.any():
        raise Exception("Null values found.")

    #check that all timestamps are after 1/1/2020
    date_time = dt.datetime(2020,1,1,1,1,1)
    begin_date = date_time.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if dt.datetime.strptime(timestamp, '%Y-%m-%d') < begin_date:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

def run_spotify_etl():
    #define constants
    DRIVER_NAME = '{SQL Server}'
    SERVER_NAME = 'DESKTOP-R5Q6CSR\SQLSERVER2019'
    MASTER_DATABASE = 'master'
    NEW_DATABASE = 'SpotifyProject'
    TOKEN = 'BQCQF9ZftVZ9EJtVMqdpOKwgPwVd2VVeD5Ae1FnWiXdYLqifSYHc'
    
    #catch API results
    headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",
    "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    date_time = dt.datetime(2020,1,1,1,1,1)
    unix_date_time = int(time.mktime(date_time.timetuple()))

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={date}".format(date=unix_date_time), headers = headers)
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    #extract relevant fields from json object
    for song in data['items']:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    #store values in a dictionary
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)

    #validation
    if check_valid_data(song_df):
        print("All data validation checks passed.")

    #connect to SQL Server master database to create a new database
    try:
        connection = pyodbc.connect(driver=DRIVER_NAME, server=SERVER_NAME, database=MASTER_DATABASE,
                                    trusted_connection='true', autocommit='true')
        print("\nConnection successful!")
    except pyodbc.Error as e:
        print("Connection failed!\n")
        print(e)

    #create database
    cursor = connection.cursor()
    cursor.execute("DROP DATABASE SpotifyProject")
    cursor.execute("CREATE DATABASE SpotifyProject")
    connection.close()

    #connect to new database created
    try:
        connection = pyodbc.connect(driver=DRIVER_NAME, server=SERVER_NAME, database=NEW_DATABASE,
                                    trusted_connection='true', autocommit='true')
        print("\nConnection successful!")
    except pyodbc.Error as e:
        print("Connection failed!\n")
        print(e)

    #create tables
    cursor = connection.cursor()
    query = """
    CREATE TABLE MyPlayedTracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT PK_MyPlayedTracks PRIMARY KEY (played_at)
    )
    """
    cursor.execute(query)
    connection.close()

    #create sqlalchemy engine to use pandas to_sql method
    try:
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{SERVER_NAME}/{NEW_DATABASE}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
        print("\nConnection successful!")
    except pyodbc.Error as e:
        print("Connection failed!")
        print(e)

    #load data
    try:
        song_df.to_sql('MyPlayedTracks', con=engine, if_exists='append', index=False)
    except pyodbc.Error as e:
        print(e)

    print(f"Data import to database {NEW_DATABASE} has completed successfully.")