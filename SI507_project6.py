# Import statements
import psycopg2
import psycopg2.extras
import csv
import uuid
from config import *

# Write code / functions to set up database connection and cursor here.
def get_connection_and_cursor():
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor


# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():
    # Invovles DDL commands
    # DDL --> Data Definition Language
    # CREATE, DROP, ALTER, RENAME, TRUNCATE

    conn, cur = get_connection_and_cursor()

    cur.execute("DROP TABLE IF EXISTS Sites")

    # ID (SERIAL)
    # Name (VARCHAR up to 128 chars, UNIQUE)
    # Type [e.g. "National Lakeshore" or "National Park"] (VARCHAR up to 128 chars)
    # State_ID (INTEGER - FOREIGN KEY REFERENCING States)
    # Location (VARCHAR up to 255 chars)
    # Description (TEXT)
    cur.execute("""CREATE TABLE Sites(
        ID SERIAL PRIMARY KEY,
        Name VARCHAR(128),
        Type VARCHAR(128),
        State_ID INTEGER,
        Location VARCHAR(255),
        Description TEXT
    )""")

    # ID (SERIAL)
    # Name (VARCHAR up to 40 chars, UNIQUE)
    cur.execute("DROP TABLE IF EXISTS States")
    cur.execute("""CREATE TABLE States(
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(40)
    )""")

    conn.commit()
    print('Setup database complete')

# Write code / functions to deal with CSV files and insert data into the database here.
def load_cache(file_name):
    cache_list = []
    try:
        with open(file_name, newline='') as pscfile:
            reader = csv.DictReader(pscfile)
            for row in reader:
                dict = {}
                dict[NAME] = row[NAME]
                dict[LOCATION] = row[LOCATION]
                dict[TYPE] = row[TYPE]
                dict[ADDRESS] = row[ADDRESS]
                dict[DESCRIPTION] = row[DESCRIPTION]
                cache_list.append(dict)
    except:
        cache_list = []
    return cache_list

# Make sure to commit your database changes with .commit() on the database connection.
def insertStates(cur, data_list):
    for each in data_list:
        cur.execute("""INSERT INTO
            States (ID, Name)
            VALUES(%s, %s)
            ON CONFLICT DO NOTHING""",
            (uuid.uuid4().fields[1], each))

def insertSites(cur, data_list, stateID):
    for each in data_list:
        # print("uuid:",uuid.uuid4().fields[1])
        # print("name:",each[NAME])
        # print("type:",each[TYPE])
        # print("stateId:",stateID)
        # print("location:",each[LOCATION])
        # print("description:",each[DESCRIPTION])
        cur.execute("""INSERT INTO
            Sites (ID, Name, Type, State_ID, Location, Description)
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING""",
            (uuid.uuid4().fields[1], each[NAME], each[TYPE], stateID, each[LOCATION], each[DESCRIPTION]))

def get_and_insert_data():
    ark_dict = load_cache(arkansas_csv)
    mich_dict = load_cache(michigan_csv)
    cali_dict = load_cache(california_csv)

    # print('--- arkansas data: ---')
    # print(ark_dict)
    #
    # print('--- michigan data: ---')
    # print(mich_dict)
    #
    # print('--- california data: ---')
    # print(cali_dict)

    # Insert States data
    conn, cur = get_connection_and_cursor()
    states_list = ['arkansas', 'michigan', 'california']
    insertStates(cur, states_list)

    # Insert Sites data
    cur.execute("SELECT * FROM States WHERE Name = 'michigan'")
    michigan_stateId = cur.fetchone()['id']
    # print(michigan_stateId)
    insertSites(cur, mich_dict, michigan_stateId)

    cur.execute("SELECT * FROM States WHERE Name = 'arkansas'")
    arkansas_stateId = cur.fetchone()['id']
    # print(arkansas_stateId)
    insertSites(cur, ark_dict, arkansas_stateId)

    cur.execute("SELECT * FROM States WHERE Name = 'california'")
    california_stateId = cur.fetchone()['id']
    # print(california_stateId)
    insertSites(cur, cali_dict, california_stateId)

    conn.commit()



# Write code to be invoked here (e.g. invoking any functions you wrote above)
# get_and_insert_data()


# Write code to make queries and save data in variables here.
conn, cur = get_connection_and_cursor()

# all_locations
all_locations = []
cur.execute("SELECT Location FROM Sites")
for each in cur.fetchall():
    all_locations.append(each['location'])
print(all_locations, " Length:",len(all_locations))

# beautiful_sites
beautiful_sites = []
cur.execute("SELECT Name FROM Sites WHERE sites.description LIKE '%beautiful%'")
for each in cur.fetchall():
    beautiful_sites.append(each['name'])
print(beautiful_sites)

# natl_lakeshores
cur.execute("SELECT COUNT(Name) FROM Sites WHERE sites.type LIKE 'National Lakeshore'")
natl_lakeshores = cur.fetchone()['count']
print("Length:", natl_lakeshores)

# michigan_names
michigan_names = []
cur.execute("SELECT sites.name FROM Sites INNER JOIN States ON sites.state_id = states.id AND states.name LIKE 'michigan'")
for each in cur.fetchall():
    michigan_names.append(each['name'])
print(michigan_names)

# total_number_arkansas
cur.execute("SELECT COUNT(sites.name) FROM Sites INNER JOIN States ON sites.state_id = states.id AND states.name LIKE 'arkansas'")
total_number_arkansas = cur.fetchone()['count']
print(total_number_arkansas)
