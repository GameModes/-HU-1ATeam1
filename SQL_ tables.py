import psycopg2

hostname = 'localhost'
username = 'postgres'
password = 'amaryllis'
databasename = 'test_huwebshop'

#Verbinden met een database op Postgresql
def connect_db(hostname, username, password, databasename):
    conn = psycopg2.connect(host=hostname, user=username, password=password, database=databasename)
    return conn

#Deze functie verwijder tabellen als die in database zit. Hiermee krijgt je altijd een updated database.
def drop_tables():
    'Verwijder tabel in de Postgresql database als die table bestaat'
    conn = connect_db(hostname, username, password, databasename)
    cur = conn.cursor()
    try:
        cur.execute('''
        drop table if exists profiles;
        drop table if exists sessions;
        drop table if exists prosess;
        ''')

        conn.commit()
        cur.close()
        print('gelukt')
    except:
        print('Error')


# Creer tabel Profiles
def table_profile():
    conn = connect_db(hostname, username, password, databasename)
    cur = conn.cursor()

    try:
        # Query uitvoeren naar de database
        cur.execute('''
        Create Table Profiles(
        profile_id varchar(255), -- profile_id == _id from oollection profiles in Mongodb
        buids varchar (255),
        PRIMARY KEY (profile_id));
        ''')

        conn.commit() # commit aanpassingen aan de database
        cur.close()
        print('Profiles: gelukt')
    except:
        print('Error')

#Creer table Sessions
def table_session():
    conn = connect_db(hostname, username, password, databasename)
    cur = conn.cursor()
    try:
        cur.execute('''
        Create Table Sessions(
        session_id varchar(255),--session_id == _id from oollection session in Mongodb
        session_start date, 
        session_ends  date,
        buid varchar(255),
        PRIMARY KEY (session_id));
        ''')
        conn.commit()
        cur.close()
        print('Session: gelukt')
    except:
        print('Error')

#Creer tabel profile-sessions die wordt prosess
def table_profile_sessions():
    conn = connect_db(hostname, username, password, databasename)
    cur = conn.cursor()

    try:
        cur.execute('''
        Create Table Prosess(
        profile_id  varchar(255), 
        buid varchar (255),
        FOREIGN KEY (profile_id) REFERENCES Profiles(profile_id)
        );
        ''')
        print('Tabel profiles_sessions(Prosess) is gecreeerd')
    except:
        print('Error')

#--------------------------------------------------------------------
#Controle

drop_tables()
table_profile()
table_session()
table_profile_sessions()


