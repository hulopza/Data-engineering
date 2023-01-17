import configparser


# CONFIGURATIONS
#ALL CONFIGURATION VARIABLES FOR CONNECTING TO REDSHIFT AND FOR FORMATTING TABLES FROM JSON FILES
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")
REGION = config.get("S3", "REGION")


# DROP TABLES
# SECTION FOR DROPPING TABLES IN CASE TABLES ARE CREATED AGAIN

staging_events_table_drop = "DROP TABLE IF EXISTS events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATING STAGING TABLES WITH RAW DATA
# These tables will be used to insert data into the database schema

staging_events_table_create= ("""
CREATE TABLE events_table(
    artist     VARCHAR(500),
    auth VARCHAR(20),
    firstName VARCHAR(20),
    gender VARCHAR(20) ,
    iteminSession integer ,
    lastName VARCHAR(200) ,
    lenght float(4),
    level VARCHAR(12) ,
    location VARCHAR(60),
    method VARCHAR(12),
    page VARCHAR(500),
    registration float(2),
    sessionid smallint,
    song VARCHAR(500),
    status int, 
    ts BIGINT ,
    userAgent VARCHAR(1000),
    userid int 

);
""")

staging_songs_table_create = ("""
CREATE TABLE songs_table(
    num_songs int ,
    artist_id VARCHAR(200),
    artist_latitude float(4),
    artist_longitude float(4),
    artist_location VARCHAR(1000),
    artist_name VARCHAR(1000),
    song_id VARCHAR(200),
    title VARCHAR(200),
    duration float(5),
    year int 

);
""")

#CREATING SCHEMA TABLES
#THESE TABLES ARE THE FINAL DATABASE TABLES TO BE FILLED BY THE STAGING TABLES

songplay_table_create = ("""
CREATE TABLE songplay_table(
    songplay_id int not null IDENTITY(0, 1),
    start_time timestamp not null,
    user_id int not null distkey,
    level VARCHAR(12) not null,
    song_id VARCHAR(2000) not null,
    artist_id VARCHAR(2000) not null,
    session_id int not null,
    location VARCHAR(2000) not null,
    user_agent VARCHAR(12),
    PRIMARY KEY(songplay_id)
    );

""")

user_table_create = ("""
CREATE TABLE user_table(
    user_id int not null sortkey distkey,
    first_name VARCHAR(2000) not null,
    last_name VARCHAR(2000) not null,
    gender VARCHAR(20) not null,
    level VARCHAR(12) not null,
    PRIMARY KEY(user_id)
);
""")

song_table_create = ("""
CREATE TABLE song_table(
    song_id int not null sortkey distkey,
    title VARCHAR(2000),
    artist_id VARCHAR(2000),
    year int not null,
    duration float(5),
    PRIMARY KEY(song_id)

);
""")

artist_table_create = ("""
CREATE TABLE artist_table(
    artist_id int not null sortkey distkey,
    name VARCHAR(2000) not null,
    location VARCHAR(2000) not null,
    lattitude float(4),
    longitude float(4),
    PRIMARY KEY(artist_id)

);

""")

time_table_create = ("""
CREATE TABLE time_table(
    start_time timestamp not null sortkey distkey,
    weekday int not null,
    hour time not null,
    day VARCHAR(10) not null,
    week int not null,
    year int not null,
    PRIMARY KEY(start_time)
    

    
);
""")

# COPYING DATA INTO STAGING TABLES

staging_events_copy = ("""
copy events_table from {}
credentials 'aws_iam_role={}'
format as json {}
region {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
copy songs_table from {}
credentials 'aws_iam_role={}'
format as json 'auto'
region {};
""").format(SONG_DATA, IAM_ROLE, REGION)



# SECTION FOR INSERTING DATA INTO FINAL SCHEMA TABLES 
songplay_table_insert = ("""

INSERT INTO songplay_table(
    SELECT CONVERT(VARCHAR(10), DATEADD(SECOND, e.ts/1000, '1970/1/1'), 105) + ' ' + FORMAT(DATEADD(SECOND, e.ts/1000, '1970/1/1'), 'h:mm:ss tt') AS start_time, 
    e.user_id, e.level, 
    s.song_id,
    s.artist_id, 
    e.session_id, 
    e.location, 
    e.user_agent

    FROM events_table e, songs_table s;

)

""")

user_table_insert = ("""
INSERT INTO user_table

SELECT DISTINCT userid, firstName, lastName, gender, level

FROM events_table;


""")

song_table_insert = ("""
INSERT INTO song_table

SELECT DISTINCT song_id, title, artist_id, year, duration

FROM songs_table;
""")

artist_table_insert = ("""
INSERT INTO artist_table

SELECT DISTINCT artist_id, artist_name, artist_location, artist_lattitude, artist_longitude

FROM songs_table;
""")

time_table_insert = ("""
INSERT INTO time_table

SELECT DISTINCT start_time,

DATENAME(WEEKDAY, DATE(start_time)) AS weekday,
HOUR(start_time) as hour,
DATEPART(WEEK, start_time) AS week,
DATEPART(year, start_time) AS year

FROM songplay_table;
)

""")

# QUERY LISTS
#LIST OF QUERIES TO BE IMPORTED INTO create_tables.py and etl.py

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
final_tables = ['songplay_table', 'user_table', 'song_table', 'artist_table', 'time_table']
