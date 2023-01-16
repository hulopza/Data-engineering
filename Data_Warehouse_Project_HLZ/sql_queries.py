import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")
REGION = config.get("S3", "REGION")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE events_table(
    artist     VARCHAR(100),
    auth VARCHAR(20) not null,
    firstName VARCHAR(20) not null,
    gender VARCHAR(20) not null,
    iteminSession integer not null,
    lastName VARCHAR(20) not null,
    lenght float(4) not null,
    level VARCHAR(12) not null,
    location VARCHAR(60) not null,
    method VARCHAR(12),
    page VARCHAR(12),
    registration float(2) not null,
    sessionid smallint not null,
    song VARCHAR(50) not null,
    status int not null,
    ts BIGINT not null,
    userAgent VARCHAR(50) not null,
    userid int not null

);
""")

staging_songs_table_create = ("""
CREATE TABLE songs_table(
    num_songs int not null,
    artist_id VARCHAR(50) not null,
    artist_latitude float(4),
    artist_longitude float(4),
    artist_location VARCHAR(50),
    artist_name VARCHAR(12),
    song_id VARCHAR(50) not null,
    title VARCHAR(50) not null,
    duration float(5) not null,
    year int not null

);
""")

songplay_table_create = ("""
CREATE TABLE songplay_table(
    songplay_id integer not null sortkey,
    start_time timestamp not null,
    user_id int not null distkey,
    level VARCHAR(12) not null,
    song_id VARCHAR(50) not null,
    artist_id VARCHAR(50) not null,
    session_id int not null,
    location VARCHAR(50) not null,
    user_agent VARCHAR(12)
    );

""")

user_table_create = ("""
CREATE TABLE user_table(
    user_id int not null sortkey distkey,
    first_name VARCHAR(12) not null,
    last_name VARCHAR(12) not null,
    gender VARCHAR(12) not null,
    level VARCHAR(12) not null
);
""")

song_table_create = ("""
CREATE TABLE song_table(
    song_id int not null sortkey distkey,
    title VARCHAR(50) not null,
    artist_id int not null,
    year int not null,
    duration float(5)

);
""")

artist_table_create = ("""
CREATE TABLE artist_table(
    artist_id int not null sortkey distkey,
    name VARCHAR(12) not null,
    location VARCHAR(50) not null,
    lattitude float(4),
    longitude float(4)

);

""")

time_table_create = ("""
CREATE TABLE time_table(
    start_time timestamp not null sortkey distkey,
    weekday int not null,
    hour time not null,
    day VARCHAR(10) not null,
    week int not null,
    year int not null
    

    
);
""")

# STAGING TABLES

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

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
