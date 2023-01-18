

# Data Warehouse with AWS - Portfolio project

## Introduction

The purpose of this project is to showcase the skills learnt for building a data warehouse with AWS Redshift. The data used for building the database is obtained from three sources:
- Song data: *s3://udacity-dend/song_data*
- Log data: *s3://udacity-dend/log_data*
- Meta data: *s3://udacity-dend/log_json_path.json*

There are three main files that build the data warehouse:
- *create_tables.py*: Calls queries for creating tables in the database.
- *etl.py* : Processes raw data for building final tables.
- *sql_queries.py* : File containing all sql queries used in creating tables and in the ETL process.


## Creating staging and database tables (create_tables.py)

### Staging tables

Two staging tables are created to be filled with raw data from s3 sources:

1. **staging_events_table**: Table containing all raw log data.
2. **staging_songs_data**: Table containing all raw song data.


### Database schema tables

The database schema consists of one fact table and four dimension tables. 
All dimension tables were constructed from unique values in the datasets and are connected with a distkey to the fact table according to the uniqueness of the data (e.g. song_table uses song_id as the distribution key due to each song having unique ids and less repetitipns in the dataset).

![db schema] (./Shcema_diagram.png 'Database schema')

The fact table, songplay_table, contains all distribution keys and all data entries.

1. **songplay_table** : Records in event data associated with song plays. Columns: songplay_id(PRIMARY KEY), start_time, user_id (DISTKEY), level, song_id, artist_id, session_id, location, user_agent.

2. **user_table**: Users in the app. Columns: user_id (PRIMARY KEY, DISTKEY), first_name, last_name, gender, level

3. **song_table** Songs in music database. Columns: song_id (DISTKEY, PRIMARY LEY), title, artist_id, year, duration.

4. **artist_table**: Artists in music database. Columns: artist_id (DISTKEY, PRIMARY KEY), name, location, lattitude, longitude.

5. **time_table**: Timestamps of records in songplays broken down into specific units. Columns: start_time (DISTKEY, PRIMARY KEY), hour, day, week, month, year, weekday. 

These five tables are created in the *create_tables.py* file and inports queries from *sql_queries.py* 


## The ETL process (etl.py)
The ETL process consists of the following:

**Extraction & Loading**: The song data, log data and meta data files are copied into the staging tables.

**Transforming**: Data in the staging tables is then inserted into the schema tables and transformed accoringly, as is the case for the timestamp data.



## Other files (dwh.cfg, check_errors.py)

**dwh.cfg**: This file contains configuration variables for connecting to Redshift and the S3 datasets.

**check_errors.py**: This file was used for querying the *stl_load_errors* table for debugging.


