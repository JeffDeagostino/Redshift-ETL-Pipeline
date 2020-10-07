import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN= config.get("IAM_ROLE","ARN")
S3_LOG_DATA = config.get("S3","LOG_DATA")
S3_LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
S3_SONG_DATA = config.get("S3","SONG_DATA")

# DROP SCHEMAS

fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")


# CREATE SCHEMAS

fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables authorization dwhuser")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables authorization dwhuser")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables authorization dwhuser")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_tables.staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_tables.staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_tables.songplays;"
user_table_drop = "DROP TABLE IF EXISTS dimension_tables.users;"
song_table_drop = "DROP TABLE IF EXISTS dimension_tables.songs;"
artist_table_drop = "DROP TABLE IF EXISTS dimension_tables.artists;"
time_table_drop = "DROP TABLE IF EXISTS dimension_tables.time;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_tables.staging_events (
event_key INT IDENTITY(0,1), 
artist VARCHAR(255),
auth VARCHAR(255),
firstName VARCHAR(255),
gender VARCHAR(255),
itemInSession INT,
lastName VARCHAR(255),
length FLOAT,
level VARCHAR(255),
location VARCHAR(255),
method VARCHAR(255),
page VARCHAR(255),
registration FLOAT,
sessionId INT,
song VARCHAR(255),
status INT,
ts BIGINT,
userAgent VARCHAR(255),
userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_tables.staging_songs (
song_key INT IDENTITY(0,1), 
num_songs INT,
artist_id VARCHAR(255),
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR(255),
artist_name VARCHAR(255),
song_id VARCHAR(255),
title VARCHAR(255),
duration NUMERIC,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_tables.songplays (
songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP NOT NULL, 
user_id INT NOT NULL,
level VARCHAR, 
song_id VARCHAR(255) distkey, 
artist_id VARCHAR(255), 
session_id INT,
location VARCHAR(255), 
user_agent VARCHAR(255));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimension_tables.users (
user_id INT PRIMARY KEY, 
first_name VARCHAR(255), 
last_name VARCHAR(255), 
gender VARCHAR(10), 
level VARCHAR(255));
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS dimension_tables.songs (
song_id VARCHAR(255) PRIMARY KEY,
title VARCHAR(255),
artist_id VARCHAR(255),
year int,
duration FLOAT
)
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS dimension_tables.artists (
artist_id VARCHAR(255) PRIMARY KEY,
name VARCHAR(255),
location VARCHAR(255),
latitude FLOAT,
longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimension_tables.time (
start_time TIMESTAMP NOT NULL PRIMARY KEY, 
hour INT, 
day INT, 
week INT, 
month VARCHAR(20),
year INT,
weekday VARCHAR(20));
""")

# STAGING TABLES

staging_events_copy = (""" 
copy staging_tables.staging_events 
from {} 
iam_role {} 
compupdate off 
region 'us-west-2' 
json {}  
""").format(S3_LOG_DATA, DWH_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = (""" 
copy staging_tables.staging_songs 
from {} 
iam_role {} 
compupdate off 
region 'us-west-2' 
FORMAT AS json 'auto'
""").format(S3_SONG_DATA, DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_tables.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.useragent
FROM  staging_tables.staging_events se, staging_tables.staging_songs ss
WHERE se.page = 'NextSong' AND
se.song =ss.title AND
se.artist = ss.artist_name AND
se.length = ss.duration 
""")

user_table_insert = ("""
INSERT INTO dimension_tables.users (user_id, first_name, last_name, gender, level)   
    select DISTINCT userid, firstname, lastname, gender, level
    from staging_tables.staging_events
    where userid IS NOT NULL
    and page='NextSong'
""")

song_table_insert = ("""
INSERT INTO dimension_tables.songs (song_id, title, artist_id, year, duration)
    select DISTINCT song_id, title, artist_id, year, duration
    from staging_tables.staging_songs
    where artist_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO dimension_tables.artists (artist_id, name, location, latitude, longitude)   
    select DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from staging_tables.staging_songs
    where artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO dimension_tables.time (start_time, hour, day, week, month, year, weekday)
SELECT start_time,
EXTRACT (HOUR FROM start_time), EXTRACT (DAY FROM start_time),
EXTRACT (WEEK FROM start_time), EXTRACT (MONTH FROM start_time),
EXTRACT (YEAR FROM start_time), EXTRACT (WEEKDAY FROM start_time) FROM
(SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_tables.staging_events);
""")

# QUERY LISTS

create_schemas_queries = [fact_schema, dimension_schema, staging_schema]
drop_schemas_queries = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

