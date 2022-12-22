import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingEvents"
staging_songs_table_drop = "DROP TABLE IF EXISTS songsEvents"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stagingevents(
                          event_id      BIGINT identity(0, 1) NOT NULL,
                          artist        VARCHAR,
                          auth          VARCHAR,
                          firstname     VARCHAR,
                          gender        VARCHAR,
                          iteminsession INT,
                          lastname      VARCHAR,
                          length        DECIMAL,
                          level         VARCHAR,
                          location      VARCHAR,
                          method        VARCHAR,
                          page          VARCHAR,
                          regestration  DECIMAL,
                          sessionid     INT,
                          song          VARCHAR,
                          srarus        INT,
                          ts            INT,
                          useragent     VARCHAR,
                          userid        INTEGER
             )

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stagingSongs
  (
     num_songs        INT,
     artist_id        VARCHAR,
     artist_longitude DECIMAL,
     artist_latitude  DECIMAL,
     artist_location  VARCHAR,
     artist_name      VARCHAR,
     title            VARCHAR,
     duration         DECIMAL,
     year             INT
  ) 
 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
  songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
  start_time TIMESTAMP SORTKEY,
  user_id int,
  level varchar(15),
  song_id int,
  artist_id varchar,
  session int
  location varchar(15),
  user_agent varchar
  )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user (
  user_id int PRIMARY KEY sortkey,
  first_name varchar(30),
  last_name varchar(30),
  gender varchar(15),
  level varcha(15),
  session int,
  location varchar(15),
  user_agent varchar
  ) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
  song_id int  IDENTITY(0,1) PRIMARY KEY sortkey,
  title varchar(30),
  artist_id int,
  year int,
  duration decimal
)diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id varchar sortkey PRMARY KEY,
    name varchar,
    location varchar,
    lattitude decimal,
    longitude decimal,
)diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time datetime sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int,
) diststyle all;
""")

# STAGING TABLES
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


staging_events_copy = ("""
    COPY staginEvents FROM {}
    IAM_ROLE '{}'
    JSON {}
    TIMEFORMAT 'epochmillisecs'  
    REGION 'us-west-2';  
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY stagingSongs FROM {}
    CREDINTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time,user_id,level,song_id,artist_id,session_id,location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent
    FROM stagingEvents se
    JOIN stagingSongs ss ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO user (user_id, first_name,last_name,gender,level)
    SELECT  DISTINCT se.userId,
            se.firstName,
            se.lastName,
            se.gender,
            se.level    
            FROM stagingEvents se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song (song_id,title,artist_id,year,duration)
    SELECT  DISTINCT ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
    FROM stagingSongs ss;
""")
artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location,latitude,longitude)
    SELECT  DISTINCT ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude
    FROM stagingSongs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,hour,day,  week, month,year,weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(week FROM start_time)    
            FROM    stagingEvents se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
