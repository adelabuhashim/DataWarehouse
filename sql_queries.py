import configparser
=

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stagingEvents"
staging_songs_table_drop = "DROP TABLE IF EXISTS songsEvents"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
## STAGING
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stagingEvents
    (
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
        session_id     INT,
        song          VARCHAR,
        srarus        INT,
        ts            BIGINT,
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
    song_id VARCHAR ,
    title            VARCHAR,
    duration         DECIMAL,
    year             INT
  ) 
 
""")

songplay_table_create = ("""
CREATE TABLE IF not EXISTS songplay
    (
        songplay_id INTEGER identity(0,1) PRIMARY KEY,
        start_time TIMESTAMP sortkey,
        user_id    INTEGER,
        LEVEL      VARCHAR,
        song_id    VARCHAR ,
        artist_id  VARCHAR,
        session_id INTEGER,
        location   VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
CREATE TABLE IF not EXISTS users
    (
        user_id    INT PRIMARY KEY sortkey,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     VARCHAR,
        LEVEL      VARCHAR,
        session_id INT,
        location   VARCHAR,
        user_agent VARCHAR
    )
    diststyle ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
  (
     song_id   VARCHAR PRIMARY KEY,
     title     VARCHAR,
     artist_id VARCHAR,
     year      INT,
     duration  DECIMAL
  ) diststyle ALL; 
""")

artist_table_create = ("""
CREATE TABLE IF not EXISTS artist
    (
        artist_id VARCHAR PRIMARY KEY sortkey ,
        name      VARCHAR,
        location  VARCHAR,
        latitude  DECIMAL,
        longitude DECIMAL
    )
    diststyle ALL;
""")

time_table_create = ("""
CREATE TABLE IF not EXISTS TIME
    (
        start_time datetime sortkey,
        hour       INT,
        day        INT,
        week       INT,
        month      INT,
        year       INT,
        weekday    INT
    )
    diststyle ALL;
""")

# STAGING TABLES
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


staging_events_copy = ("""
    COPY stagingEvents FROM {}
    IAM_ROLE '{}'
    JSON {}
    TIMEFORMAT 'epochmillisecs'  
    REGION 'us-west-2';  
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY stagingSongs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay
            (
                start_time,
                user_id,
                LEVEL,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent
            )
SELECT DISTINCT timestamp 'epoch' + E.ts/1000 * interval '1 second' as start_time,
                E.userid,
                E.LEVEL,
                S.song_id,
                S.artist_id,
                E.session_id,
                E.location,
                E.useragent
FROM            stagingevents E
join            stagingsongs S
ON              E.artist = S.artist_name
WHERE           E.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
            (user_id,
             first_name,
             last_name,
             gender,
             LEVEL)
SELECT DISTINCT E.userid,
                E.firstname,
                E.lastname,
                E.gender,
                E.LEVEL
FROM   stagingevents E
WHERE  E.page = 'NextSong'; 
""")

song_table_insert = ("""
INSERT INTO song
            (song_id,
             title,
             artist_id,
             year,
             duration)
SELECT DISTINCT S.song_id,
                S.title,
                S.artist_id,
                S.year,
                S.duration
FROM   stagingsongs S; 
""")
artist_table_insert = ("""
INSERT INTO artist
            (artist_id,
             name,
             location,
             latitude,
             longitude)
SELECT DISTINCT S.artist_id,
                S.artist_name,
                S.artist_location,
                S.artist_latitude,
                S.artist_longitude
FROM   stagingsongs S; 
""")

time_table_insert = ("""
INSERT INTO TIME
            (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday
            )
SELECT DISTINCT timestamp 'epoch' + e.ts * interval '1 second' as start_time,
                extract(hour FROM start_time),
                extract(day FROM start_time),
                extract(week FROM start_time),
                extract(month FROM start_time),
                extract(year FROM start_time),
                extract(week FROM start_time)
FROM            stagingevents e
WHERE           e.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
