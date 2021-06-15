# Import the library needed for reading the configuration file
import configparser

# Retrieve configuration data from dwh.cfg file and store in variables, to be used later
config = configparser.ConfigParser()
config.read('dwh.cfg')

log_data_path = config.get("S3","LOG_DATA")
log_json_path = config.get("S3", "LOG_JSONPATH")
song_data_path = config.get("S3", "SONG_DATA")
dw_role_arn = config.get("IAM_ROLE", "DW_ROLE_ARN")

# Define "drop table if exists" variable scripts for Staging, Dimension, and Fact table(s)
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# Define "create table" variable scripts for Staging, Dimension and Fact table(s)
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist TEXT,
        auth TEXT,
        firstname TEXT,
        gender TEXT,
        iteminsession INT,
        lastname TEXT,
        length NUMERIC,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration NUMERIC,
        session_id INT,
        song TEXT,
        status INT,
        ts BIGINT,
        useragent TEXT,
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id TEXT,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT,
        title TEXT,
        duration NUMERIC,
        year INT
    );
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY, 
        start_time  TIMESTAMP NOT NULL REFERENCES time(start_time) distkey sortkey, 
        user_id     TEXT NOT NULL REFERENCES users(user_id), 
        level       TEXT, 
        song_id     TEXT NOT NULL REFERENCES songs(song_id), 
        artist_id   TEXT NOT NULL REFERENCES artists(artist_id), 
        session_id  INT,
        location    TEXT, 
        user_agent  TEXT
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT NOT NULL PRIMARY KEY sortkey, 
        first_name TEXT, 
        last_name TEXT, 
        gender TEXT,
        level TEXT
        ) diststyle all;
""")
    
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT NOT NULL PRIMARY KEY sortkey, 
        title TEXT, 
        artist_id TEXT, 
        year INT, 
        duration NUMERIC
        ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL PRIMARY KEY sortkey, 
        name TEXT, 
        location TEXT, 
        latitude NUMERIC, 
        longitude NUMERIC
        ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY sortkey, 
        hour INT, 
        day INT, 
        weekday INT, 
        week INT, 
        month INT, 
        year INT
        ) diststyle all;
""")


# Define variable scripts for loading data from S3 bucket to Staging Tables
staging_events_copy = """
    COPY staging_events 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    REGION 'us-west-2'
    JSON {}
""".format(log_data_path, dw_role_arn, log_json_path)

staging_songs_copy = """
    COPY staging_songs 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    REGION 'us-west-2'
    JSON 'auto'
""".format(song_data_path, dw_role_arn)

# Define variable "insert queries" for moving data from Staging tables to Dimension and Fact table(s)
songplay_table_insert = ("""

    INSERT INTO songplays
    (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id,
        location, 
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' +  se.ts/1000 * INTERVAL '1 second',
        se.user_id, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.session_id,
        se.location, 
        se.useragent
    FROM staging_events se
    INNER JOIN (SELECT DISTINCT artist_id, artist_name, song_id, title, duration FROM staging_songs) ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE page = 'NextSong'

""")

user_table_insert = ("""

    INSERT INTO users
    (
        user_id, 
        first_name, 
        last_name, 
        gender,
        level
    )
    SELECT DISTINCT
        se1.user_id,
        se1.firstname,
        se1.lastname,
        se1.gender,
        se1.level
    FROM staging_events se1
    LEFT JOIN users
        ON se1.user_id = users.user_id
    WHERE se1.page = 'NextSong' AND users.user_id IS NULL
    AND se1.ts = (SELECT MAX(ts) FROM staging_events se2 WHERE se1.user_id = se2.user_id)

""")

song_table_insert = ("""

    INSERT INTO songs
    (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
    LEFT JOIN songs
        ON ss.song_id = songs.song_id
    WHERE songs.song_id IS NULL
    
""")

artist_table_insert = ("""
    INSERT INTO artists
    (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT DISTINCT
        ss.artist_id,
        ss.title,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
    LEFT JOIN artists
        ON ss.artist_id = artists.artist_id
    WHERE artists.artist_id IS NULL
""")

time_table_insert = ("""
    INSERT INTO time
    (
        start_time, 
        hour, 
        day, 
        weekday, 
        week, 
        month, 
        year
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second',
        extract(hour from TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        extract(day from TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        extract(weekday from TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        extract(week from TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        extract(month from TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        extract(year from TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second')
    FROM staging_events se
    LEFT JOIN time
        ON TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' = time.start_time
    WHERE se.page = 'NextSong' AND time.start_time IS NULL
""")

# Define variable query lists to be used outside of this python program
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
