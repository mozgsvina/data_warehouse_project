import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('Data_Warehouse_Project_Template/dwh.cfg')

ROLE_ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist          VARCHAR (200),
        auth            VARCHAR (20),
        firstName       VARCHAR (25),
        gender          VARCHAR (1),
        itemInSession   INTEGER,
        lastName        VARCHAR (25),
        length          DOUBLE PRECISION,
        level           VARCHAR (10),
        location        VARCHAR (100),
        method          VARCHAR (5),
        page            VARCHAR (30),
        registration    DOUBLE PRECISION,
        sessionId       INTEGER,
        song            VARCHAR (400),                        
        status          INTEGER,
        ts              BIGINT,
        userAgent       VARCHAR (250),
        userId          INTEGER
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        song_id             VARCHAR(20) NOT NULL DISTKEY,
        num_songs           INTEGER,
        artist_id           VARCHAR(20) NOT NULL,
        artist_latitude     DOUBLE PRECISION,
        artist_longitude    DOUBLE PRECISION,
        artist_location     VARCHAR (400),
        artist_name         VARCHAR (400) NOT NULL,
        title               VARCHAR (400),
        duration            DOUBLE PRECISION,
        year                INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id    INTEGER         IDENTITY(1,1)    PRIMARY KEY,
        start_time     TIMESTAMP       SORTKEY          NOT NULL,
        user_id        INTEGER         NOT NULL,
        level          VARCHAR (50)    NOT NULL,
        song_id        VARCHAR(50)     NOT NULL          DISTKEY,
        artist_id      VARCHAR(20)     NOT NULL,
        session_id     INTEGER         NOT NULL,
        location       VARCHAR (500),
        user_agent     VARCHAR (500)   NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id    INTEGER     PRIMARY KEY  SORTKEY,
        first_name VARCHAR (150)   NOT NULL,
        last_name  VARCHAR         NOT NULL,
        gender     VARCHAR (5)     NOT NULL,
        level      VARCHAR (50)    NOT NULL
        ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id    VARCHAR(50)     PRIMARY KEY SORTKEY  DISTKEY,
        title      VARCHAR (500)   NOT NULL,
        artist_id  VARCHAR(20)     NOT NULL,
        year       INTEGER         NOT NULL,
        duration   DECIMAL         NOT NULL
        )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id    VARCHAR(20)     PRIMARY KEY SORTKEY,
        name         VARCHAR (500)   NOT NULL,
        location     VARCHAR (500),
        latitude     DOUBLE PRECISION,
        longitude    DOUBLE PRECISION
        ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY   SORTKEY,
        hour INTEGER      NOT NULL,
        day INTEGER       NOT NULL,
        week INTEGER      NOT NULL,
        month INTEGER     NOT NULL,
        year INTEGER      NOT NULL,
        weekday  INTEGER  NOT NULL
        ) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
        COPY staging_songs
        FROM {}
        CREDENTIALS {}
        FORMAT AS JSON 'auto'
        REGION 'us-west-2'
        MAXERROR 5
""").format(SONG_DATA, ROLE_ARN)
 
staging_songs_copy = ("""
        COPY staging_events
        FROM {}
        CREDENTIALS {}
        FORMAT AS JSON {}
        REGION 'us-west-2'
""").format(LOG_DATA, ROLE_ARN, LOG_JSONPATH)

# PREPROCESS DATA

update_songs = """UPDATE songs
        SET artist_name = artist_name_n
        FROM (
            SELECT artist_id, artist_name AS artist_name_n
            FROM (
                SELECT artist_id, artist_name, 
                    ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY COUNT(*) DESC) AS rnk
                FROM songs
                GROUP BY artist_id, artist_name
            ) ranked_names
            WHERE rnk = 1
        ) most_frequent_names
        WHERE songs.artist_id = most_frequent_names.artist_id"""

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events e
    JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT e.userId, e.firstName, e.lastName, e.gender, e.level FROM staging_events e
    WHERE e.userId is not null
""")

song_table_insert = ("""
    INSERT INTO song_data (song_id, title, artist_id, year, duration)
    WITH song_rows AS (SELECT DISTINCT song_id, title, artist_id, year, duration, row_number() OVER (PARTITION BY song_id ORDER BY year DESC) AS row_number
    FROM staging_songs) SELECT song_id, title, artist_id, year, duration FROM song_rows WHERE row_number = 1 
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    WITH artist_rows AS (SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude, row_number() OVER (PARTITION BY artist_id, artist_name ORDER BY year DESC) AS row_number FROM staging_songs s)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM artist_rows WHERE row_number = 1 
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' as d, 
    EXTRACT(HOUR FROM d), 
    EXTRACT(DAY FROM d), 
    EXTRACT(WEEK FROM d), 
    EXTRACT(MONTH FROM d), 
    EXTRACT(YEAR FROM d), 
    EXTRACT(WEEKDAY FROM d)
    FROM staging_events e
""")

# ANALYTICAL QUERIES

active_users_q = """
    SELECT u.user_id AS ID, u.first_name AS NAME, u.last_name AS LAST_NAME, count(*) AS TOTAL
    FROM users u JOIN songplay s on u.user_id = s.user_id
    GROUP BY u.user_id, u.first_name, u.last_name
    ORDER BY count(*) DESC
    LIMIT 10
"""

friday_q = """SELECT DISTINCT s.title, a.name, count(*) AS friday_count
                FROM songplay s JOIN songs s ON s.song_id = s.song_id 
                JOIN artists a ON s.artist_id = a.artist_id
                JOIN time t on s.start_time = t.start_time
                GROUP BY s.title, a.name, t.weekday, t.hour
                HAVING t.weekday = 5 AND t.hour BETWEEN 16 AND 23
                ORDER BY count(*) DESC
                LIMIT 5
                """

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
preprocess_queries = [update_songs]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytical_queries = [active_users_q, friday_q]
