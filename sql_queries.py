import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                (
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender varchar,
                                itemInSession int,
                                lastName varchar,
                                length numeric,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration bigint,
                                sessionId int,
                                song varchar,
                                status int,
                                ts bigint,
                                userAgent varchar,
                                userId int
                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                num_songs int, 
                                artist_id varchar, 
                                artist_latitude varchar, 
                                artist_longitude varchar,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration numeric,
                                year int
                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
                            (
                                songplay_id int PRIMARY KEY IDENTITY (1,1), 
                                start_time bigint NOT NULL SORTKEY, 
                                user_id int NOT NULL, 
                                level varchar, 
                                song_id varchar NOT NULL DISTKEY,
                                artist_id varchar NOT NULL, 
                                session_id int, 
                                location varchar, 
                                user_agent varchar
                            )
            
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (
                            user_id INT PRIMARY KEY SORTKEY, 
                            first_name varchar, 
                            last_name varchar, 
                            gender varchar, 
                            level varchar
                        )diststyle all;

""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song
                        (
                            song_id varchar PRIMARY KEY DISTKEY SORTKEY, 
                            title varchar NOT NULL, 
                            artist_id varchar NOT NULL, 
                            year int, 
                            duration numeric
                        )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist
                          (
                              artist_id varchar PRIMARY KEY SORTKEY, 
                              name varchar NOT NULL, 
                              location varchar, 
                              lattitude varchar, 
                              longitude varchar
                          )
                

""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                            start_time timestamp PRIMARY KEY SORTKEY, 
                            hour smallint NOT NULL, 
                            day smallint NOT NULL, 
                            week smallint NOT NULL, 
                            month smallint NOT NULL, 
                            year smallint NOT NULL, 
                            weekday varchar NOT NULL
                        )diststyle all;
""")

# STAGING TABLES
#JSON 's3://udacity-dend/log_json_path.json'

staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""")



staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            SELECT DISTINCT e.ts as start_time, e.userId as user_id, e.level, s.song_id, s.artist_id, e.sessionId as session_id, s.artist_location as location, e.userAgent as user_agent
                            FROM staging_songs as s
                            JOIN staging_events as e 
                            ON (s.artist_name = e.artist AND s.title = e.song)

""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level) 
                            SELECT DISTINCT e.userId, e.firstName, e.lastName, e.gender, e.level
                            FROM staging_events as e 
                            WHERE e.userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) 
                            SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
                            FROM staging_songs as s

""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, lattitude, longitude) 
                            SELECT DISTINCT s.artist_id, s.artist_name,s.artist_location, s.artist_latitude, s.artist_longitude
                            FROM staging_songs as s
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                            EXTRACT(hour FROM start_time),
                            EXTRACT(day FROM start_time),
                            EXTRACT(week FROM start_time),
                            EXTRACT(month FROM start_time),
                            EXTRACT(year FROM start_time),
                            EXTRACT(dayofweek FROM start_time)
                            FROM staging_events as e 

""")


#Queries to count Rows in final tables
count_rows_query =("SELECT COUNT(*) FROM {};")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# TABLE LIST

table_list = ['staging_events', 'staging_songs', 'songplay', 'user_table', 'song', 'artist', 'time']