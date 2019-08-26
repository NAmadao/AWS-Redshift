import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN=config.get('IAM_ROLE', 'ARN')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" \
                            CREATE TABLE staging_events ( \
                                artist TEXT, \
                                auth TEXT, \
                                firstName TEXT, \
                                gender CHAR(1), \
                                itemInSession INTEGER, \
                                lastName TEXT, \
                                length NUMERIC, \
                                level TEXT, \
                                location TEXT, \
                                method TEXT, \
                                page TEXT, \
                                registration NUMERIC, \
                                sessionId INTEGER, \
                                song TEXT, \
                                status INTEGER, \
                                ts BIGINT, \
                                userAgent TEXT, \
                                userId INTEGER \
                            );\
""")

staging_songs_table_create = (""" \
                            CREATE TABLE staging_songs ( \
                                num_songs INTEGER, \
                                artist_id TEXT, \
                                artist_latitude NUMERIC, \
                                artist_longitude NUMERIC, \
                                artist_location TEXT, \
                                artist_name TEXT, \
                                song_id TEXT, \
                                title TEXT, \
                                duration NUMERIC, \
                                year INTEGER \
                            );\
""")

songplay_table_create = (""" \
                        CREATE TABLE songplays ( \
                            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, \
                            start_time TIMESTAMP, \
                            user_id INTEGER NOT NULL, \
                            level TEXT, \
                            song_id TEXT, \
                            artist_id TEXT, \
                            session_id INTEGER, \
                            location TEXT, \
                            user_agent TEXT \
                        );\
""")

user_table_create = (""" \
                    CREATE TABLE users ( \
                        user_id INTEGER PRIMARY KEY, \
                        first_name TEXT NOT NULL, \
                        last_name TEXT NOT NULL, \
                        gender CHAR(1), \
                        level TEXT \
                    );\
""")

song_table_create = (""" \
                    CREATE TABLE songs ( \
                        song_id TEXT PRIMARY KEY, \
                        title TEXT, \
                        artist_id TEXT, \
                        year INTEGER, \
                        duration NUMERIC \
                    );\
""")

artist_table_create = (""" \
                    CREATE TABLE artists ( \
                        artist_id TEXT PRIMARY KEY, \
                        name TEXT, \
                        location TEXT, \
                        latitude NUMERIC, \
                        longitude NUMERIC \
                    );\
""")

time_table_create = (""" \
                    CREATE TABLE time ( \
                        start_time TIMESTAMP PRIMARY KEY, \
                        hour INTEGER, \
                        day INTEGER, \
                        week INTEGER, \
                        month INTEGER, \
                        year INTEGER, \
                        weekday INTEGER \
                    );\
""")

# STAGING TABLES

staging_events_copy = ("""
                    COPY staging_events FROM {}
                    iam_role {}
                    JSON {}
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
                    COPY staging_songs FROM {}
                    iam_role {}
                    JSON 'auto'
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = (""" \
    INSERT into songplays(start_time, user_id, level, song_id, \
    artist_id, session_id, location, user_agent) \
    SELECT timestamp 'epoch' + events.ts/1000 * interval '1 second' as start_time, \
        events.userId, events.level, songs.song_id, songs.artist_id, \
        events.sessionId, events.location, events.userAgent \
    FROM staging_songs songs, staging_events events \
    WHERE events.page = 'NextSong' AND \
        events.song = songs.title AND \
        events.artist = songs.artist_name AND \
        events.length = songs.duration;\
""")

user_table_insert = (""" \
    INSERT INTO users(user_id, first_name, last_name, gender, level) \
    SELECT distinct userId, firstName, lastName, gender, level \
    FROM staging_events \
    WHERE page='NextSong';\
""")

song_table_insert = (""" \
    INSERT INTO songs(song_id, title, artist_id, year, duration) \
    SELECT song_id, title, artist_id, year, duration \
    FROM staging_songs \
    WHERE song_id IS NOT NULL;\
""")

artist_table_insert = (""" \
    INSERT INTO artists(artist_id, name, location, latitude, longitude) \
    SELECT distinct artist_id, artist_name, artist_location, \
                    artist_latitude, artist_longitude \
    FROM staging_songs \
    WHERE artist_id IS NOT NULL;\
""")

time_table_insert = (""" \
    INSERT INTO time(start_time, hour, day, week, month, year, weekDay) \
    SELECT start_time, extract(hour from start_time), extract(day from start_time), \
        extract(week from start_time), extract(month from start_time), \
        extract(year from start_time), extract(dayofweek from start_time) \
    FROM songplays;\
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
