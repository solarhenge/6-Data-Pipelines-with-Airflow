class SqlQueries:

    songplay_table_insert = ("""
INSERT INTO songplays
SELECT
    --md5(events.sessionid || events.start_time) songplay_id,
    (events.sessionid::text || events.start_time::text) as playid,
    events.start_time,
    events.userid::int as userid,
    events.level,
    songs.song_id as songid,
    songs.artist_id as artistid,
    events.sessionid,
    events.location,
    events.useragent 
FROM
    (
        SELECT
            TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_time,
            * 
        FROM
            staging_events 
        WHERE
            page = 'NextSong' 
    )
    events 
    JOIN
        staging_songs songs 
        ON events.song = songs.title 
        AND events.artist = songs.artist_name 
        AND events.length = songs.duration
""")

    users_table_insert = ("""
INSERT INTO users
SELECT distinct
    se.userid::int,
    se.firstname as first_name,
    se.lastname as last_name,
    se.gender
    --,se.level 
FROM
    staging_events se 
    JOIN
        staging_songs ss 
        ON se.song = ss.title 
        AND se.artist = ss.artist_name 
        AND se.length = ss.duration 
WHERE
    1 = 1 
    AND se.page = 'NextSong'
""")

    songs_table_insert = ("""
INSERT INTO songs
SELECT distinct ss.song_id as songid
      ,ss.title
      ,ss.artist_id as artistid
      ,ss.year
      ,ss.duration
FROM
    staging_events se 
    JOIN
        staging_songs ss 
        ON se.song = ss.title 
        AND se.artist = ss.artist_name 
        AND se.length = ss.duration 
WHERE
    1 = 1 
    AND se.page = 'NextSong'
    """)

    artists_table_insert = ("""
INSERT INTO artists
SELECT distinct ss.artist_id as artistid
      ,ss.artist_name as name
      ,ss.artist_location as location
      ,ss.artist_latitude as latitude
      ,ss.artist_longitude as longitude
FROM
    staging_events se 
    JOIN
        staging_songs ss 
        ON se.song = ss.title 
        AND se.artist = ss.artist_name 
        AND se.length = ss.duration 
WHERE
    1 = 1 
    AND se.page = 'NextSong'
""")

    artists_table_update = ("""
update
    artists 
set
    location = null 
where
    location = ''
""")

    time_table_insert = ("""
INSERT INTO time
SELECT distinct start_time
      ,extract(hour from start_time) as hour
      ,extract(day from start_time) as day
      ,extract(week from start_time) as week
      ,extract(month from start_time) as month
      ,extract(year from start_time) as year
      ,extract(dayofweek from start_time) as weekday
FROM songplays
""")