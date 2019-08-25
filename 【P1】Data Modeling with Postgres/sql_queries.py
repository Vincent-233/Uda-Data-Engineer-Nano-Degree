# DROP TABLES

songplay_table_drop = "Drop Table If Exists songplays"
user_table_drop = "Drop Table If Exists users"
song_table_drop = "Drop Table If Exists songs"
artist_table_drop = "Drop Table If Exists artists"
time_table_drop = "Drop Table If Exists time"

# CREATE TABLES

songplay_table_create = ("""
Create Table songplays(
    songplay_id serial Primary Key,
    start_time timestamp,
    user_id int,
    level varchar(20),
    song_id varchar(50),
    artist_id varchar(50),
    session_id int,
    location varchar(400),
    user_agent varchar(400)
)
""")

user_table_create = ("""
Create Table users(
    user_id int Primary Key,
    first_name varchar(50),
    last_name varchar(50),
    gender varchar(5),
    level varchar(20)
)
""")

song_table_create = ("""
Create Table songs(
    song_id varchar(50) Primary Key,
    title varchar(200), 
    artist_id varchar(50), 
    year int, 
    duration numeric(10,5)
)
""")

artist_table_create = ("""
Create Table artists(                    
    artist_id varchar(50) Primary Key,
    name varchar(50), 
    location varchar(200), 
    lattitude numeric(10,6), 
    longitude numeric(10,6)
)
""")

time_table_create = ("""
Create Table time(
    start_time timestamp Primary Key,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar(20)
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
Insert Into songplays(start_time,user_id,level,song_id,artist_id,
                      session_id,location,user_agent) 
    Values(%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
Insert Into users(user_id,first_name,last_name,gender,level) 
    Values(%s,%s,%s,%s,%s)
    On Conflict (user_id) Do Nothing
""")

song_table_insert = ("""
Insert Into songs(song_id,title,artist_id,year,duration) 
    Values(%s,%s,%s,%s,%s)
    On Conflict (song_id) Do Nothing
""")

artist_table_insert = ("""
Insert Into artists(artist_id,name,location,lattitude,longitude) 
    Values(%s,%s,%s,%s,%s)
    On Conflict (artist_id) Do Nothing
""")

time_table_insert = ("""
Insert Into time(start_time,hour,day,week,month,year,weekday)
    Values(%s,%s,%s,%s,%s,%s,%s)
    On Conflict (start_time) Do Nothing
""")

# FIND SONGS

song_select = ("""
Select a.song_id,b.artist_id
From songs a
Inner Join artists b On a.artist_id = b.artist_id
where a.title = %s 
  And b.name = %s 
  And a.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]