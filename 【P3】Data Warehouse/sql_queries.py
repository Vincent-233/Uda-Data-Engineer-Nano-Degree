import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "Drop Table If Exists stage_events"
staging_songs_table_drop = "Drop Table If Exists stage_songs"
songplay_table_drop = "Drop Table If Exists songplays"
user_table_drop = "Drop Table If Exists users"
song_table_drop = "Drop Table If Exists songs"
artist_table_drop = "Drop Table If Exists artists"
time_table_drop = "Drop Table If Exists time"

# create tableS

staging_events_table_create= ("""
create table stage_events(
    artist  varchar(400),
    auth  varchar(100),
    firstName  varchar(200),
    gender  varchar(5),
    itemInSession  int,
    lastName  varchar(200),
    length  float,
    level  varchar(20),
    location  varchar(500),
    method  varchar(100),
    page  varchar(100),
    registration  decimal(20,6),
    sessionId  decimal(15,1),
    song  varchar(200),
    status  int,
    ts  timestamp,
    userAgent  varchar(500),
    userId  int
)
""")

staging_songs_table_create = ("""
create table stage_songs(
    num_songs  int,
    artist_id  varchar(50),
    artist_latitude  float,
    artist_longitude  float,
    artist_location  varchar(200),
    artist_name  varchar(400),
    song_id  varchar(50),
    title  varchar(200),
    duration  float,
    year  int
)
""")

songplay_table_create = ("""
create table songplays(
    songplay_id int identity(0,1) primary Key,
    start_time timestamp,
    user_id int not null,
    level varchar(20),
    song_id varchar(50) not null,
    artist_id varchar(50) not null,
    session_id int,
    location varchar(400),
    user_agent varchar(400)
)
""")

user_table_create = ("""
create table users(
    user_id int primary Key,
    first_name varchar(50),
    last_name varchar(50),
    gender varchar(5),
    level varchar(20)
)
""")

song_table_create = ("""
create table songs(
    song_id varchar(50) primary Key,
    title varchar(200), 
    artist_id varchar(50) not null, 
    year int, 
    duration numeric(10,5)
)
""")

artist_table_create = ("""
create table artists(                    
    artist_id varchar(50) primary Key,
    name varchar(400), 
    location varchar(200), 
    latitude float, 
    longitude float
)
""")

time_table_create = ("""
create table time(
    start_time timestamp primary Key,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar(20)
)
""")
# STAGING TABLES

staging_events_copy = ("""
copy stage_events
from '{}'
iam_role '{}' 
json '{}'
timeformat 'epochmillisecs'
region 'us-west-2'
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy stage_songs
from '{}'
iam_role '{}' 
json 'auto'
region 'us-west-2'
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    select a.ts,a.userid,a.level,b.song_id,b.artist_id,a.sessionid,a.location,a.useragent
    from stage_events a 
    inner join stage_songs b 
    on a.song = b.title and a.length = b.duration and a.artist = b.artist_name
    where a.page = 'NextSong'
""")

user_table_insert = ("""
insert into users(user_id,first_name,last_name,gender,level)
    select a.userid,a.firstname,a.lastname,a.gender,a.level
    from (select userid,firstname,lastname,gender,level
                 ,row_number() over(partition by userid order by ts desc) as rn
          from stage_events
          where page = 'NextSong') a
    where a.rn = 1
""")

song_table_insert = ("""
insert into songs(song_id,title,artist_id,year,duration)
    select a.song_id,a.title,a.artist_id,a.year,a.duration
    from stage_songs a
    where a.song_id is not null
""")

artist_table_insert = ("""
insert into artists(artist_id,name,location,latitude,longitude)
    select distinct a.artist_id,a.artist_name,a.artist_location,a.artist_latitude,a.artist_longitude
    from stage_songs a
    where a.artist_id is not null
""")

time_table_insert = ("""
insert into time(start_time,hour,day,week,month,year,weekday)
    select distinct start_time,datepart(h,start_time),datepart(d,start_time),datepart(w,start_time),datepart(mon,start_time),datepart(y,start_time),datepart(dw,start_time)
    from songplays a
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
