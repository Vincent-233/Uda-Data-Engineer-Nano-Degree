import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,DateType,TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# def create_spark_session():
#     spark = SparkSession \
#         .builder \
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#         .getOrCreate()
#     return spark

AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession.builder\
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)\
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.format('json').load(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
        .partitionBy('year','artist_id')\
        .parquet(os.path.join(output_data,'songs.parquet'),mode = 'overwrite')
    
    print('songs.parquet write finished!')
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
        .distinct()\
        .withColumnRenamed('artist_name','name')\
        .withColumnRenamed('artist_location','location')\
        .withColumnRenamed('artist_latitude','latitude')\
        .withColumnRenamed('artist_longitude','longitude')

    # write artists table to parquet files
    artists_table.write\
        .parquet(os.path.join(output_data,'artists.parquet'),mode = 'overwrite')
    
    print('artists.parquet write finished!')
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data,'log-data/*.json')

    # read log data file
    df = spark.read.format('json').load(log_data)

    # filter by actions for song plays
    df = df.where("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select('userid','firstname','lastname','gender','level')\
        .distinct()\
        .withColumnRenamed('userid','user_id')\
        .withColumnRenamed('firstname','first_name')\
        .withColumnRenamed('lastname','last_name')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users.parquet'),mode = 'overwrite')

    print('users.parquet write finished!')
    
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.fromtimestamp(x/1000),TimestampType())
    df = df.withColumn('start_time',get_timestamp(df['ts']))
    df = df.withColumn('year',year(df['start_time']))\
           .withColumn('month',month(df['start_time']))

    
    # create temp view for furture use
    df.select('start_time')\
        .distinct()\
        .createOrReplaceTempView('vw_df_StartTime')

    # extract columns to create time table
    time_table = spark.sql("""
        select start_time
              ,hour(start_time) as hour
              ,dayofmonth(start_time) as day
              ,weekofyear(start_time) as week
              ,month(start_time) as month
              ,year(start_time) as year
              ,dayofweek(start_time) as weekday
        from vw_df_StartTime
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write\
            .partitionBy('year','month')\
            .parquet(os.path.join(output_data,'time.parquet'),mode = 'overwrite')
    
    print('time.parquet write finished!')
    
    # read in song and artists data to use for songplays table
    song_df = spark.read.format('parquet')\
        .load(os.path.join(output_data,'songs.parquet'))

    artist_df = spark.read.format('parquet')\
        .load(os.path.join(output_data,'artists.parquet'))

    # extract columns from joined datasets to create songplays table 
    songplays_table = df.join(song_df,[df.song == song_df.title, df.length == song_df.duration])\
        .join(artist_df,[song_df.artist_id == artist_df.artist_id, df.artist == artist_df.name])\
        .select(df.start_time, df.userId, df.level, song_df.song_id,song_df.artist_id,
                df.sessionId,df.location, df.userAgent, df.year, df.month)

    # rename columns and add songplay_id
    songplays_table = songplays_table.withColumnRenamed('userid','user_id')\
        .withColumnRenamed('sessionid','session_id')\
        .withColumnRenamed('useragent','user_agent')\
        .withColumn("songplay_id",monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
            .partitionBy('year','month')\
            .parquet(os.path.join(output_data,'songplays.parquet'),mode = 'overwrite')
    
    print('songplays.parquet write finished!')
    
def main():
    spark = create_spark_session()
    
    # input_data = '/home/ghost/workdata/P4/'
    # output_data = '/home/ghost/workdata/Out-P4/'
    
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://bucket-vincent/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()