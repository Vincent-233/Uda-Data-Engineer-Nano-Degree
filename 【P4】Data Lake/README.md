## Indtroduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I build an ETL pipeline that loads the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Data
- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

#### Song Dataset
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.For example, here are filepaths of two files in this dataset.
> song_data/A/B/C/TRABCEI128F424C983.json    
> song_data/A/A/B/TRAABJL12903CDCF1A.json
#### Log Dataset
The second dataset consists of log files also in JSON format.The log files in the dataset are partitioned by year and month.For example, here are filepaths to two files in this dataset.
> log_data/2018/11/2018-11-12-events.json    
> log_data/2018/11/2018-11-13-events.json

## Data Model Design
Considering that the data above is intended for analysis, I adopted the dimensional modeling method and used the star model.Star model can reduce the number of joins between tables, easier to understand and query.
I built four dimension tables and one fact table.

#### Dimensions table
- users - users in the app
- songs - songs in music database,partitioned by year and artist
- artists - artists in music database
- time - timestamps of records in songplays broken down into specific units,partitioned by year and month
#### Fact table
- songplays - records in log data associated with song plays,partitioned by year and month

## ETL Pipline
- load data from song_data file, write back into the ` songs ` and ` artists `  parquet file
- load data from log_data file, write back into ` users ` and ` time ` parquet file
- loade data from log_data file, looking up data from the ` songs ` and ` artists ` parquet file, write the result data into the  ` songplays ` parquet file

## File Description
- **dwh.cfg** some configurations for AWS,you should fill in with yours if you want to run my codes.
- **etl.py** load data from s3 and then process the data into final analytics tables files.


## Run
In the terminal,run the python file

```shell
python3 etl.py
```
 If you don't have a spark cluster locally, AWS EMR service is a good try.