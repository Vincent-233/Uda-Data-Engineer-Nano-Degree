## Indtroduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. They don't have an easy way to query their data which resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I create a database schema based on RedShift and ETL pipeline for this analysis. 

## Data
- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

#### Song Dataset
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.For example, here are filepaths to two files in this dataset.
> song_data/A/B/C/TRABCEI128F424C983.json    
> song_data/A/A/B/TRAABJL12903CDCF1A.json
#### Log Dataset
The second dataset consists of log files also in JSON format.The log files in the dataset are partitioned by year and month.For example, here are filepaths to two files in this dataset.
> log_data/2018/11/2018-11-12-events.json    
> log_data/2018/11/2018-11-13-events.json


## Database Design
Considering that the data above is intended for analysis, I adopted the dimensional modeling method and used the star model.Star model can reduce the number of joins between tables, easier to understand and query.
I built four dimension tables and one fact table.

#### Dimensions table
- users - users in the app
- songs - songs in music database
- artists - artists in music database
- time - timestamps of records in songplays broken down into specific units
#### Fact table
- songplays - records in log data associated with song plays

## ETL Pipline
- extract data from song_data file, insert into the ` songs ` and ` artists ` dimensions table
- extract data from log_data file, insert into ` users ` and ` time ` dimensions table
- extracte data from log_data file, looking up data from the ` songs ` and ` artists ` table, insert the result data into the fact table ` songplays `

## File Description
- **dwh.cfg** some configurations for AWS,you should fill in with yours if you want to run my codes.
- **create_tables.py** drops and creates tables. you should run this file to reset tables before each time run the ETL scripts.
- **etl.py** load data into staging tables on RedShift and then process the data into final analytics tables.
- **sql_queries.py** contains all the sql queries invoked by other python files.

## Installation
1. In addition to pandas,numpy and other libraries that come with Anaconda, install psycopg2 and ipython-sql:
```shell
pip install ipython-sql
pip install psycopg2
```
2. Given then the database is on RedShift,so you should have a AWS account, configure it to have access to S3 and can connect to RedShift romotely.Fill your AWS infomaiton in dwh.cfg.

## Run
- in the terminal,run  two py file in order

```shell
python3  create_tables.py
python3  etl.py
```
