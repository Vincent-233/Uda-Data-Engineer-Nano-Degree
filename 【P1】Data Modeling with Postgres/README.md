## Indtroduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.They don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I create a database schema and ETL pipeline for this analysis. 

## Data
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
- **test.ipynb** displays the first few rows of each table to check the database.
- **create_tables.py** drops and creates your tables.run this file to reset tables before each time run the ETL scripts.
- **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into the corresponding tables. This notebook contains detailed description on the ETL process for each of the tables.
- etl.py reads and processes all the files from song_data and log_data and loads into tables.
- **sql_queries.py** contains all the sql queries invoked by other py files.

## Installation
In addition to pandas,numpy and other libraries that come with Anaconda, install psycopg2 and ipython-sql:
```shell
pip install ipython-sql
pip install psycopg2
```

## Run
- in the terminal,run  two py file in order

```shell
python3  create_tables.py
python3  etl.py
```

- open test.ipynb and run some test query to check the data
