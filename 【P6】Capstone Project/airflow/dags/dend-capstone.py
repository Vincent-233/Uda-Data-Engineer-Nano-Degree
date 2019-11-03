from datetime import datetime, timedelta
import os
import distutils.dir_util as dir_util
import re
import glob
import json  
from shutil import copy
import pandas as pd
from pandas.io.json import json_normalize
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# import user defined operator
from operators.data_quality import DataQualityOperator
from operators.load_to_redshift import LoadToRedshiftOperator
from operators.s3_upload import UploadToS3Operator
from helpers.sql_queries import SqlQueries


default_args = {
    'owner':'vincent',
    'depends_on_past':False,
    'start_date':datetime(2019,10, 12), 
    'retries':2,
    'retry_delay':timedelta(minutes=3),
    'catchup':False,
    'email_on_retry':False
}

dag = DAG('dend-capstone',
          default_args=default_args,
          description='Data Engineering Capstone Project',
          schedule_interval=None
)

def clear_folder(folder_name):
    """clear folder"""
    for name in os.listdir(folder_name):
        file = os.path.join(folder_name,name)
        try:
            dir_util.remove_tree(file)
        except OSError:
            os.remove(file)

def local_file_archive():
    """archive input folder,clear upload folder"""
    data_input = Variable.get('data-input')
    data_upload = Variable.get('data-upload')
    data_input_archive = Variable.get('data-input-archive')
    fold_name = datetime.today().strftime('%Y%m%d')

    # move input file to archive folder
    dir_util.copy_tree(data_input,os.path.join(data_input_archive,fold_name)) 
    clear_folder(data_input)

    # clear input folder
    clear_folder(data_upload)

def json_to_csv():
    with open(f"{Variable.get('data-input')}/iso_3166_2.json") as f:
        iso_country_region = json.load(f)

    cols = ['country_code','country_name','region_code','region_name']
    df_iso_country_region = pd.DataFrame(columns = cols)

    for country_code in iso_country_region:
        df_country_region = json_normalize(iso_country_region[country_code])
        df_country_region['country_code'] = country_code
        df_country_region.rename(columns = {'name':'country_name'},inplace = True)
        unpivot_cols = [col for col in list(df_country_region) if col not in ('country_name','country_code')]
        df_country_region = pd.melt(df_country_region,id_vars=['country_code','country_name'],value_vars = unpivot_cols
                                   ,var_name = 'region_code',value_name = 'region_name')
        df_country_region['region_code'] = df_country_region['region_code'].str.replace('divisions.','')
        df_iso_country_region = df_iso_country_region.append(df_country_region,ignore_index = True)

    csv_out_path = f"{Variable.get('data-upload')}/csv"
    df_iso_country_region.to_csv(f"{csv_out_path}/iso_country_region.csv",sep='|',index=False)
    
def other_file_process():
    """"other csv file copy to data-upload,some processing if need"""
    csv_out_path = f"{Variable.get('data-upload')}/csv"
    if not os.path.exists(csv_out_path):
        os.mkdir(csv_out_path)

    # airport file process
    df = pd.read_csv(f"{Variable.get('data-input')}/airport_codes.csv",sep = ',')
    df_coorinate = df['coordinates'].str.split(',',expand=True)
    df['longitude'] = df_coorinate[0].str.strip().astype(float)
    df['latitude'] = df_coorinate[1].str.strip().astype(float)
    df.to_csv(f"{csv_out_path}/airport_codes.csv",sep='|',index=False)
    
    # other file copy to upload folder
    copy(f"{Variable.get('data-input')}/us_cities_demographics.csv",csv_out_path)

def sas7bdat_to_parquet():
    spark = SparkSession.builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()

    schema = """
        cicid	double,
        i94yr	double,
        i94mon	double,
        i94cit	double,
        i94res	double,
        i94port	varchar(200),
        arrdate	double,
        i94mode	double,
        i94addr	varchar(200),
        depdate	double,
        i94bir	double,
        i94visa	double,
        count	double,
        dtadfile	varchar(200),
        visapost	varchar(200),
        occup	varchar(200),
        entdepa	varchar(200),
        entdepd	varchar(200),
        entdepu	varchar(200),
        matflag	varchar(200),
        biryear	double,
        dtaddto	varchar(200),
        gender	varchar(200),
        insnum	varchar(200),
        airline	varchar(200),
        admnum	double,
        fltno	varchar(200),
        visatype	varchar(200)
    """

    # read all sas7bdat file under data-input folder
    df_spark = spark.createDataFrame([],schema) # create a empty dataframe
    for filename in glob.glob(f"{Variable.get('data-input')}/*.sas7bdat"):
        df_sas = spark.read\
                .format('com.github.saurfang.sas.spark')\
                .schema(schema)\
                .load(filename)
        df_spark = df_spark.union(df_sas)
    
    # write to parquet
    df_spark.write.mode('overwrite')\
            .parquet(f"{Variable.get('data-upload')}/sas_data.parquet")

def match_to_csv(match_var,csv_file_name):
    lines = []
    reg_exp = r'\s*=\s*'
    for line in match_var.split('\n'):
        if line.strip() and re.search(reg_exp,line): # if not empty line and match reg
            line_t = re.sub(reg_exp,'|',line.strip().replace("'",''))    # replace xxx = 'yyy' to xxx|yyy
            lines.append(line_t + '\n')
    with open(csv_file_name,'w') as fw:
        fw.writelines(lines)

def sas_desc_to_csv():
    sas_desc_file = f"{Variable.get('data-input')}/I94_SAS_Labels_Descriptions.SAS"
    csv_out_path = f"{Variable.get('data-upload')}/csv"

    if not os.path.exists(csv_out_path):
        os.mkdir(csv_out_path)
    
    # sas desc file is small,so read all into one variable 
    with open(sas_desc_file,'r') as file: 
        sas_desc = file.read()

    # group 1:table name，group 2:data content
    reg_exp = r'\*/\s*value\s+(\S+)(.*?);'
    matches = re.findall(reg_exp,sas_desc,re.S)
    for m in matches:
        file_name = os.path.join(csv_out_path,m[0].replace('$','') + '.csv') # removing leading $
        match_to_csv(m[1],file_name)

    # visa code
    reg_exp = r'/\* I94VISA(.*?)\*/'
    matches = re.findall(reg_exp,sas_desc,re.S)
    match_to_csv(matches[0],os.path.join(csv_out_path,'i94visa.csv'))


# Operator
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

sas7bdat_to_parquet = PythonOperator(
    task_id = 'sas7bdat_to_parquet',
    python_callable = sas7bdat_to_parquet,
    dag = dag
)

sas_desc_to_csv = PythonOperator(
    task_id = 'sas_desc_to_csv',
    python_callable = sas_desc_to_csv,
    dag = dag
)

json_to_csv = PythonOperator(
    task_id = 'json_to_csv',
    python_callable = json_to_csv,
    dag = dag
)

other_file_process = PythonOperator(
    task_id = 'other_file_process',
    python_callable = other_file_process,
    dag = dag
)

upload_to_s3 = UploadToS3Operator(
    task_id = "upload_to_s3",
    path = Variable.get("data-upload"),
    exclude = "*.crc",
    bucket = Variable.get("stage_bucket"),
    aws_credentials_id="aws_credentials_dend",
    dag = dag
)

create_table = PostgresOperator(
    task_id = "create_table",
    postgres_conn_id = "redshift_dend",
    sql = [value for value in SqlQueries.create_table.values()],
    dag = dag
)

stage_immigraton = LoadToRedshiftOperator(
    task_id = "stage_immigraton",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_immigraton",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "sas_data",
    region = Variable.get("bucket_region"),
    overwrite = False,
    file_format = 'parquet',
    dag = dag
)

stage_iso_country_region = LoadToRedshiftOperator(
    task_id = "stage_iso_country_region",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_iso_country_region",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "csv/iso_country_region.csv",
    region = Variable.get("bucket_region"),
    ignore_header = 1,
    overwrite = True,
    file_format = 'csv',
    dag = dag
)

stage_i94prtl = LoadToRedshiftOperator(
    task_id = "stage_i94prtl",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_i94prtl",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "csv/i94prtl.csv",
    region = Variable.get("bucket_region"),
    overwrite = True,
    file_format = 'csv',
    dag = dag
)

stage_i94cntyl = LoadToRedshiftOperator(
    task_id = "stage_i94cntyl",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_i94cntyl",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "csv/i94cntyl.csv",
    region = Variable.get("bucket_region"),
    overwrite = True,
    file_format = 'csv',
    dag = dag
)

stage_i94visa = LoadToRedshiftOperator(
    task_id = "stage_i94visa",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_i94visa",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "csv/i94visa.csv",
    region = Variable.get("bucket_region"),
    overwrite = True,
    file_format = 'csv',
    dag = dag
)

stage_airport_codes = LoadToRedshiftOperator(
    task_id = "stage_airport_codes",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_airport_codes",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "csv/airport_codes.csv",
    ignore_header = 1,
    region = Variable.get("bucket_region"),
    overwrite = True,
    file_format = 'csv',
    dag = dag
)

stage_us_cities_demographics = LoadToRedshiftOperator(
    task_id = "stage_us_cities_demographics",
    redshift_conn_id = "redshift_dend",
    aws_credentials_id = "aws_credentials_dend",
    table = "stage_us_cities_demographics",
    s3_bucket = Variable.get("stage_bucket"),
    s3_key = "csv/us_cities_demographics.csv",
    ignore_header = 1,
    delimiter = ";",
    region = Variable.get("bucket_region"),
    overwrite = True,
    file_format = 'csv',
    dag = dag
)

load_immigraton = PostgresOperator(
    task_id = "load_immigraton",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['immigraton'],
    dag = dag
)

load_airport_codes = PostgresOperator(
    task_id = "load_airport_codes",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['airport_codes'],
    dag = dag
)

load_visa_type = PostgresOperator(
    task_id = "load_visa_type",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['visa_type'],
    dag = dag
)

load_country = PostgresOperator(
    task_id = "load_country",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['country'],
    dag = dag
)

load_port_of_entry = PostgresOperator(
    task_id = "load_port_of_entry",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['port_of_entry'],
    dag = dag
)

load_iso_country_region = PostgresOperator(
    task_id = "load_iso_country_region",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['iso_country_region'],
    dag = dag
)

load_us_cities_demographics = PostgresOperator(
    task_id = "load_us_cities_demographics",
    postgres_conn_id = "redshift_dend",
    sql = SqlQueries.load_table['us_cities_demographics'],
    dag = dag
)

data_quality_check = DataQualityOperator(
    task_id = "data_quality_check",
    redshift_conn_id = "redshift_dend",
    tables = [key for key in SqlQueries.create_table if 'stage' not in key],
    trigger_rule = "all_done",
    dag = dag
)

local_file_archive = PythonOperator(
    task_id = 'local_file_archive',
    python_callable = local_file_archive,
    dag = dag
)

s3_file_archive = BashOperator(
    task_id = 's3_file_archive',
    bash_command = "aws s3 mv s3://bucket-vincent/ s3://bucket-vincent-archive/{{ ds_nodash }}/ --recursive",
    dag = dag
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)
start_stage = DummyOperator(task_id='start_stage', dag=dag)
start_load_table = DummyOperator(task_id='start_load_table',trigger_rule = "all_done",dag=dag)

# task dependency
start_operator >> [sas7bdat_to_parquet,sas_desc_to_csv,json_to_csv,other_file_process] >> upload_to_s3
upload_to_s3 >> create_table >> start_stage >> [stage_immigraton ,stage_iso_country_region ,stage_i94prtl,
                                                stage_i94cntyl,stage_i94visa,
                                                stage_airport_codes,stage_us_cities_demographics] >> start_load_table
start_load_table >> [load_immigraton,load_airport_codes,
                     load_visa_type,load_country,load_port_of_entry,
                     load_iso_country_region,
                     load_us_cities_demographics] >> data_quality_check
data_quality_check >> [s3_file_archive,local_file_archive] >> end_operator