import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType # needed for udf return type
import datetime # needed for python function to return datetimestamp 

from datetime import datetime, timedelta
from pyspark.sql import types as T

from pyspark.sql.functions import * # for regexp_replace

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_demographics_data(spark, input_data, output_data):
    """
    Summary : Procedure to process Song data from S3 song files and write song and Artist parquet files back to S3
    
    Parameters
    spark - The spark session creted in the main function
    input_data - The location of the root folder on S3 under which all the json files are stored. 
    output_data - The location of the root folder on S3 under which all the processed parquet files will be stored.
    
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*'
    
    # read song data file
    #df = spark.read.csv(song_data)  -- assuming data is in json format
    rdd = spark.read.format('csv').options(header='true', delimiter=';') \
                    .load('us-cities-demographics.csv')
    
    rdd.createOrReplaceTempView("state_data") 
    
    # Populate the Dim State
    rdd_state_data = spark.sql('''
                                SELECT distinct  `State Code` Code , State
                                FROM state_data  
                                where  `State Code` is not null
                                ''')
    rdd_state_data.write.mode("overwrite").parquet(os.path.join(output_data,'states.parquet') )
    print('State Dim populated')
    
    rdd_demographics_data = spark.sql('''
                            SELECT distinct city , `Median Age` , `Male Population` , `Female Population` ,
                                    `Total Population` , `Number of Veterans` , `Foreign-born` , 
                                   `Average Household Size` , `State Code`
                            FROM state_data  
                            where  `State Code` is not null
                        ''')
    
    rdd_demographics_data.write.mode("overwrite").parquet(os.path.join(output_data,'demographics.parquet') )
    print('demographics Dim populated')
    
    print ('demographics extract completed')  

    
def process_airports_data(spark, input_data, output_data):
    """
    Summary : Procedure to process Song data from S3 song files and write song and Artist parquet files back to S3
    
    Parameters
    spark - The spark session creted in the main function
    input_data - The location of the root folder on S3 under which all the json files are stored. 
    output_data - The location of the root folder on S3 under which all the processed parquet files will be stored.
    
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*'
    
    # read song data file
    #df = spark.read.csv(song_data)  -- assuming data is in json format
    dfarpt = spark.read.format('csv').options(header='true', delimiter=',') \
                    .load('airport-codes_csv.csv')    
    
    # filter for anly US airports
    dfarpt = dfarpt.filter("iso_country = 'US'") \
                   .withColumn('state', regexp_replace('iso_region', 'US-', ''))
    
    #drop duplicate data if exists
    dfarpt =  dfarpt.dropDuplicates(['ident','type'])
    
    dfarpt.write.mode("overwrite").parquet(os.path.join(output_data,'airports.parquet') )
    print('Airports Dim populated')
    
    

def process_immig_data(spark, input_data, output_data):
    """
    Summary : Procedure to process log data from S3 song files and extract and write User, time and songdata parquet files back to S3
    
    Parameters
    spark - The spark session creted in the main function
    input_data - The location of the root folder on S3 under which all the json files are stored. 
    output_data - The location of the root folder on S3 under which all the processed parquet files will be stored.
    
    Python functions needed to convert epoch time in logs to datetimestamp to extract all time relevant information.
    
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*'

    # read log data file
    immg_data = spark.read.format('csv').options(header='true', delimiter=',' ) \
                    .load('immigration_data_sample.csv')
    
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None
    
    # cleanup
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    immg_data = immg_data \
        .withColumn("i94yr", col("i94yr").cast("integer")) \
        .withColumn("i94mon", col("i94mon").cast("integer")) \
        .withColumn("i94cit", col("i94cit").cast("integer")) \
        .withColumn("i94res", col("i94res").cast("integer")) \
        .withColumn("i94visa", col("i94visa").cast("integer")) \
        .withColumn("biryear", col("biryear").cast("integer")) \
        .withColumn("admnum", col("admnum").cast("integer")) \
        .withColumn("arrival_date", udf_datetime_from_sas(col("arrdate").cast("integer"))) \
        .withColumn("departure_date", udf_datetime_from_sas(col("depdate").cast("integer"))) 

    #drop duplicates
    immg_data = immg_data.distinct()

    immg_data.createOrReplaceTempView("immg_data")    
    
    # write time table to parquet files partitioned by year and month
    immg_data.write.partitionBy("i94yr","i94mon").mode("overwrite")
        .parquet(os.path.join(output_data,'immg_data.parquet'))

    print ('Immig data extract completed')  

    #Add the time dimension
    time_data = spark.sql('''
            SELECT DISTINCT arrival_date  , year(arrival_date) as Year ,
                                        month(arrival_date) as month , day(arrival_date) as day,
                                        weekofyear(arrival_date) as weekofyear , 
                                        weekday(arrival_date) as weekday
                            FROM immg_data  
                            where arrival_date is not null
            union all 
            SELECT DISTINCT departure_date  , year(departure_date) as Year ,
                                        month(departure_date) as month , day(departure_date) as day,
                                        weekofyear(departure_date) as weekofyear , 
                                        weekday(departure_date) as weekday
                            FROM immg_data  
                            where departure_date is not null            
                        ''')
    
    time_data.write.partitionBy("year").mode("overwrite")
        .parquet(os.path.join(output_data,'time.parquet'))
    
    print ('Immig data extract completed')

 

def data_Quality_checks(spark, input_data, output_data):
    """
    Summary
    
    """
    
    # read in song data to use for songplays table
    song_output = output_data + 'songs.parquet'
    song_df = spark.read.parquet(song_output)
    song_df.createOrReplaceTempView("song_data")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT  row_number() over (order by TimestampType) as songplay_id, TimestampType as start_time , 
                                           userId , level , s.song_id , s.artist_id ,
                                           sessionId, location, userAgent , year(TimestampType) as year , month(TimestampType) as month 
                                FROM Log_data_Timestamp l  INNER JOIN song_data s ON l.song = s.title 

                                ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(os.path.join(output_data,'songplays.parquet'))

    print ('Data Quality checks completed')  
    
def main():
    """
    Summary 
    Main function which will be called and will invoke sub procedures to load and process log data from S3 bucket.
    
    This procedure will first invoke and create a Spark session 
    Variable input_data will store location of raw log files in S3 bucket 
    Variable output_data will store the location of the files where the final processed parquet files will be stored.
    
    """
    spark = create_spark_session()
    input_data =   "" #s3a://udacity-dend/"  ## "udacity-dend/"  ## 
    output_data =  "Output/" #"s3a://udacity-dend/Output121220/"   ## "udacity-dend/Output/"   ##  
    
    process_demographics_data(spark, input_data, output_data)    
    process_airports_data(spark, input_data, output_data)
    process_immig_data(spark, input_data, output_data)    
    data_Quality_checks(spark, input_data, output_data):

    print ('Spark activity completed')    

if __name__ == "__main__":
    main()
