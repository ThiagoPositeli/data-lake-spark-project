import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    this funcion create spark session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    this funcion get the data from json song_data, create temp view table, 
    performs all the necessary treatments and writes in a parquet file.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create temp view of song data for songplays table to join
    df.createOrReplaceTempView("song_data_view")

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id')\
               .parquet(path=output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',\
                              'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """ 
    this funcion get the data from json song_data, create temp view table, 
    performs all the necessary treatments and writes in a parquet file, for user_table, time_table and songplays_table.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users')

    
    # extract columns to create time table
    df = df.withColumn('start_time', (df['ts']/1000).cast('timestamp'))
    df = df.withColumn('weekday', date_format(df['start_time'], 'E'))
    df = df.withColumn('year', year(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('hour', hour(df['start_time']))
    time_table = df.select('start_time', 'weekday', 'year', 'month',\
                           'week', 'day', 'hour').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month')\
              .parquet(path=output_data + 'time')
    
    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM song_data_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title)\
                                       & (df.artist == song_df.artist_name)\
                                       & (df.length == song_df.duration), "inner")\
                        .distinct()\
                        .select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent',\
                                df['year'].alias('year'), df['month'].alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month')\
                   .parquet(path=output_data + 'songplays')



def main():
    spark = create_spark_session()
    input_data = SOURCE_S3_BUCKET #variable from dl.cfg
    output_data = DEST_S3_BUCKET #variable from dl.cfg
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()