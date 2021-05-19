import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
    Create SPARK session to process data
    
    Output:
    spark -- SPARK session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    """
    Extract and load JSON input data (song_data) from input_data path.
    Process the extracted data. Extract relevant columns to create songs_table and artists_table.
    Store the queried data as parquet files at output_data path.
    
    Input arguments:
    spark -- reference to SPARK session
    input_data -- path to input_data to be processed (song_data)
    output_data -- path to location to store the output (parquet files)
    
    Output:
    songs_table -- reference to songs_table data
    artists_table -- reference to artists_table data
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(*["song_id", "title", "artist_id", "year", "duration"])
    songs_table = songs_table.dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table/" + "songs_table" + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f'))

    # extract columns to create artists table
    artists_table = df.select(*['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']) \
                      .orderBy("artist_id", ascending=False) \
                      .dropDuplicates(["artist_id"])
    
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
                        .withColumnRenamed("artist_location", "location") \
                        .withColumnRenamed("artist_latitude", "latitude") \
                        .withColumnRenamed("artist_longitude", "longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table/" + "artists_table" + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f'))
    return songs_table, artists_table



def process_log_data(spark, input_data, output_data):
    """
    Extract and load JSON input data (log_data) from input_data path.
    Process the extracted data. Extract relevant columns to create users_table and time_table.
    Load song_data and join wih log_data. Use joined data/table to extract relevant columns to create songplays_table.
    Store the queried data as parquet files at output_data path.
    
    Input arguments:
    spark -- reference to SPARK session
    input_data -- path to input_data to be processed (log_data)
    output_data -- path to location to store the output (parquet files)
    
    Output:
    users_table -- reference to songs_table data
    time_table -- reference to time_table data
    songplays_table -- reference to songplays_table data
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df["page"]=='NextSong')

    # extract columns for users table    
    users_table = df.select(*['userId', 'firstName', 'lastName', 'gender', 'level']).orderBy("lastName").dropDuplicates(["userId"])
    users_table = users_table.withColumnRenamed('userId', 'user_id') \
                        .withColumnRenamed('firstName', 'first_name') \
                        .withColumnRenamed('lastName', 'last_name')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table/" + "users_table" + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(f=lambda ts: datetime.fromtimestamp(ts/1000.0), returnType=TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000.0).strftime('%Y-%m-%d %H:%M:%S'), returnType=StringType())
    df = df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    df.createOrReplaceTempView("df_fil")
    time_table = spark.sql('''SELECT DISTINCT datetime AS start_time,
                                          HOUR(datetime) AS hour,
                                          DAY(datetime) AS day,
                                          WEEKOFYEAR(datetime) AS week,
                                          MONTH(datetime) AS month,
                                          YEAR(datetime) AS year,
                                          DAYOFWEEK(datetime) AS weekday
                              FROM df_fil
                        ''').dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table/" + "time_table" + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f'))

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table
    df_joined = df.join(song_df, on=(df.artist==song_df.artist_name) & (df.song==song_df.title), how='inner')
    df_joined = df_joined.withColumn("songplay_id", F.monotonically_increasing_id())
    df_joined.createOrReplaceTempView("df_joined")

    songplays_table = spark.sql('''SELECT songplay_id,
                                          timestamp AS start_time,
                                          userId AS user_id,
                                          level,
                                          song_id,
                                          artist_id,
                                          sessionId AS session_id,
                                          artist_location AS location,
                                          userAgent AS user_agent,
                                          year,
                                          MONTH(timestamp) AS month
                                   FROM df_joined
                                   ORDER BY (user_id, session_id)
                        ''').dropDuplicates(["songplay_id"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table/" + "songplays_table" + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f'))
    return users_table, time_table, songplays_table



def main():
    """
    Extract and load JSON input data (song_data and log_data) from input_data path.
    Process the extracted data. Extract relevant columns to create songs_table, artists_table, users_table, 
    time_table and songplays_table.
    Store the queried data as parquet files at output_data path.
    
    Input arguments:
    spark -- reference to SPARK session
    input_data -- path to input_data to be processed (log_data)
    output_data -- path to location to store the output (parquet files)
    
    Output:
    users_table -- reference to songs_table data
    time_table -- reference to time_table data
    songplays_table -- reference to songplays_table data
    """
    
    spark = create_spark_session()
    input_data = "./data/" #"s3a://udacity-dend/"
    output_data = "./data/output_data/" #"s3a://project4-data/output_data/"
    
    songs_table, artists_table = process_song_data(spark, input_data, output_data)    
    users_table, time_table, songplays_table = process_log_data(spark, input_data, output_data)
    
    ### Sample Queries
    # Queries on Songplay analysis
    table_count = lambda table: table.count() 
    print("Number of records in songs table: ", table_count(songs_table))
    print("Number of records in users table: ", table_count(users_table))  
    print("Number of records in artists table: ", table_count(artists_table))  
    print("Number of records in time table: ", table_count(time_table))  
    print("Number of records in songplays table: ", table_count(songplays_table))  

    # Get info of songs with duration greater than 200s limit to 10 records
    songplays_table = songplays_table.drop(*['year', 'month'])
    print(songplays_table.join(songs_table, on=(songs_table.song_id==songplays_table.song_id)) \
                    .filter("duration>200") \
                    .select("title", "year", "duration").limit(10).show())

if __name__ == "__main__":
    main()
