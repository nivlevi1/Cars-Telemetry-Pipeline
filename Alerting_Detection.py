from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

def alerting_detection():
    #Initialize Spark Session
    spark = SparkSession \
    .builder \
    .master("local") \
    .appName('AlertDetection') \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-dev:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()


    #Consume Data from Kafka
    stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka-dev:9092") \
    .option("subscribe", "samples-enriched") \
    .option('startingOffsets', 'earliest') \
    .load() \
    .select(F.col('value').cast(T.StringType()))  # Select the value column and cast to String

    #Define JSON Schema for Incoming Data
    json_schema = T.StructType([
        T.StructField("event_id", T.StringType(), False),    # UUID as a string
        T.StructField("event_time", T.StringType(), False),  # Timestamp as string
        T.StructField("car_id", T.IntegerType(), False),     # Car ID as Integer
        T.StructField("speed", T.IntegerType(), False),      # Speed as Integer
        T.StructField("rpm", T.IntegerType(), False),        # RPM as Integer
        T.StructField("gear", T.IntegerType(), False),       # Gear as Integer
        T.StructField("driver_id", T.StringType(), False),        # driver_id as Integer
        T.StructField("brand_name", T.StringType(), False),        # brand_name as Integer
        T.StructField("model_name", T.StringType(), False),        # model_name as Integer
        T.StructField("color_name", T.StringType(), False),        # color_name as Integer
        T.StructField("expected_gear", T.IntegerType(), False)        # expected_gear as Integer
    ])

    # Parse the JSON string into individual columns
    parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
    .select(F.col('parsed_json.*'))


    # Filter the df where Speed is greater than 120 & Expected gear is not equals to actual gear & RPM is greater than 6000
    filtered_df = parsed_df.filter(
        (F.col('speed') >= F.lit('120')) &
        (F.col('expected_gear') != F.col('gear')) &
        (F.col('rpm') >= F.lit('6000'))
    )

    # Converting to Json                   
    fields_list = filtered_df.schema.fieldNames() #Fetching Field Names:
    fields_as_cols = list(map(lambda col_name: F.col(col_name), fields_list)) #Mapping Field Names to Columns:
    json_df = filtered_df \
    .withColumn('to_json_struct', F.struct(fields_as_cols)) \
    .select(F.to_json(F.col('to_json_struct')).alias('value'))

    #Writing the Stream to Kafka:
    query = json_df \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka-dev:9092") \
    .option("topic", "alert-data") \
    .option('checkpointLocation', 's3a://spark/checkpoints/cars/DataAlerting/review_calculation') \
    .outputMode('update') \
    .start()

    query.awaitTermination()
    spark.stop()



    # ########### Write Stream to Console ########### Can't use .show()
    # query = filtered_df.writeStream.outputMode("append").format("console").option("truncate", False).start()

    # query.awaitTermination()  # Keep the stream running

if __name__ == "__main__":
    alerting_detection()

