from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

def alerting_counter():

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
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()


    #Consume Data from Kafka
    stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka-dev:9092") \
    .option("subscribe", "alert-data") \
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
    df_parsed = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
    .select(F.col('parsed_json.*'))


    # Add processing time timestamp
    df_parsed = df_parsed.withColumn("processing_time", F.current_timestamp())

    # Perform aggregations based on processing time
    df_aggregated = df_parsed \
        .groupBy(F.window(F.col("processing_time"), "1 minutes")) \
        .agg(
            F.count("*").alias("num_of_rows"),
            F.count(F.when(F.col("color_name") == F.lit("Black"), True)).alias("num_of_black"),
            F.count(F.when(F.col("color_name") == F.lit("White"), True)).alias("num_of_white"),
            F.count(F.when(F.col("color_name") == F.lit("Silver"), True)).alias("num_of_silver"),
            F.max(F.col("speed")).alias("maximum_speed"),
            F.max(F.col("gear")).alias("maximum_gear"),
            F.max(F.col("rpm")).alias("maximum_rpm")
        )

    # Write output to console
    df_aggregated.writeStream \
        .outputMode("update") \
        .format("console") \
        .trigger(processingTime="1 minute") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    alerting_counter()