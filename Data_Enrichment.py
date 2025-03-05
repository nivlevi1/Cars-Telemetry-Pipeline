from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

def data_enrichment():
    #Initialize Spark Session
    spark = SparkSession \
    .builder \
    .master("local") \
    .appName('DataEnrichment') \
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
    .option("subscribe", "sensors-sample") \
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
        T.StructField("gear", T.IntegerType(), False)        # Gear as Integer
    ])

    # Parse the JSON string into individual columns
    parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
    .select(F.col('parsed_json.*'))



    # Enrich the data
    cars_df = spark.read.parquet('s3a://spark/data/dims/cars/')
    cars_colors_df = spark.read.parquet('s3a://spark/data/dims/car_colors/')
    cars_models_df = spark.read.parquet('s3a://spark/data/dims/car_models/')


    # Enrich parsed_df with car details
    Enriched_df = parsed_df.join(cars_df, parsed_df.car_id == cars_df.car_id, 'left') \
                            .join(cars_models_df, cars_df.model_id == cars_models_df.model_id, 'left') \
                            .join(cars_colors_df, cars_df.color_id == cars_colors_df.color_id, 'left') \
                            .select(
                                    parsed_df.event_id,
                                    parsed_df.event_time,
                                    parsed_df.car_id,
                                    parsed_df.speed,
                                    parsed_df.rpm,
                                    parsed_df.gear,
                                    cars_df.driver_id.cast(T.StringType()),
                                    cars_models_df.car_brand.cast(T.StringType()).alias('brand_name'),
                                    cars_models_df.car_model.cast(T.StringType()).alias('model_name'),
                                    cars_colors_df.color_name.cast(T.StringType()),
                                    F.round(parsed_df.speed / 30).cast(T.IntegerType()).alias("expected_gear"),
                            )                   

    # Converting to Json                   
    fields_list = Enriched_df.schema.fieldNames() #Fetching Field Names:
    fields_as_cols = list(map(lambda col_name: F.col(col_name), fields_list)) #Mapping Field Names to Columns:
    json_df = Enriched_df \
    .withColumn('to_json_struct', F.struct(fields_as_cols)) \
    .select(F.to_json(F.col('to_json_struct')).alias('value'))

    #Writing the Stream to Kafka:
    query = json_df \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka-dev:9092") \
    .option("topic", "samples-enriched") \
    .option('checkpointLocation', 's3a://spark/checkpoints/cars/DataEnrichment/review_calculation') \
    .outputMode('update') \
    .start()



    query.awaitTermination()

    cars_df.unpersist()
    cars_models_df.unpersist()
    cars_colors_df.unpersist()

    spark.stop()

if __name__ == "__main__":
    data_enrichment()

    # ########### Write Stream to Console ########### Can't use .show()
    # query = json_df.writeStream \
    # .outputMode("append") \
    # .format("console") \
    # .start()
