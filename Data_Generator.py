import time
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer

def data_generator():
    # Initialize Spark session
    spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('DataGenerator') \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-dev:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

    # Read car data
    car_df = spark.read.parquet('s3a://spark/data/dims/cars')

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='course-kafka-dev:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        # Generate event data for all cars
        df = car_df.select(
            F.expr("uuid()").alias("event_id"),
            F.current_timestamp().cast(T.StringType()).alias("event_time"),
            F.col("car_id").cast(T.IntegerType()),
            F.floor(F.rand() * 200).cast(T.IntegerType()).alias("speed"),
            F.floor(F.rand() * 8000).cast(T.IntegerType()).alias("rpm"),
            F.floor(F.rand() * 7 + 1).cast(T.IntegerType()).alias("gear")
        )

        # Convert DataFrame to JSON and send to Kafka
        for row in df.collect():
            json_data = row.asDict()
            producer.send(topic='sensors-sample', value=json_data)

        producer.flush()
        # print('Sent 20 events to Kafka')

        # Sleep for 2 second before generating the next batch
        time.sleep(2)

    
    producer.close()
    spark.stop()

if __name__ == "__main__":
    data_generator()