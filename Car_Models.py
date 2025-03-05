from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def car_models():
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName('ModelCreation') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-dev:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    schema = StructType([
        StructField("model_id", T.IntegerType(), True),
        StructField("car_brand", T.StringType(), True),
        StructField("car_model", T.StringType(), True)
    ])

    car_data = [
        (1, "Mazda", "3"),
        (2, "Mazda", "6"),
        (3, "Toyota", "Corolla"),
        (4, "Hyundai", "i20"),
        (5, "Kia", "Sportage"),
        (6, "Kia", "Rio"),
        (7, "Kia", "Picanto"),
    ]

    df = spark.createDataFrame(car_data, schema)

    ##send to s3
    df.write.parquet('s3a://spark/data/dims/car_models/', mode='overwrite')
    print('----------------------------Upload to S3 completed!----------------------------')


    spark.stop()

if __name__ == "__main__":
    car_models()