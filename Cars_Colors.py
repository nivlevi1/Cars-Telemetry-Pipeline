from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def cars_colors():
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
        StructField("color_id", T.IntegerType(), True),
        StructField("color_name", T.StringType(), True),
    ])

    color_data = [
        (1, "Black"),
        (2, "Red"),
        (3, "Gray"),
        (4, "White"),
        (5, "Green"),
        (6, "Blue"),
        (7, "Pink"),
    ]

    df = spark.createDataFrame(color_data, schema)

    ##send to s3
    df.write.parquet('s3a://spark/data/dims/car_colors/', mode='overwrite')
    print('----------------------------Upload to S3 completed!----------------------------')


    spark.stop()

if __name__ == "__main__":
    cars_colors()