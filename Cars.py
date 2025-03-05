from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def cars():
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
        StructField("car_id", T.IntegerType(), True),
        StructField("driver_id", T.IntegerType(), True),
        StructField("model_id", T.IntegerType(), True),
        StructField("color_id", T.IntegerType(), True),
    ])

    df = spark.range(20).select(
        (F.floor(F.rand() * 9000000) + 1000000).alias("car_id"),
        (F.floor(F.rand() * 900000000) + 100000000).alias("driver_id"),
        (F.floor(F.rand() * 7) + 1).alias("model_id"),
        (F.floor(F.rand() * 7) + 1).alias("color_id")  
    )

    df_with_schema = spark.createDataFrame(df.rdd, schema)

    # Send to S3
    df.write.parquet('s3a://spark/data/dims/cars/', mode='overwrite')
    print('----------------------------Upload to S3 completed!----------------------------')

    spark.stop()

if __name__ == "__main__":
    cars()