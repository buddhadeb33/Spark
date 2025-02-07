from pyspark.sql import SparkSession

# Create a custom SparkSession (bypassing AWS Glue)
spark = SparkSession.builder \
    .appName("CustomSparkSession") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Verify SparkSession
print(spark.version)
