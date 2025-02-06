import boto3
from pyspark.sql import SparkSession

def get_auto_spark_session(s3_bucket, s3_prefix, format="parquet"):
    """
    Dynamically configures Spark session based on dataset size in S3.
    
    Parameters:
        s3_bucket (str): S3 bucket name.
        s3_prefix (str): Folder (prefix) in the S3 bucket.
        format (str): File format (parquet, csv, json, etc.).
    
    Returns:
        SparkSession: Optimized Spark session.
    """

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Estimate dataset size from S3
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    file_size_gb = sum(obj['Size'] for obj in response.get('Contents', [])) / (1024**3) if 'Contents' in response else 0

    # Auto-set Spark parameters
    num_partitions = max(10, int(file_size_gb * 100))  # Auto partitioning (100 per GB)
    executor_memory = max(4, int(file_size_gb * 2))  # Auto memory allocation (2GB per GB)

    # Create an adaptive Spark session
    spark = SparkSession.builder \
        .appName("AutoOptimizedSparkSession") \
        .config("spark.sql.adaptive.enabled", "true") \  # Auto-tune query execution
        .config("spark.dynamicAllocation.enabled", "true") \  # Auto-scale executors
        .config("spark.shuffle.service.enabled", "true") \  # Needed for dynamic scaling
        .config("spark.sql.shuffle.partitions", num_partitions) \  # Auto partitions
        .config("spark.executor.memory", f"{executor_memory}g") \  # Auto memory
        .getOrCreate()

    print(f"✅ Spark Auto-Configured:")
    print(f"- Dataset Size: {file_size_gb:.2f} GB")
    print(f"- Auto Partitions: {num_partitions}")
    print(f"- Auto Executor Memory: {executor_memory} GB")

    return spark

def read_s3_data_auto(spark, s3_bucket, s3_prefix, format="parquet"):
    """
    Reads large datasets from S3 with auto-optimized settings in Spark.

    Parameters:
        spark (SparkSession): Spark session.
        s3_bucket (str): S3 bucket name.
        s3_prefix (str): Folder (prefix) in S3.
        format (str): Data format (parquet, csv, json, etc.).

    Returns:
        DataFrame: Spark DataFrame.
    """

    # Construct S3 path
    s3_path = f"s3a://{s3_bucket}/{s3_prefix}"

    # Read data efficiently
    df = spark.read \
        .format(format) \
        .option("header", "true") \  # Only for CSV files
        .option("inferSchema", "true") \  # Helps with data type optimization
        .load(s3_path)

    # Auto repartition based on cluster size
    df = df.repartition("auto")  # Adaptive partitioning (enabled by AQE)

    print(f"✅ Data Loaded from {s3_path}")
    print(f"- Estimated Rows: {df.count()} (approx.)")
    print(f"- Adaptive Partitions: {df.rdd.getNumPartitions()}")

    return df


 s3_bucket = "dev07-cc1-mldev-s3-mlops-rdat-workspace"
s3_prefix = "stimulated_data_big_data_v3/"

# Create auto-optimized Spark session
spark = get_auto_spark_session(s3_bucket, s3_prefix)

# Read data auto-optimized
df = read_s3_data_auto(spark, s3_bucket, s3_prefix, format="parquet")

# Show schema and sample
df.printSchema()
df.show(5)

