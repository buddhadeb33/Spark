import boto3
from pyspark.sql import SparkSession

def get_optimal_spark_session(s3_bucket, s3_prefix, format="parquet"):
    """
    Inspects dataset size from an S3 bucket and dynamically configures a Spark session.

    Parameters:
        s3_bucket (str): S3 bucket name.
        s3_prefix (str): Prefix (folder path) inside the bucket.
        format (str): File format (parquet, csv, json, etc.).

    Returns:
        SparkSession: Optimized Spark session.
    """

    # Initialize S3 client
    s3 = boto3.client('s3')

    # List objects under the given prefix
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    # Calculate total dataset size
    file_size_gb = sum(obj['Size'] for obj in response.get('Contents', [])) / (1024**3) if 'Contents' in response else 0

    # Adaptive number of partitions based on data size
    num_partitions = max(10, int(file_size_gb * 100))  # 100 partitions per GB (adjustable)

    # Set executor memory dynamically (2GB per 1GB of data)
    executor_memory = max(4, int(file_size_gb * 2))  # At least 4GB memory

    # Create optimized Spark session
    spark = SparkSession.builder \
        .appName("AutoOptimizedSparkSession") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.sql.shuffle.partitions", num_partitions) \
        .config("spark.executor.memory", f"{executor_memory}g") \
        .getOrCreate()

    print(f"✅ Spark Configurations Applied:")
    print(f"- Estimated Dataset Size: {file_size_gb:.2f} GB")
    print(f"- Partitions Set: {num_partitions}")
    print(f"- Executor Memory Set: {executor_memory} GB")

    return spark


s3_bucket = "dev07-cc1-mldev-s3-mlops-rdat-workspace"
s3_prefix = "stimulated_data_big_data_v3/"

spark = get_optimal_spark_session(s3_bucket, s3_prefix, format="parquet")
