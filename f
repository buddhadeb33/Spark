# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')  # Change region if needed

# Define Glue job name
job_name = "my-glue-job"

# Start the job with a specific worker type and number of workers
response = glue_client.start_job_run(
    JobName=job_name,
    Arguments={
        '--worker-type': 'G.2X',   # Change this to G.1X, G.2X, G.4X, etc.
        '--number-of-workers': '10'  # Adjust based on workload
    }
)

# Print Job Run ID
print(f"Started AWS Glue Job with ID: {response['JobRunId']}")



###
import time

job_run_id = response['JobRunId']

while True:
    job_status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    state = job_status['JobRun']['JobRunState']
    
    print(f"Job Status: {state}")
    
    if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
        break
    
    time.sleep(10)  # Check every 10 seconds



    ###

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark & Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Tune Spark settings
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.executor.memory", "10g")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Read JSON with optimized settings
df = spark.read.option("inferSchema", "true").json("s3://my-bucket/sample.json").repartition(50)

# Convert to AWS Glue DynamicFrame
dyf = DynamicFrame.fromDF(df, glueContext, "df_dyf")

# Optimize partitioning
dyf = dyf.repartition(50)

# Convert back to DataFrame (if needed)
df = dyf.toDF()

# Show Data
df.show()

