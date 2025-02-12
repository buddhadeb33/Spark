import boto3

# Create a Glue client (adjust region as needed)
glue_client = boto3.client('glue', region_name='us-east-1')

# Start the Glue job by specifying the job name and required arguments.
response = glue_client.start_job_run(
    JobName='my-glue-job',  # Replace with your Glue job name
    Arguments={
        '--JOB_NAME': 'my-glue-job',f
        '--INPUT_PATH': 's3://my-input-bucket/path/',
        '--OUTPUT_PATH': 's3://my-output-bucket/path/'
    }
)

print("Job started successfully:", response)
