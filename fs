import boto3

glue_client = boto3.client('glue', region_name='us-east-1')

job_name = 'my-glue-job'
script_location = 's3://your-bucket/path/glue_script.py'
iam_role = 'arn:aws:iam::123456789012:role/your-glue-job-role'  # Replace with your actual IAM role ARN

try:
    response = glue_client.create_job(
        Name=job_name,
        Role=iam_role,
        Command={
            'Name': 'glueetl',            # Specifies an ETL job.
            'ScriptLocation': script_location,
            'PythonVersion': '3'
        },
        DefaultArguments={
            '--job-language': 'python'
        },
        Timeout=2880,                    # Timeout in minutes.
        GlueVersion='2.0'                # Adjust if necessary.
    )
    print(f"Created Glue job '{job_name}'.")
except glue_client.exceptions.AlreadyExistsException:
    print(f"Glue job '{job_name}' already exists. Using existing configuration.")

### ---

import boto3
import time

# -------------------------------
# Step 1: Create a Glue client.
# -------------------------------
# Replace 'us-east-1' with your appropriate AWS region.
glue_client = boto3.client('glue', region_name='us-east-1')

# -------------------------------
# Step 2: Specify your Glue Job details.
# -------------------------------
job_name = 'my-glue-job'  # Replace with the name of your pre-configured Glue job

# These are the runtime arguments your Glue script expects.
# For example, if your script reads an input S3 path and writes to an output S3 path.
job_arguments = {
    '--JOB_NAME': job_name,  # Your script might use this if required.
    '--INPUT_PATH': 's3://your-input-bucket/path/',    # Update with your input data S3 path.
    '--OUTPUT_PATH': 's3://your-output-bucket/path/'    # Update with your output data S3 path.
}

# -------------------------------
# Step 3: Start the Glue Job Run.
# -------------------------------
response = glue_client.start_job_run(
    JobName=job_name,
    Arguments=job_arguments
)

job_run_id = response['JobRunId']
print(f"Glue job '{job_name}' started successfully. JobRunId: {job_run_id}")

# -------------------------------
# Step 4: (Optional) Poll for Job Status.
# -------------------------------
# This loop polls the job status until it reaches a terminal state.
while True:
    job_run = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    status = job_run['JobRun']['JobRunState']
    print("Current Job Status:", status)
    
    if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
        break
    
    # Wait for 30 seconds before checking again.
    time.sleep(30)

print("Final Job Status:", status)
