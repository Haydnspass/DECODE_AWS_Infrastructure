import boto3
import json
import os
from utils.queue import SQSJobQueue


def _start_job(job: dict, config: dict):
    """Starts training/fitting job on AWS batch.

    :param job: job information as a dictionary. Required keys:
        - id: job id
        - job_type: 'train' or 'fit'
        - job_def_revision: number of the revision of the batch job definition to use (dependent on the DECODE version)
        - attributes: dict of command line arguments (key-value)
    :param config: AWS configuration dict.
    :returns: log_path: S3 path to the directory where the training logs are output (None if job['job_type'] is 'fit')
    """
    client = boto3.client('batch', region_name=config['region_name'])
    config_batch = config['batch']
    config_s3 = config['s3']
    log_path = None

    # job type-specific arguments
    job_type = job['job_type']
    if job_type == 'train':
        log_path = f"s3://{config_s3['bucket_name']}/{config_s3['logs_root_path']}/{job['id']}/"  #TODO: add in API
        cmd = ['--train', '-l', log_path]
    elif job_type == 'inference':
        cmd = ['--fit']
    else:
        raise ValueError(f"Job must be of type either 'train' or 'fit', not {job_type}.")

    cmd = cmd + [f"-m {job['model_path']}"] + [f"--{k}={v}" for k, v in job['attributes'].items()]
    client.submit_job(
        # job definition: name from config, revision from job dict
        jobDefinition=f"{config_batch['job_defs'][job_type]['name']}:{job['job_def_revision']}",
        jobName=f"decode_job_{job['id']}",
        jobQueue=config_batch['queue_name'],
        containerOverrides={'command': cmd},
    )
    return log_path


def _update_api_db(job: dict, config: dict, log_path: str):
    """Update job status on api and store logs location.
    """
    #TODO:
    pass


def lambda_handler(event, context):
    """Handler function: pulls messages from queues, sets up, and sends the job to batch.
    """
    config = json.loads(os.environ['config_json'])
    queues = [SQSJobQueue(name) for name in config['sqs']['queue_names'].values()]
    older_than = config['sqs']['aws_if_older_than']

    jobs_started = []
    for queue in queues:
        while True:
            job = queue.dequeue(older_than=older_than)
            if not job:
                break
            log_path = _start_job(job=job, config=config)
            api_url = None  #TODO: set api
            #_update_api_db(job=job, api_url=api_url, log_path=log_path)
            jobs_started.append(job)

    return {'statusCode': 200, 'body': json.dumps({'jobs_started': jobs_started})}
