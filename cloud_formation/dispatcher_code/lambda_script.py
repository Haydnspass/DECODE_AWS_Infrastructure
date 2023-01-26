import boto3
import json
import os
from utils.queue import SQSJobQueue


def _start_job(job: dict, config: dict):
    """Send job to AWS batch.
    Prepares and mounts EFS directory for logging and storing intermediate results.
    """
    client = boto3.client('batch', region_name=config['region_name'])
    config_batch = config['batch']
    cmd = []
    if job.job_type == 'train':
        param_path = job.attributes['config_file']
        model_path = job.attributes['model_file']
        calib_path = None  #TODO:
        cmd = ['--train', '-c', calib_path, '-m', model_path, '-p', param_path]
    elif job.job_type == 'inference':
        emitter_path = None  #TODO:
        frame_path = None  #TODO:
        frame_meta_path = None  #TODO:
        model_path = job.attributes['model_file']
        cmd = ['--fit', '-e', emitter_path, '-f', frame_path, '-k', frame_meta_path, '-m', model_path]
    client.submit_job(
        jobDefinition=config_batch['job_def_name'],
        jobName=f'decode_job_{job.id}',
        jobQueue=config_batch['queue_name'],  #TODO: select based on decode version
        #containerOverrides={'command': cmd + [f'--out_path={job.id}']},
    )
    #TODO: choose jobdefinition -> Dockerimage based on version used


def _update_api_db(job: dict, config: dict):
    """Update job status on api and store logs location.
    """
    #TODO:
    pass


def lambda_handler(event, context):
    """Handler function.
    Sets up and sends the job to batch.
    """
    # config
    config = json.loads(os.environ['config_json'])
    queues = [SQSJobQueue(name) for name in config['sqs']['queue_names'].values()]
    older_than = config['sqs']['aws_if_older_than']

    jobs_started = []
    for queue in queues:
        while True:
            job = queue.dequeue(older_than=older_than)
            # example for testing:
            # {"id": "id", "job_type": "train", "date_created": "2023-01-20T20:23:54", "date_started": "date_started", "date_finished": "date_finished", "status": "status", "model_id": "model_id", "model": "model", "environment": "environment", "attributes": {"config_file": "", "model_file": ""}}
            if not job:
                break
            _start_job(job=job, config=config)
            api_url = None  #TODO: set api
            #_update_api_db(job=job,, api_url=api_url)
            jobs_started.append(job)

    return {'statusCode': 200, 'body': json.dumps({'jobs_started': jobs_started})}
