import boto3
import json
import os
from fastapi.encoders import jsonable_encoder
from utils.queue import SQSJobQueue


def _start_job(job: dict, config: dict):
    """Send job to AWS batch.
    Prepares and mounts EFS directory for logging and storing intermediate results.
    """
    client = boto3.client('batch', region_name=config['region_name'])
    config_batch = config['batch']
    log_path = None
    if job.job_type == 'train':
        param_path = job.attributes['config_file']
        model_path = job.attributes['model_file']
        calib_path = job.attributes['calib_file']  #TODO: add in API
        log_path = f's3://{config["s3"]["bucket_name"]}{config["s3"]["logs_root_path"]}/{job.id}/'  #TODO: add in API
        cmd = ['--train', '-c', calib_path, '-m', model_path, '-p', param_path, '-l', log_path]
    elif job.job_type == 'inference':
        emitter_path = job.attributes['emitter_file']  #TODO: add in API
        frame_path = job.attributes['frame_file']  #TODO: add in API
        frame_meta_path = job.attributes['frame_meta_file']  #TODO: add in API
        model_path = job.attributes['model_file']
        cmd = ['--fit', '-e', emitter_path, '-f', frame_path, '-k', frame_meta_path, '-m', model_path]
    else:
        raise ValueError(f'Job must be of type either "train" or "fit", not {job.job_type}.')
    client.submit_job(
        jobDefinition=config_batch['job_def_name'],  #TODO: select based on decode version
        jobName=f'decode_job_{job.id}',
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
            if not job:
                break
            log_path = _start_job(job=job, config=config)
            api_url = None  #TODO: set api
            #_update_api_db(job=job, api_url=api_url, log_path=log_path)
            jobs_started.append(jsonable_encoder(job))

    return {'statusCode': 200, 'body': json.dumps({'jobs_started': jobs_started})}
