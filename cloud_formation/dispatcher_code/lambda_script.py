import boto3
import json
import os
from utils.queue import SQSJobQueue


def _start_job(job: dict, efs_path: str, config: dict):
    """Send job to AWS batch.
    Prepares and mounts EFS directory for logging and storing intermediate results.
    """
    client = boto3.client('batch', region_name=config['region_name'])
    #TODO: create mount point
    #https://aws.amazon.com/premiumsupport/knowledge-center/batch-mount-efs/
    #https://aws.amazon.com/blogs/hpc/introducing-support-for-per-job-amazon-efs-volumes-in-aws-batch/
    config_batch = config['batch']
    cmd = []
    priority = 2
    if job['job_type'] == 'train':
        param_path = job['model']['config_file']
        model_path = job['model']['model_file']
        calib_path = None  #TODO
        cmd = ['--train', '-c', calib_path, '-m', model_path, '-p', param_path]
        priority = 0  # lower prio
    elif job['job_type'] == 'inference':
        emitter_path = None  #TODO
        frame_path = None  #TODO
        frame_meta_path = None  #TODO
        model_path = job['model']['model_file']
        cmd = ['--fit', '-e', emitter_path, '-f', frame_path, '-k', frame_meta_path, '-m', model_path]
        priority = 1  # higher prio
    client.submit_job(
        jobDefinition=config_batch['job_def_name'],
        jobName=f'decode_job_{job.id}',
        jobQueue=config_batch['queue_name'],
        schedulingPriorityOverride=priority,
        #TODO: set (https://docs.aws.amazon.com/batch/latest/userguide/efs-volumes.html)
        #containerOverrides={  
        #    'volumes': [{
        #        'host': {'sourcePath': 'string'},
        #        'name': 'string',
        #        'efsVolumeConfiguration': {
        #            'fileSystemId': 'string',
        #            'rootDirectory': 'string',
        #            'transitEncryption': 'ENABLED'|'DISABLED',
        #            'transitEncryptionPort': 123,
        #            'authorizationConfig': {
        #                'accessPointId': 'string',
        #                'iam': 'ENABLED'|'DISABLED'}}}],
        #    'mountPoints': [{
        #        'containerPath': 'string',
        #        'readOnly': True|False,
        #        'sourceVolume': 'string'}],
        #    'command': cmd}
    )
    #TODO: override Dockerimage based on version used
    #TODO: add cmd params


def _update_api_db(job: dict, efs_path: str, config: dict):
    """Update job status on api and store logs location.
    """
    #TODO
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
            # {"id": "id", "job_type": "job_type", "date_created": "2023-01-20T20:23:54",
            #  "date_started": "date_started", "date_finished": "date_finished", "status": "status",
            #  "model_id": "model_id", "model": "model", "environment": "environment", "attributes": "attributes"}
            if not job:
                break
            efs_path = None  #TODO: set efs_path from config
            _start_job(job=job, efs_path=efs_path, config=config)
            api_url = None  #TODO: set api
            #_update_api_db(job=job, efs_path=efs_path, api_url=api_url)
            #jobs_started.append(job)

    return {'statusCode': 200, 'body': json.dumps({'jobs_started': jobs_started})}
