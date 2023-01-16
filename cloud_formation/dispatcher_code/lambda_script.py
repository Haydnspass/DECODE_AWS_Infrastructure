import boto3
import json
from .queue import SQSJobQueue


def _pull_queue(queue: SQSJobQueue, older_than: float) -> dict | None:
    """Pulls an SQS queue to get a new job.
    """
    #TODO: queue as submodule
    return queue.deque(older_than=older_than)


def _start_job(job: dict, efs_path: str, config: dict):
    """Send job to AWS batch.
    Prepares and mounts EFS directory for logging and storing intermediate results.
    """
    client = boto3.client('batch', region_name='eu-central-1')  #TODO: add flexibility
    #https://aws.amazon.com/premiumsupport/knowledge-center/batch-mount-efs/
    #https://aws.amazon.com/blogs/hpc/introducing-support-for-per-job-amazon-efs-volumes-in-aws-batch/
    #TODO: create mount point
    client.submit_job(
        jobDefinition=config['job_definition_name'],
        jobName=f'decode_job_{job.id}',
        jobQueue=config['job_queue_name'],
        #containerOverrides={  #TODO: set (https://docs.aws.amazon.com/batch/latest/userguide/efs-volumes.html)
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
        #        'sourceVolume': 'string'}]}
    )
    #TODO: override Dockerimage based on version used
    #TODO: add params


def _update_api_db(job: dict, efs_path: str, config: dict):
    """Update job status on api and store logs location.
    """
    pass


def lambda_handler(event, context):
    # config
    config = json.loads(context['Variables']['config_json'])
    # here and not in _pull_queue to avoid initializing more than once
    queues = [SQSJobQueue(name) for name in config['sqs']['queue_names'].values()]
    older_than = config['sqs']['aws_if_older_than']

    jobs_started = []
    for queue in queues:
        while True:
            job = _pull_queue(queue, older_than)
            if not job:
                break
            efs_path = None  #TODO
            _start_job(job=job, efs_path=efs_path, config=config['batch'])
            api_url = None  #TODO
            _update_api_db(job=job, efs_path=efs_path, api_url=api_url)
            jobs_started.append(job)

    return {'statusCode': 200, 'body': json.dumps({'jobs_started': jobs_started})}
