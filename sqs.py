import boto3
from utils import get_config


def setup(skip_if_exists=False):
    """Creates the queues defined in the configuration.
    
    Returns: queue_urls (dict of name: url pairs)
    """
    config = get_config()['sqs']
    queue_names = config['queue_names']
    sqs_client = boto3.client('sqs')

    queue_urls = dict()

    for queue_name in queue_names:
        # SQS fifo requires .fifo ending
        queue_name = queue_name + '.fifo' if not queue_name.endswith('.fifo') else queue_name

        # handle existing queues
        try:
            queue_url = sqs_client.get_queue_url(QueueName=queue_name)
            queue_exists = True
        except sqs_client.exceptions.QueueDoesNotExist:
            queue_exists = False

        if queue_exists:
            if not skip_if_exists:
                raise ValueError(f"A queue named {queue_name} already exists.")
        else:    
            # create queue
            res = sqs_client.create_queue(QueueName=queue_name, Attributes={
                'FifoQueue': 'true', 'ContentBasedDeduplication': 'true', 'VisibilityTimeout': str(config['visibility_timeout'])})
            queue_url = res['QueueUrl']

        queue_urls[queue_name] = queue_url

    return queue_urls


def update():
    """Creates non-existing queues (e.g.) after one environment is added.
    """
    return setup(skip_if_exists=True)
