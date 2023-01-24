import boto3
import botocore
import datetime
import json
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from typing import Tuple, Union


#TODO: should be imported somehow
class Job:
    def __init__(self, id, job_type, date_created, date_started, date_finished, status, model_id, model, environment, attributes):
        self.id = id
        self.job_type = job_type
        self.date_created = date_created
        self.date_started = date_started
        self.date_finished = date_finished
        self.status = status
        self.model_id = model_id
        self.model = model
        self.environment = environment
        self.attributes = attributes


class JobQueue:

    def dequeue(self, older_than: float=0) -> Union[Job, None]:
        """Peek last element and remove it from the queue if it is older than `older_than'.
        """
        # get last element
        job_json, receipt_handle = self.peek()
        # if element found
        if job_json:
            # get job
            job = Job(**job_json)
            # check "old enough"
            time_now = datetime.datetime.utcnow()
            job_age = time_now - datetime.datetime.fromisoformat(job.date_created)
            if job_age > datetime.timedelta(seconds=older_than):
                # remove from queue and return
                self.pop(receipt_handle)
                return job
        return None


class SQSJobQueue(JobQueue):
    """SQS job queue.
    """

    def __init__(self, queue_name: str):
        if not queue_name.endswith('.fifo'):
            queue_name = queue_name + '.fifo'
        self.queue_name = queue_name
        self.sqs_client = boto3.client("sqs")
        try:
            self.queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        except self.sqs_client.exceptions.QueueDoesNotExist:
            pass

    def exists(self) -> bool:
        try:
            self.sqs_client.get_queue_url(QueueName=self.queue_name)
            return True
        except self.sqs_client.exceptions.QueueDoesNotExist:
            return False

    def create(self):
        res = self.sqs_client.create_queue(QueueName=self.queue_name, Attributes={
            'FifoQueue': 'true', 'ContentBasedDeduplication': 'true', 'VisibilityTimeout': '5'})
        self.queue_url = res['QueueUrl']

    def delete(self):
        self.sqs_client.delete_queue(QueueUrl=self.queue_url)

    def enqueue(self, job: Job):
        try:
            self.sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(jsonable_encoder(job)),
                MessageGroupId="0",
            )
        except botocore.exceptions.ClientError as error:
            print(error)
            raise HTTPException(
                status_code=500,
                detail=f"Error sending message to SQS queue: {error}.")

    def peek(self) -> Tuple[dict, Union[str, None]]:
        try:
            response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=10,
                )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=500,
                detail=f"Error receiving message from SQS queue: {error}.")
        if len(response.get('Messages', [])):
            message = response['Messages'][0]
            message_body = message['Body']
            job_json = json.loads(message_body)
            receipt_handle = message['ReceiptHandle']
            return job_json, receipt_handle
        return None, None

    def pop(self, receipt_handle: Union[str, None]):
        try:
            response = self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
        except botocore.exceptions.ClientError as error:
            print(error)
            raise HTTPException(
                status_code=500,
                detail=f"Error deleting message from SQS queue: {error}.")
        return response
