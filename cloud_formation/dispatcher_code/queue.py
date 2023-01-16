import boto3
import botocore
import datetime
import json
import os
import pickle
from abc import ABC, abstractmethod
from dict_hash import sha256
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from typing import Tuple

from ..models import Job


class JobQueue(ABC):
    """Abstract job queue.
    """

    @abstractmethod
    def exists(self):
        """Check if the queue already exists.
        """
        pass

    @abstractmethod
    def create(self):
        """Create the initialized queue (raises error if queue already exists).
        """
        pass

    @abstractmethod
    def delete(self):
        """Delete the queue.
        """
        pass

    @abstractmethod
    def enqueue(self, job: Job):
        """Push a new job to the queue.
        """
        pass

    @abstractmethod
    def peek(self) -> Tuple[dict, str | None] | None:
        """Look at first element in the queue.

        Returns:
         - job (as a dict)
         - receipt handle (to provide to the pop method to delete the job from the queue)
        """
        pass

    @abstractmethod
    def pop(self, receipt_handle: str | None) -> None:
        """Delete job from the queue.
        """
        pass

    def dequeue(self, older_than: float=0) -> Job | None:
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


class LocalJobQueue(JobQueue):
    """Local job queue, for testing purposes only.
    """

    def __init__(self, queue_path: str):
        self.queue_path = queue_path

    def exists(self):
        return os.path.exists(self.queue_path)

    def create(self):
        if os.path.exists(self.queue_path):
            raise ValueError("A queue at this path already exists.")
        queue = list()
        with open(self.queue_path, 'wb') as f:
            pickle.dump(queue, f)

    def delete(self):
        try:
            os.remove(self.queue_path)
        except:
            pass

    def enqueue(self, job: Job):
        with open(self.queue_path, 'rb+') as f:
            # Read in
            queue = pickle.load(f)
            # Push new job
            queue.append(jsonable_encoder(job))
            # Overwrite
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)
    
    def peek(self) -> Tuple[dict | None, str | None]:
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
        if len(queue):
            job_json = queue[0]
            return job_json, sha256(job_json)
        return None, None

    def pop(self, receipt_handle):
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
            if sha256(queue[0]) != receipt_handle:
                raise HTTPException(
                    status_code=400,
                    detail=f"Element not first in queue.")
            queue = queue[1:]
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)


#TODO: queue will need to have a high enough visibility timeout
#TODO: AWS lambda will need to pull infrequently enough that local
# has the time to pull
class SQSJobQueue(JobQueue):
    """SQS job queue.
    """

    def __init__(self, queue_name: str):
        if not queue_name.endswith('.fifo'):
            queue_name = queue_name + '.fifo'
        self.queue_name = queue_name
        self.sqs_client = boto3.client("sqs")
        try:
            self.queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)
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

    def peek(self) -> Tuple[dict, str | None]:
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

    def pop(self, receipt_handle: str | None):
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


def get_queue(name: str, create_if_not_exists: bool=False) -> JobQueue:
    """Gets JobQueue from name.
    
    The queue can be either on aws (sqs), if `name' is prefixed by 'aws:',
    or local (the name must be a local path).
    """
    if name.startswith('aws:'):
        queue = SQSJobQueue(name[4:])
    else:
        queue = LocalJobQueue(name)
    if not queue.exists():
        if create_if_not_exists:
            queue.create()
        else:
            raise ValueError(f"Queue {name} does not exist.")
    return queue
