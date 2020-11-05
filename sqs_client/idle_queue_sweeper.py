import logging
from multiprocessing import Process
from time import time, sleep
from hashlib import sha1

from sqs_client.message import RequestMessage
from sqs_client.utils import timestamp
from sqs_client.contracts import (
    Publisher,
    Subscriber,
    SqsConnection,
    IdleQueueSweeper as IdleQueueSweeperBase
)

TRIGGER_MESSAGE_BODY = "SweepingTrigger"

class IdleQueueSweeper(IdleQueueSweeperBase):

    def __init__(self, 
        sqs_connection: SqsConnection, 
        subscriber: Subscriber, 
        publisher: Publisher,
        list_queues_max_results: int=1000, 
        idle_queue_retention_period: int=600,
        request_message_class=RequestMessage
    ):
        self._connection = sqs_connection
        self._subscriber = subscriber
        self._publisher = publisher
        self._list_queues_max_results = list_queues_max_results
        self._idle_queue_retention_period = idle_queue_retention_period
        self._request_message_class = request_message_class
        self._logger = logging.getLogger()
    
    def set_name(self, name):
        self._name = name
    
    def get_queue_name(self):
        return self._name + 'sweeper.fifo'

    def start(self):
        self._create_queue()
        self._start_trigger_process()
        self._start_sweeper_process()
    
    def stop(self):
        self._trigger_process.terminate()
        self._trigger_process.join()
        self._sweeper_process.terminate()
        self._sweeper_process.join()
    
    def _start_trigger_process(self):
        self._trigger_process = Process(
            target=self._trigger_sweeper
        )
        self._trigger_process.daemon = True
        self._trigger_process.start()
    
    def _start_sweeper_process(self):
        self._sweeper_process = Process(
            target=self._start_sweeper
        )
        self._sweeper_process.daemon = True
        self._sweeper_process.start()
    
    def _trigger_sweeper(self):
        while True:
            sleep(self._idle_queue_retention_period)
            try:
                self._logger.info("Triggering Idle Queue Sweeper")
                message = self._request_message_class(
                    body=TRIGGER_MESSAGE_BODY,
                    queue_url=self._queue_url,
                    group_id=TRIGGER_MESSAGE_BODY
                )
                self._publisher.send_message(message)
            except Exception as e:
                self._logger.exception(e)
    
    def _create_queue(self):
        try:
            self._logger.info('Creating Idle Queue Sweeper Queue')
            self._queue_url = self._connection.resource.create_queue(
                QueueName=self.get_queue_name(),
                Attributes={
                    'FifoQueue': 'true',
                    'ContentBasedDeduplication': 'true',
                    'ReceiveMessageWaitTimeSeconds': '20' # long polling
                }
            ).url
        except Exception as e:
            error = e.__class__.__name__
            if error != 'QueueNameExists':
                raise e 
            self._queue_url = self._connection.client.get_queue_url(
                QueueName=self.get_queue_name()
            )
        
    def _start_sweeper(self):
        self._subscriber.set_queue(self._queue_url)
        for messages in self._subscriber.receive_messages():
            for message in messages:
                if message.body == TRIGGER_MESSAGE_BODY:
                    self._publish_queues()
                else:
                    self._sweep_idle_queue(message.body)        
            messages.delete()
    
    def _publish_queues(self):
        self._logger.info("Publishing Queues in order to check for idleness.")
        next_token = None
        while True:
            response = self._list_queues(next_token)
            for queue_url in response['QueueUrls']:
                self._publish_queue(queue_url)
            
            next_token = response.get('NextToken')
            if not next_token:
                break
    
    def _publish_queue(self, queue_url):
        if queue_url == self._queue_url:
            return
        message = self._request_message_class(
            body=queue_url,
            queue_url=self._queue_url,
            group_id=queue_url
        )
        self._publisher.send_message(message)
    
    def _sweep_idle_queue(self, queue_url):
        self._logger.info("Checking for idleness: " + queue_url)
        if self._is_queue_idle(queue_url) and self._is_queue_empty(queue_url):
            self._logger.info("Deleting idle queue...")
            self._connection.client.delete_queue(QueueUrl=queue_url)
        
    def _list_queues(self, next_token=''):
        params = {
            'QueueNamePrefix': self._name,
            'MaxResults': self._list_queues_max_results
        }
        if next_token:
            params['NextToken'] = next_token
        return self._connection.client.list_queues(**params)
    
    def _is_queue_idle(self, queue_url):
        tags = self._connection.client.list_queue_tags(
            QueueUrl=queue_url
        )['Tags']
        last_heartbeat = int(tags['heartbeat'])
        return (timestamp() - last_heartbeat) > self._idle_queue_retention_period
    
    def _is_queue_empty(self, queue_url):
        response = self._connection.client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages'
            ]
        )
        return not int(response['Attributes']['ApproximateNumberOfMessages'])
         