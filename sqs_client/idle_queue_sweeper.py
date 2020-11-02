from multiprocessing import Process
from time import time


class IdleQueueSweeper:

    def __init__(self, 
        sqs_connection, 
        subscriber, 
        max_results=100, 
        idle_queue_retention_period=1200
    ):
        self._connection = sqs_connection
        self._subscriber = subscriber
        self._max_results = max_results
        self._idle_queue_retention_period = idle_queue_retention_period

    def set_name(self, name):
        self._name = name
    
    def start(self):
        self._create_queue()
        # self._start_sweeper()
    
    def _create_queue(self):
        try:
            self._queue_url = self._connection.resource.create_queue(
                QueueName=self._name + '_sweeper',
                Attributes={
                    'FifoQueue': True,
                    'ContentBasedDeduplication': True
                }
            ).url
        except Exception as e:
            error = e.__class__.__name__
            if error != 'QueueNameExists':
                raise e 
            self._queue_url = self._connection.resource.get_queue_url(
                QueueName=self._name
            )
        
    def _start_sweeper(self):
        self._subscriber.set_queue(self._queue_url)
        for messages in self._subscriber.receive_messages():
            for message in messages:
                # messages.delete() does not remove from queue messages that were not read.
                pass
            self._sweep_idle_queues()            
            messages.delete()
    
    def _sweep_idle_queues(self):
        next_token = None
        while True:
            response = self._list_queues(next_token)
            for queue_url in response['QueueUrls']:
                self._sweep_idle_queue(queue_url)
            
            next_token = response['NextToken']
            if not next_token:
                break
    
    def _sweep_idle_queue(self, queue_url):
        if self._is_queue_idle(queue_url) and not self._is_queue_empty(queue_url):
            self._connection.resource.delete_queue(QueueUrl=queue_url)
        
    def _list_queues(self, next_token=None):
        params = {
            'QueueNamePrefix': self._name,
            'MaxResults': self._max_results
        }
        if next_token:
            params['NextToken'] = next_token
        return self._connection.client.list_queues(**params)
    
    def _is_queue_idle(self, queue_url):
        tags = self._connection.client.list_queue_tags(
            QueueUrl=queue_url
        )['Tags']
        last_heartbeat = int(tags['heartbeat'])
        return (time() - last_heartbeat) > self._idle_queue_retention_period
    
    def _is_queue_empty(self, queue_url):
        response = self._connection.client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages'
            ]
        )
        return int(response['Attributes']['ApproximateNumberOfMessages']) > 0
         