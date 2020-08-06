import random 
import threading 
from time import time

from sqs_client.exceptions import ReplyTimeout
from sqs_client.contracts import (
    SqsConnection,
    Subscriber,
    ReplyQueue as ReplyQueueBase,
    Message
)

class ReplyQueue(ReplyQueueBase):

    def __init__(self, 
        sqs_connection: SqsConnection,
        name: str, 
        subscriber: Subscriber, 
        seconds_before_cleaning: int=20,
        num_messages_before_cleaning: int=200
    ):
        self._id = None
        self._queue = None
        self._name = name
        self._connection = sqs_connection
        self._subscriber = subscriber
        self._seconds_before_cleaning = seconds_before_cleaning
        self._num_messages_before_cleaning = num_messages_before_cleaning
        self._sub_thread = None 
        self._cleaner_thread = None
        self._messages = {}

    def get_url(self):
        if not self._queue:
            self._build_queue() 
        return self._queue.url 
    
    def get_response_by_id(self, message_id: str, timeout: int=5) -> Message:
        start = time()
        while True: 
            message = self._messages.get(message_id)
            if not message:
                if (time() - start) > timeout:
                    raise ReplyTimeout
                continue                 
            return message
    
    def _build_queue(self):
        self._id = str(random.getrandbits(128))
        self._queue = self._connection.resource.create_queue(QueueName=self._name + self._id)
        self._start_sub_thread()
    
    def remove_queue(self):
        if self._queue:
            self._connection.client.delete_queue(QueueUrl=self._queue.url)
            self._queue = None
    
    def _start_sub_thread(self):
        self._sub_thread = threading.Thread(target=self._subscribe)
        self._sub_thread.start()
    
    def _subscribe(self):
        try:
            self._receive_messages()
        except Exception as e:
            # TODO: fix it..
            error = e.__class__.__name__
            if error != 'QueueDoesNotExist' and self._queue:
                raise e 
    
    def _receive_messages(self):
        self._subscriber.set_queue(self._queue.url)
        while True:
            qty_messages = 0
            for messages in self._subscriber.receive_messages(message_attribute_names=['RequestMessageId']):
                qty_messages += len(messages)
                for message in messages:
                    self._messages[message.request_id] = message
                messages.delete()
                if qty_messages >= self._num_messages_before_cleaning:
                    break 
            self._clean_old_messages()
    
    def _clean_old_messages(self):
        messages_to_delete = []
        for message in self._messages.values():
            current_time = time()
            diff = current_time - message.initial_time 
            if diff > self._seconds_before_cleaning:
                messages_to_delete.append(message.request_id)
        for request_id in messages_to_delete:
            del self._messages[request_id]
