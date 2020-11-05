import random 
import sys
import logging
from threading import Thread
from time import time, sleep
from multiprocessing import Process

from multiprocessing_logging import install_mp_handler, uninstall_mp_handler

from sqs_client.exceptions import ReplyTimeout
from sqs_client.utils import str_timestamp
from sqs_client.contracts import (
    SqsConnection,
    Subscriber,
    IdleQueueSweeper,
    ReplyQueue as ReplyQueueBase,
    Message
)

class ReplyQueue(ReplyQueueBase):

    def __init__(self, 
        sqs_connection: SqsConnection,
        name: str, 
        subscriber: Subscriber,
        idle_queue_sweeper: IdleQueueSweeper, 
        message_retention_period: int=60,
        seconds_before_cleaning: int=20,
        num_messages_before_cleaning: int=200,
        heartbeat_interval_seconds=300
    ):
        self._id = None
        self._queue = None
        self._name = name
        self._connection = sqs_connection
        self._subscriber = subscriber
        self._message_retention_period = message_retention_period
        self._seconds_before_cleaning = seconds_before_cleaning
        self._num_messages_before_cleaning = num_messages_before_cleaning
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._idle_queue_sweeper = idle_queue_sweeper
        self._sub_thread = None 
        self._cleaner_thread = None
        self._messages = {}
        self._logger = logging.getLogger()

    def get_url(self):
        if not self._queue:
            self._create_queue() 
        return self._queue.url 
    
    def get_name(self):
        self._id = str(random.getrandbits(128))
        return self._name + self._id
    
    def get_response_by_id(self, message_id: str, timeout: int=5) -> Message:
        start = time()
        while True: 
            message = self._messages.get(message_id)
            if not message:
                if (time() - start) > timeout:
                    raise ReplyTimeout
                continue                 
            return message
    
    def _create_queue(self):        
        self._queue = self._connection.resource.create_queue(
            QueueName=self.get_name(),
            Attributes={
                'MessageRetentionPeriod': str(self._message_retention_period)
            },
            tags={
                'heartbeat': str_timestamp()
            }
        )
        install_mp_handler(self._logger)       
        self._start_sub_thread()
        self._start_heartbeat()
        self._start_idle_queue_sweeper()
    
    def _start_idle_queue_sweeper(self):
        self._idle_queue_sweeper.set_name(self._name)
        self._idle_queue_sweeper.start()
    
    def remove_queue(self):
        if self._queue:
            self._stop_heartbeat()
            self._idle_queue_sweeper.stop()
            self._connection.client.delete_queue(QueueUrl=self._queue.url)
            self._queue = None
            uninstall_mp_handler(self._logger)
    
    def _start_sub_thread(self):
        self._sub_thread = Thread(target=self._subscribe)
        self._sub_thread.daemon = True
        self._sub_thread.start()
    
    def _start_heartbeat(self):
        self._heartbeat_process = Process(
            target=self._heartbeat, 
            args=(self._heartbeat_interval_seconds, self._queue.url)
        )
        self._heartbeat_process.daemon = True
        self._heartbeat_process.start()
    
    def _stop_heartbeat(self):
        self._heartbeat_process.terminate()
        self._heartbeat_process.join()
    
    def _heartbeat(self, heartbeat_interval_seconds, queue_url):
        while True:
            sleep(heartbeat_interval_seconds)
            self._logger.info('Reply Queue Heartbeat')
            self._connection.client.tag_queue(
                QueueUrl=queue_url,
                Tags={
                    'heartbeat': str_timestamp()
                }
            )        
    
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
