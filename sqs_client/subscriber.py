from time import time
from logging import exception

from sqs_client.message import RequestMessage, MessageList
from sqs_client.contracts import (
    MessageHandler, 
    SqsConnection,
    Publisher,
    Subscriber as SubscriberBase,
    MessagePoller as MessagePollerBase
)

class Subscriber(SubscriberBase):

    def __init__(self, sqs_connection: SqsConnection, queue_url=None, max_number_of_messages=10):
        self._connection = sqs_connection
        self._queue_url = queue_url
        self._max_number_of_messages = max_number_of_messages
    
    def set_queue(self, queue_url):
        self._queue_url = queue_url

    def receive_messages(self, return_none=False, message_attribute_names=[]):
        while True:
            messages = self._connection.client.receive_message(
                QueueUrl=self._queue_url, 
                MaxNumberOfMessages=self._max_number_of_messages,
                MessageAttributeNames=message_attribute_names
            )
            if 'Messages' in messages:
                yield MessageList(self._connection.client, self._queue_url, messages)
            elif return_none:
                yield None     

    def chunk(self, num_messages=500, limit_seconds=30):
        """
            Ex.:
                sqs_config = ....
                subscriber = Subscriber(sqs_config)
                for messages in subscriber.chunk(num_messages=50, limit_seconds=8):
                    for message in messages:
                        print(message['Body'])
                    messages.delete()
        """
        if num_messages < 10:
            self.max_number_of_messages = num_messages
        messages_received = None
        num = 0
        start = time()
        for message_list in self.receive_messages(return_none=True):
            if not messages_received:
                messages_received = message_list
            elif message_list:
                messages_received += message_list
            
            num += 0 if not message_list else len(message_list)
            now = time()
            if messages_received and (num >= num_messages or (now - start) >= limit_seconds):
                yield messages_received
                messages_received = None
                num = 0
                start = now

class MessagePoller(MessagePollerBase):

    def __init__(self, 
        handler: MessageHandler, 
        subscriber: Subscriber, 
        publisher: Publisher, 
        request_message_class=RequestMessage
    ):
        self._subscriber = subscriber 
        self._publisher = publisher
        self._request_message_class = request_message_class
        self._handler = handler

    def start(self):
        for messages in self._subscriber.receive_messages(message_attribute_names=['RequestMessageId', 'ReplyTo']):
            for message in messages:
                try:
                    response = self._handler.process_message(message)
                    self._send_response(message, response)
                except Exception as e:
                    exception('Error while trying to process a message')
                    messages.remove(message.id)  
            messages.delete()
    
    def _send_response(self, message, response=None):
        if not response:
            return 
        reply_queue_url = message.reply_queue_url
        if not reply_queue_url:
            return        
        try:
            response_message = self._request_message_class(
                body=response,
                queue_url=reply_queue_url,
                message_attributes = {
                    'RequestMessageId': message.attributes['RequestMessageId']
                }
            )
            self._publisher.send_message(response_message)
        except Exception as e:
            exception(e)
            return
