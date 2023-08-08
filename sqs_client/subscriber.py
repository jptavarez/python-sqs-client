from logging import exception
from time import time
from typing import List, Optional

from sqs_client.contracts import MessageHandler
from sqs_client.contracts import MessagePoller as MessagePollerBase
from sqs_client.contracts import Publisher, SqsConnection
from sqs_client.contracts import Subscriber as SubscriberBase
from sqs_client.message import Message, MessageList, RequestMessage


class Subscriber(SubscriberBase):
    """
    A class responsible for receiving messages from an Amazon SQS queue.

    The Subscriber class provides methods to receive and process messages from the specified queue.
    """
    def __init__(
        self,
        sqs_connection: SqsConnection,
        queue_url: Optional[str]=None,
        max_number_of_messages: int=10,
        visibility_timeout: int=30,
    ):
        """
        Initialize a Subscriber instance.

        Args:
            sqs_connection (SqsConnection): An instance of the SqsConnection class.
            queue_url (str, optional): The URL of the queue to receive messages from. Defaults to None.
            max_number_of_messages (int, optional): The maximum number of messages to receive in one batch. Defaults to 10.
            visibility_timeout (int, optional): The visibility timeout for received messages in seconds. Defaults to 30.
        """
        self._connection = sqs_connection
        self._queue_url = queue_url
        self._max_number_of_messages = max_number_of_messages
        self._visibility_timeout = visibility_timeout

    def set_queue(self, queue_url: str):
        """
        Set the queue URL for the current instance.

        Args:
            queue_url (str): The URL of the queue to receive messages from.
        """
        self._queue_url = queue_url

    def receive_messages(self, return_none: bool=False, message_attribute_names: List[str]=[]):
        """
        Receive and yield messages from the queue.

        Args:
            return_none (bool, optional): Whether to yield None when no messages are available. Defaults to False.
            message_attribute_names (list, optional): List of message attribute names to retrieve. Defaults to an empty list.

        Yields:
            MessageList or None: Yields a MessageList instance containing received messages or None if return_none is True.
        """
        while True:
            messages = self._connection.client.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=self._max_number_of_messages,
                MessageAttributeNames=message_attribute_names,
                VisibilityTimeout=self._visibility_timeout,
                WaitTimeSeconds=20,
            )
            if "Messages" in messages:
                yield MessageList(self._connection.client, self._queue_url, messages)
            elif return_none:
                yield None

    def chunk(self, num_messages: int=500, limit_seconds: int=30):
        """
        Generator that yields chunks of received messages based on specified conditions.

        Args:
            num_messages (int, optional): The target number of messages per chunk. Defaults to 500.
            limit_seconds (int, optional): The time limit in seconds for each chunk. Defaults to 30.

        Yields:
            MessageList: Yields a MessageList instance containing received messages based on conditions.
        
        Usage:
            sqs_config = ....
            subscriber = Subscriber(sqs_config)
            for messages in subscriber.chunk(num_messages=50, limit_seconds=8):
                for message in messages:
                    print(message.body)
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
            if messages_received and (
                num >= num_messages or (now - start) >= limit_seconds
            ):
                yield messages_received
                messages_received = None
                num = 0
                start = now


class MessagePoller(MessagePollerBase):
    """
    A class responsible for polling messages from a Subscriber, processing them using a MessageHandler,
    and sending responses using a Publisher.

    The MessagePoller class continuously polls messages from a Subscriber, processes them using a provided
    MessageHandler, and sends responses using a Publisher to the appropriate reply queue.
    """
    def __init__(
        self,
        handler: MessageHandler,
        subscriber: Subscriber,
        publisher: Publisher,
        request_message_class=RequestMessage,
    ):
        """
        Initialize a MessagePoller instance.

        Args:
            handler (MessageHandler): An instance of the MessageHandler class for message processing.
            subscriber (Subscriber): An instance of the Subscriber class for receiving messages.
            publisher (Publisher): An instance of the Publisher class for sending responses.
            request_message_class (type, optional): The class used to create request messages. Defaults to RequestMessage.
        """
        self._subscriber = subscriber
        self._publisher = publisher
        self._request_message_class = request_message_class
        self._handler = handler

    def start(self):
        """
        Start the message polling and processing loop.

        This method continuously polls messages from the Subscriber, processes each message using the provided
        MessageHandler, and sends responses back using the Publisher.
        """
        for messages in self._subscriber.receive_messages(
            message_attribute_names=["RequestMessageId", "ReplyTo"]
        ):
            for message in messages:
                try:
                    response = self._handler.process_message(message)
                    self._send_response(message, response)
                except Exception as e:
                    exception("Error while trying to process a message")
                    messages.remove(message.id)
            messages.delete()

    def _send_response(self, message: Message, response: Optional[str]=None):
        """
        Send a response to the appropriate reply queue.

        Args:
            message (Message): The original received message.
            response (str, optional): The response body to be sent. Defaults to None.
        """
        if not response:
            return
        reply_queue_url = message.reply_queue_url
        if not reply_queue_url:
            return
        try:
            response_message = self._request_message_class(
                body=response,
                queue_url=reply_queue_url,
                message_attributes={
                    "RequestMessageId": message.attributes["RequestMessageId"]
                },
            )
            self._publisher.send_message(response_message)
        except Exception as e:
            exception(e)
            return
