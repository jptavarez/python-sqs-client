import logging
from uuid import uuid4
from multiprocessing import Process
from threading import Thread
from time import sleep, time

from multiprocessing_logging import install_mp_handler, uninstall_mp_handler

from sqs_client.contracts import IdleQueueSweeper, Message
from sqs_client.contracts import ReplyQueue as ReplyQueueBase
from sqs_client.contracts import SqsConnection, Subscriber
from sqs_client.exceptions import ReplyTimeout
from sqs_client.utils import str_timestamp


class ReplyQueue(ReplyQueueBase):
    """
    A class representing a reply queue used for receiving responses from a Simple Queue Service (SQS).

    The ReplyQueue class provides functionalities to manage a reply queue, receive and store messages,
    and retrieve responses by message ID.
    """
    def __init__(
        self,
        sqs_connection: SqsConnection,
        name: str,
        subscriber: Subscriber,
        idle_queue_sweeper: IdleQueueSweeper,
        message_retention_period: int = 60,
        seconds_before_cleaning: int = 20,
        num_messages_before_cleaning: int = 200,
        heartbeat_interval_seconds=300,
    ):
        self._id = str(uuid4())
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

    def get_url(self) -> str:
        """
        Returns the URL associated with the queue, 
        after checking if the queue exists or creating a new one if needed.
    
        Returns:
            str: The queue URL.
        """
        if not self._queue:
            self._create_queue()
        return self._queue.url

    def get_name(self) -> str:
        """
        Returns the combined name of the queue and its unique identifier.
        
        Returns:
            str: The combined name.
        """
        return self._name + self._id

    def get_response_by_id(self, message_id: str, timeout: int = 5) -> Message:
        """
        Retrieve a response message by its ID.
        
        Args:
            message_id (str): The ID of the response message.
            timeout (int, optional): Timeout in seconds. Defaults to 5.
        
        Returns:
            Message: The response message.
        
        Raises:
            ReplyTimeout: If the response retrieval times out.
        """
        start = time()
        while True:
            message = self._messages.get(message_id)
            if not message:
                if (time() - start) > timeout:
                    raise ReplyTimeout
                continue
            return message

    def _create_queue(self):
        """
        Create a new queue and initialize various components for its maintenance.

        This method creates a new queue using the SQS connection, sets its attributes such as
        message retention period, and starts necessary threads for managing the queue.
        """
        self._queue = self._connection.resource.create_queue(
            QueueName=self.get_name(),
            Attributes={"MessageRetentionPeriod": str(self._message_retention_period)},
            tags={"heartbeat": str_timestamp()},
        )
        self._logger.info(self._queue.url)
        install_mp_handler(self._logger)
        self._start_heartbeat()
        self._start_idle_queue_sweeper()
        self._start_sub_thread()

    def _start_idle_queue_sweeper(self):
        """
        Start the idle queue sweeper for maintaining the queue's cleanliness.

        This method sets the queue's name for the idle queue sweeper and starts the sweeper thread.
        """
        self._idle_queue_sweeper.set_name(self._name)
        self._idle_queue_sweeper.start()

    def remove_queue(self):
        """
        Remove the queue and associated components.

        This method stops the heartbeat, idle queue sweeper, deletes the queue using the SQS connection,
        and uninstalls the multiprocess handler from the logger.
        """
        if self._queue:
            self._stop_heartbeat()
            self._idle_queue_sweeper.stop()
            self._connection.client.delete_queue(QueueUrl=self._queue.url)
            self._queue = None
            uninstall_mp_handler(self._logger)

    def _start_sub_thread(self):
        """
        Start the subscription thread for receiving messages.

        This method creates and starts a new thread to handle message subscription and processing.
        """
        self._sub_thread = Thread(target=self._subscribe)
        self._sub_thread.daemon = True
        self._sub_thread.start()

    def _start_heartbeat(self):
        """
        Start the heartbeat process for queue health monitoring.

        This method creates and starts a new process to handle sending periodic heartbeat messages.
        """
        self._heartbeat_process = Process(target=self._heartbeat)
        self._heartbeat_process.daemon = True
        self._heartbeat_process.start()

    def _stop_heartbeat(self):
        """
        Stop the heartbeat process.

        This method terminates the heartbeat process and waits for its completion.
        """
        self._heartbeat_process.terminate()
        self._heartbeat_process.join()

    def _heartbeat(self):
        """
        Periodically send heartbeat messages to the queue to monitor its health.

        This method sends heartbeat messages to the queue at regular intervals to indicate that the queue is active.
        """
        self._logger.info(
            "heartbeat_interval_seconds " + str(self._heartbeat_interval_seconds)
        )

        while True:
            self._logger.info("Reply Queue Heartbeat")
            try:
                self._connection.client.tag_queue(
                    QueueUrl=self._queue.url, Tags={"heartbeat": str_timestamp()}
                )
            except Exception as e:
                self._logger.exception(e)
            sleep(self._heartbeat_interval_seconds)

    def _subscribe(self):
        """
        Start receiving messages from the queue.

        This method initiates the process of receiving and processing messages from the queue.
        """
        try:
            self._receive_messages()
        except Exception as e:
            # TODO: fix it..
            error = e.__class__.__name__
            if error != "QueueDoesNotExist" and self._queue:
                raise e

    def _receive_messages(self):
        """
        Receive and process messages from the queue.

        This method sets up the subscriber to the queue and continuously receives messages from the queue,
        processing and storing them in the `_messages` dictionary.
        """
        self._subscriber.set_queue(self._queue.url)
        while True:
            qty_messages = 0
            for messages in self._subscriber.receive_messages(
                message_attribute_names=["RequestMessageId"]
            ):
                qty_messages += len(messages)
                for message in messages:
                    self._messages[message.request_id] = message
                messages.delete()
                if qty_messages >= self._num_messages_before_cleaning:
                    break
            self._clean_old_messages()

    def _clean_old_messages(self):
        """
        Clean up old messages from the `_messages` dictionary.

        This method removes messages from the `_messages` dictionary that have exceeded the configured
        time limit for cleaning.
        """
        messages_to_delete = []
        for message in self._messages.values():
            current_time = time()
            diff = current_time - message.initial_time
            if diff > self._seconds_before_cleaning:
                messages_to_delete.append(message.request_id)
        for request_id in messages_to_delete:
            del self._messages[request_id]
