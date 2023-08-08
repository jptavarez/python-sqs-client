import logging
from datetime import datetime
from typing import List
from multiprocessing import Process
from time import sleep

from sqs_client.contracts import IdleQueueSweeper as IdleQueueSweeperBase
from sqs_client.contracts import Publisher, SqsConnection, Subscriber
from sqs_client.message import RequestMessage
from sqs_client.utils import timestamp

TRIGGER_MESSAGE_BODY = "SweepingTrigger"


class IdleQueueSweeper(IdleQueueSweeperBase):
    """
    A class to monitor and clean up idle queues in a Simple Queue Service (SQS) system.

    The IdleQueueSweeper class efficiently handles idle queue monitoring and cleaning in
    distributed systems, making it well-suited for use in a fleet of EC2 instances or any other
    distributed setup.

    Architecture Overview:
    ----------------------
    The IdleQueueSweeper class is designed with a distributed systems approach, enabling it to
    efficiently monitor and clean up idle queues across multiple instances or processes.

    Key Concepts:
    --------------------------------------------
    1. Sweeper Queue and Trigger Process:
       The class implements a sweeper queue and a trigger process to initiate the sweeping process.
       The trigger process runs in the background and periodically sends a trigger message to the
       sweeper queue. This trigger message serves as a signal to initiate the actual sweeping process.

    2. Distributed Sweeping:
       The sweeper process listens to messages from the sweeper queue. When a trigger message is received,
       the sweeper process fetches the list of queues in the SQS system and publishes each queue URL as a
       message to the sweeper queue. This distributed approach ensures that the sweeping work is divided
       among active clients, promoting scalability in distributed systems.

    3. Idle Queue Retention Period:
       The class allows customization of the retention period for idle queues by setting the
       `idle_queue_retention_period` parameter when initializing the `IdleQueueSweeper` object.
       This parameter defines the duration after which a queue is considered idle and eligible for removal.
       The default retention period is set to 600 seconds (10 minutes).

    Class Functionality:
    -------------------
    The `IdleQueueSweeper` class is equipped with two processes to manage the queue sweeping:

    1. Trigger Process:
       The trigger process runs continuously and triggers the sweeping process at regular intervals.
       It checks the current time and initiates the sweeping process if the conditions are met.
       By triggering the process at regular intervals, the `IdleQueueSweeper` class ensures consistent
       monitoring and cleaning of idle queues in a distributed setup.

    2. Sweeper Process:
       The sweeper process acts as a worker that listens to messages from the sweeper queue.
       Upon receiving a trigger message, the sweeper process fetches the list of queues in the SQS system
       and sends each queue URL as a message to the sweeper queue. This distribution of tasks ensures that
       multiple instances or processes collaborate to clean up idle queues effectively.

    Sweeping Process:
    -----------------
    The sweeping process involves checking if each queue URL received from the sweeper queue is idle.
    If the queue is found to be idle (based on the specified retention period), it is deleted from
    the SQS system.

    Note:
    -----
    The `IdleQueueSweeper` class efficiently manages idle queue monitoring and cleaning in distributed
    systems by utilizing a combination of a sweeper queue, trigger process, and distributed sweeping.
    It is optimized for scenarios where multiple instances or processes need to collaboratively monitor and
    clean up idle queues in an SQS system within a distributed environment.
    """
    def __init__(
        self,
        sqs_connection: SqsConnection,
        subscriber: Subscriber,
        publisher: Publisher,
        list_queues_max_results: int = 1000,
        idle_queue_retention_period: int = 600,
        request_message_class=RequestMessage,
    ):
        """
        Initialize the IdleQueueSweeper.

        Args:
            sqs_connection (SqsConnection): The SQS connection object.
            subscriber (Subscriber): The subscriber object for receiving messages.
            publisher (Publisher): The publisher object for sending messages.
            list_queues_max_results (int, optional): Maximum number of queues to list at once. Defaults to 1000.
            idle_queue_retention_period (int, optional): Idle queue retention period in seconds. Defaults to 600.
            request_message_class (class, optional): The class for constructing request messages. Defaults to RequestMessage.
        """
        self._connection = sqs_connection
        self._subscriber = subscriber
        self._publisher = publisher
        self._list_queues_max_results = list_queues_max_results
        self._idle_queue_retention_period = idle_queue_retention_period
        self._request_message_class = request_message_class
        self._logger = logging.getLogger()

    def set_name(self, name):
        """
        Set the name of the sweeper queue.

        Args:
            name (str): The name of the sweeper queue.
        """
        self._name = name

    def get_queue_name(self) -> str:
        """
        Get the name of the sweeper queue.

        Returns:
            str: The name of the sweeper queue.
        """
        return self._name + "sweeper.fifo"

    def start(self):
        """Start the idle queue sweeper process."""
        self._create_queue()
        self._start_trigger_process()
        self._start_sweeper_process()

    def stop(self):
        """Stop the idle queue sweeper process."""
        self._trigger_process.terminate()
        self._trigger_process.join()
        self._sweeper_process.terminate()
        self._sweeper_process.join()

    def _start_trigger_process(self):
        """Start the trigger process."""
        self._trigger_process = Process(target=self._trigger_sweeper)
        self._trigger_process.daemon = True
        self._trigger_process.start()

    def _start_sweeper_process(self):
        """Start the sweeper process."""
        self._sweeper_process = Process(target=self._start_sweeper)
        self._sweeper_process.daemon = True
        self._sweeper_process.start()

    def _trigger_sweeper(self):
        """Periodically trigger the idle queue sweeper."""
        minutes = list(range(0, 60, 2))
        while True:
            now = datetime.now()
            if now.minute in minutes and now.second == 0:
                self._sweeper()
            sleep(1)
            
    def _sweeper(self):
        """Send a trigger message to initiate the sweeping process."""
        try:
            self._logger.info("Triggering Idle Queue Sweeper at " + str(datetime.now()))
            message = self._request_message_class(
                body=TRIGGER_MESSAGE_BODY,
                queue_url=self._queue_url,
                group_id=TRIGGER_MESSAGE_BODY,
            )
            self._publisher.send_message(message)
        except Exception as e:
            self._logger.exception(e)

    def _create_queue(self):
        """Create the sweeper queue."""
        try:
            self._logger.info("Creating Idle Queue Sweeper Queue")
            self._queue_url = self._connection.resource.create_queue(
                QueueName=self.get_queue_name(),
                Attributes={
                    "FifoQueue": "true",
                    "ContentBasedDeduplication": "true",
                    "ReceiveMessageWaitTimeSeconds": "20",  # long polling
                },
            ).url
        except Exception as e:
            error = e.__class__.__name__
            if error != "QueueNameExists":
                raise e
            self._queue_url = self._connection.client.get_queue_url(
                QueueName=self.get_queue_name()
            )

    def _start_sweeper(self):
        """Start the sweeping process."""
        self._subscriber.set_queue(self._queue_url)
        for messages in self._subscriber.receive_messages():
            for message in messages:
                try:
                    if message.body == TRIGGER_MESSAGE_BODY:
                        self._publish_queues()
                    else:
                        self._sweep_idle_queue(message.body)
                except Exception as e:
                    self._logger.exception(e)
            messages.delete()

    def _publish_queues(self):
        """Publish the list of queues to check for idleness."""
        self._logger.info("Publishing Queues in order to check for idleness.")
        next_token = None
        while True:
            response = self._list_queues(next_token)
            for queue_url in response["QueueUrls"]:
                self._publish_queue(queue_url)

            next_token = response.get("NextToken")
            if not next_token:
                break

    def _publish_queue(self, queue_url: str):
        """
        Publish a single queue URL to the sweeper queue.

        Args:
            queue_url (str): The URL of the queue to publish.
        """
        if queue_url == self._queue_url:
            return
        message = self._request_message_class(
            body=queue_url, queue_url=self._queue_url, group_id=queue_url
        )
        self._publisher.send_message(message)

    def _sweep_idle_queue(self, queue_url: str):
        """
        Sweep an idle queue.

        Args:
            queue_url (str): The URL of the queue to sweep.
        """
        self._logger.info("Checking for idleness: " + queue_url)
        if self._is_queue_idle(queue_url):
            self._logger.info("Deleting idle queue: " + queue_url)
            self._connection.client.delete_queue(QueueUrl=queue_url)

    def _list_queues(self, next_token: str ="") -> List[dict]:
        """
        List the queues with a specific prefix.

        Args:
            next_token (str, optional): The token for pagination. Defaults to "".

        Returns:
            dict: The response containing the list of queue URLs and the next token for pagination.
        """
        params = {
            "QueueNamePrefix": self._name,
            "MaxResults": self._list_queues_max_results,
        }
        if next_token:
            params["NextToken"] = next_token
        return self._connection.client.list_queues(**params)

    def _is_queue_idle(self, queue_url) -> bool:
        """
        Check if a queue is idle.

        Args:
            queue_url (str): The URL of the queue to check.

        Returns:
            bool: True if the queue is idle, False otherwise.
        """
        tags = self._connection.client.list_queue_tags(QueueUrl=queue_url)["Tags"]

        last_heartbeat = datetime.fromtimestamp(int(tags["heartbeat"]))
        now = datetime.fromtimestamp(timestamp())
        elapsed_time = (now - last_heartbeat).total_seconds()
        return elapsed_time > self._idle_queue_retention_period

    def _is_queue_empty(self, queue_url) -> bool:
        """
        Check if a queue is empty.

        Args:
            queue_url (str): The URL of the queue to check.

        Returns:
            bool: True if the queue is empty, False otherwise.
        """
        response = self._connection.client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return not int(response["Attributes"]["ApproximateNumberOfMessages"])
