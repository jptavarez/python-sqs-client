from time import sleep

from boto3.resources.base import ServiceResource

from sqs_client.contracts import Publisher as PublisherBase
from sqs_client.contracts import RequestMessage, SqsConnection


class Publisher(PublisherBase):
    """
    A class responsible for sending messages to an Amazon SQS queue.

    The Publisher class provides methods to send messages using a specified request message
    and handles the interaction with the SQS queue.
    """

    def __init__(self, sqs_connection: SqsConnection, queue_url=None):
        """
        Initialize a Publisher instance.

        Args:
            sqs_connection (SqsConnection): An instance of the SqsConnection class.
            queue_url (str, optional): The URL of the queue to which messages will be sent. Defaults to None.
        """
        self._connection = sqs_connection
        self._queue_url = queue_url

        if queue_url:
            self._set_queue(queue_url)
            self._queue = self._get_queue()
        else:
            self._queue = None

    def send_message(self, request_message: RequestMessage) -> dict:
        """
        Send a message using the specified request message.

        Args:
            request_message (RequestMessage): The message to be sent.

        Returns:
            dict: The response from the send_message API call.
        """
        params = request_message.get_params()

        if self._queue:
            return self._queue.send_message(**params)
        else:
            self._set_queue(request_message.queue_url)
            return self._get_queue().send_message(**params)

    def _get_queue(self) -> ServiceResource:
        """
        Retrieve the queue resource associated with the current queue URL.

        Returns:
            boto3.resources.base.ServiceResource: The SQS queue resource.
        """
        return self._connection.get_queue_resource()

    def _set_queue(self, queue_url):
        """
        Set the queue URL for the current instance.

        Args:
            queue_url (str): The URL of the queue to be set.
        """
        self._connection.set_queue(queue_url)


class RetryPublisher(PublisherBase):
    """
    A class that implements a retry mechanism for publishing messages using a base Publisher.

    The RetryPublisher class attempts to publish messages using a provided Publisher instance and retries
    the operation a certain number of times. If the operation fails, it can optionally fallback to an outbox
    repository to store the message for later processing.
    """
    def __init__(
        self, publisher: Publisher, retries: int=3, outbox_repository=None, queue_url: str=None
    ):
        """
        Initialize a RetryPublisher instance.

        Args:
            publisher (Publisher): An instance of the Publisher class for message publishing.
            retries (int, optional): The number of retries for message publishing. Defaults to 3.
            outbox_repository (Any, optional): An outbox repository to store messages. Defaults to None.
            queue_url (str, optional): The URL of the queue to which messages will be sent. Defaults to None.
        """
        self._queue_url = queue_url
        self._publisher = publisher
        self._outbox_repository = outbox_repository
        self._retries = retries

    def send_message(self, request_message: RequestMessage):
        """
        Send a message using the provided request message with retry and fallback mechanism.

        Args:
            request_message (RequestMessage): The message to be sent.

        Raises:
            Exception: If the message cannot be successfully sent after all retries and fallback attempts.
        """
        for _ in range(0, self._retries):
            try:
                self._publish(request_message)
            except Exception as e:
                if self._publish_via_outbox(request_message):
                    return
            finally:
                return
        # TODO: Add specific exception
        raise Exception("Message could not be sent")

    def _publish(self, request_message: RequestMessage):
        """
        Publish a message using the base Publisher with retry logic.

        Args:
            request_message (RequestMessage): The message to be sent.

        Raises:
            Exception: If the message cannot be published after all retries.
        """
        success = False
        for _ in range(3):
            try:
                self._publisher.send_message(request_message)
            except Exception as e:
                print("Error while trying to publish event.. trying again..")
                sleep(0.25)
            else:
                success = True
                break
        if not success:
            # TODO: Add specific exception
            raise Exception("Event not published in queue.")

    def _publish_via_outbox(self, request_message: RequestMessage) -> bool:
        """
        Publish a message via the outbox repository as a fallback mechanism.

        Args:
            request_message (RequestMessage): The message to be sent.

        Returns:
            bool: True if the message was successfully published via the outbox, False otherwise.
        """
        if not self._outbox_repository:
            return False
        try:
            self._outbox_repository.create(request_message)
        except Exception as e:
            return False
        return True
