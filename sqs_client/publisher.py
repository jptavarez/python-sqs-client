from time import sleep

from sqs_client.contracts import Publisher as PublisherBase
from sqs_client.contracts import RequestMessage, SqsConnection


class Publisher(PublisherBase):
    def __init__(self, sqs_connection: SqsConnection, queue_url=None):
        self._connection = sqs_connection
        self._queue_url = queue_url

        if queue_url:
            self._set_queue(queue_url)
            self._queue = self._get_queue()
        else:
            self._queue = None

    def send_message(self, request_message: RequestMessage):
        params = request_message.get_params()

        if self._queue:
            return self._queue.send_message(**params)
        else:
            self._set_queue(request_message.queue_url)
            return self._get_queue().send_message(**params)

    def _get_queue(self):
        return self._connection.get_queue_resource()

    def _set_queue(self, queue_url):
        self._connection.set_queue(queue_url)


class RetryPublisher(PublisherBase):
    def __init__(
        self, publisher: Publisher, retries=3, outbox_repository=None, queue_url=None
    ):
        self._queue_url = queue_url
        self._publisher = publisher
        self._outbox_repository = outbox_repository
        self._retries = retries

    def send_message(self, request_message: RequestMessage):
        for _ in range(0, self._retries):
            try:
                self._publish(request_message)
            except Exception as e:
                if self._publish_via_outbox(request_message):
                    return
            finally:
                return
        raise Exception("Message could not be sent")

    def _publish(self, request_message: RequestMessage):
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
            raise Exception("Event not published in queue.")

    def _publish_via_outbox(self, request_message: RequestMessage) -> bool:
        if not self._outbox_repository:
            return False
        try:
            self._outbox_repository.create(request_message)
        except Exception as e:
            return False
        return True
