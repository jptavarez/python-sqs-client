from sqs_client.connection import SqsConnection
from sqs_client.idle_queue_sweeper import IdleQueueSweeper
from sqs_client.publisher import Publisher
from sqs_client.reply_queue import ReplyQueue
from sqs_client.subscriber import Subscriber


class SqsConnectionFactory:
    def __init__(
        self, region_name=None, access_key=None, secret_key=None, endpoint_url=None
    ):
        self._region_name = region_name
        self._access_key = access_key
        self._secret_key = secret_key
        self._endpoint_url = endpoint_url

    def build(self):
        return SqsConnection(
            access_key=self._access_key,
            secret_key=self._secret_key,
            endpoint_url=self._endpoint_url,
            region_name=self._region_name,
        )


class BaseFactory:
    def __init__(
        self,
        region_name=None,
        access_key=None,
        secret_key=None,
        endpoint_url=None,
        sqs_connection_factory=SqsConnectionFactory,
    ):
        self._region_name = region_name
        self._access_key = access_key
        self._secret_key = secret_key
        self._endpoint_url = endpoint_url
        self._sqs_connection_factory = sqs_connection_factory

    def build(self):
        raise NotImplementedError

    def _build_sqs_connection(self):
        return self._sqs_connection_factory(
            self._region_name, self._access_key, self._secret_key, self._endpoint_url
        ).build()


class SubscriberFactory(BaseFactory):
    def __init__(self, *args, queue_url=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue_url = queue_url

    def build(self, max_number_of_messages=10, visibility_timeout=30):
        return Subscriber(
            sqs_connection=self._build_sqs_connection(),
            queue_url=self._queue_url,
            max_number_of_messages=max_number_of_messages,
            visibility_timeout=visibility_timeout,
        )


class PublisherFactory(BaseFactory):
    def __init__(self, *args, queue_url=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue_url = queue_url

    def build(self):
        return Publisher(
            sqs_connection=self._build_sqs_connection(), queue_url=self._queue_url
        )


class ReplyQueueFactory(BaseFactory):
    def __init__(
        self,
        *args,
        name="reply_queue_",
        message_retention_period=60,
        seconds_before_cleaning=20,
        num_messages_before_cleaning=200,
        heartbeat_interval_seconds=60,
        list_queues_max_results=1000,
        idle_queue_retention_period=120,
        subscriber_factory=SubscriberFactory,
        publisher_factory=PublisherFactory,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._name = name
        self._message_retention_period = message_retention_period
        self._seconds_before_cleaning = seconds_before_cleaning
        self._num_messages_before_cleaning = num_messages_before_cleaning
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._list_queues_max_results = list_queues_max_results
        self._idle_queue_retention_period = idle_queue_retention_period
        self._subscriber_factory = subscriber_factory
        self._publisher_factory = publisher_factory

    def build(self):
        return ReplyQueue(
            name=self._name,
            sqs_connection=self._build_sqs_connection(),
            subscriber=self._build_subscriber(),
            idle_queue_sweeper=self._build_idle_queue_sweeper(),
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )

    def _build_idle_queue_sweeper(self):
        return IdleQueueSweeper(
            sqs_connection=self._build_sqs_connection(),
            subscriber=self._build_subscriber(),
            publisher=self._build_publisher(),
            list_queues_max_results=self._list_queues_max_results,
            idle_queue_retention_period=self._idle_queue_retention_period,
        )

    def _build_subscriber(self):
        return self._subscriber_factory(
            self._region_name, self._access_key, self._secret_key, self._endpoint_url
        ).build()

    def _build_publisher(self):
        return self._publisher_factory(
            self._region_name, self._access_key, self._secret_key, self._endpoint_url
        ).build()
