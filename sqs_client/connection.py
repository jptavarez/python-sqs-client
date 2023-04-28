import boto3

from sqs_client.contracts import SqsConnection as SqsConnectionBase


class SqsConnection(SqsConnectionBase):
    def __init__(
        self,
        region_name: str = None,
        access_key: str = None,
        secret_key: str = None,
        endpoint_url: str = None,
    ):
        self._access_key = access_key
        self._secret_key = secret_key
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._queue_url = None
        self._load_resource()
        self._load_client()

    def set_queue(self, queue_url: str):
        self._queue_url = queue_url

    def get_queue_resource(self, queue_url: str = None):
        self._set_queue(queue_url)
        return self.resource.Queue(self._queue_url)

    def _set_queue(self, queue_url: str = None):
        self._queue_url = queue_url if queue_url else self._queue_url
        if not self._queue_url:
            raise Exception("Queue is not defined.")

    def _load_resource(self):
        session = boto3.Session(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )
        self.resource = session.resource(
            "sqs", endpoint_url=self._endpoint_url, region_name=self._region_name
        )

    def _load_client(self):
        self.client = boto3.client(
            "sqs",
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            endpoint_url=self._endpoint_url,
            region_name=self._region_name,
        )
