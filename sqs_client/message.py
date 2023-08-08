import random
from time import time

from sqs_client.contracts import (
    Message as MessageBase,
    MessageList as MessageListBase,
    ReplyQueue as ReplyQueue,
    RequestMessage as RequestMessageBase
)


class RequestMessage(RequestMessageBase):
    def __init__(
        self,
        body: str,
        queue_url: str,
        group_id: str = None,
        delay_seconds: int = 0,
        reply_queue: ReplyQueue = None,
        message_attributes: dict = {},
    ):
        self._request_id = str(random.getrandbits(128))
        self._body = body
        self._group_id = group_id
        self._delay_seconds = delay_seconds
        self._message_attributes = message_attributes
        self.queue_url = queue_url
        self._reply_queue = reply_queue

    def get_params(self) -> dict:
        params = {
            "MessageBody": self._body,
            "DelaySeconds": self._delay_seconds,
            "MessageAttributes": self._message_attributes,
        }
        if self._group_id:
            params["MessageGroupId"] = self._group_id

        if self._reply_queue:
            params["MessageAttributes"]["ReplyTo"] = {
                "StringValue": self._reply_queue.get_url(),
                "DataType": "String",
            }
            params["MessageAttributes"]["RequestMessageId"] = {
                "StringValue": self._request_id,
                "DataType": "String",
            }
        return params

    def get_response(self, timeout=10) -> MessageBase:
        return self._reply_queue.get_response_by_id(self._request_id, timeout)


class Message(MessageBase):
    def __init__(self, message: dict):
        self.initial_time = time()
        self._message = message

    @property
    def body(self) -> str:
        return self._message["Body"]

    @property
    def id(self) -> str:
        return self._message["MessageId"]

    @property
    def request_id(self) -> str:
        try:
            return self.attributes["RequestMessageId"]["StringValue"]
        except KeyError:
            return None

    @property
    def reply_queue_url(self) -> str:
        try:
            return self.attributes["ReplyTo"]["StringValue"]
        except KeyError:
            return None

    @property
    def attributes(self) -> dict:
        return self._message["MessageAttributes"]


class MessageList(MessageListBase):
    def __init__(self, client, queue, messages):
        self.client = client
        self.queue = queue
        self.messages = messages
        self.read_messages = []

    def __len__(self):
        return len(self.messages["Messages"])

    def __iter__(self):
        return self._fetch_one()

    def __add__(self, other_list):
        self.messages["Messages"] += other_list.messages["Messages"]
        return self

    def _fetch_one(self):
        messages_id = []
        for message in self.messages["Messages"]:
            if message["MessageId"] in messages_id:
                continue
            messages_id.append(message["MessageId"])
            self.read_messages.append(
                {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
            )
            yield Message(message)

    def remove(self, message_id):
        """
        Removes a message from the object by id.
        """
        self.read_messages = list(
            filter(lambda m: m["Id"] != message_id, self.read_messages)
        )
        self.messages["Messages"] = list(
            filter(lambda m: m["MessageId"] != message_id, self.messages["Messages"])
        )

    def delete(self):
        """
        Deletes messages from the queue.
        It only deletes messages that were returned by the method _fetch_one
        """
        # SQS only accepts up to ten messages per request
        for entries in self._delete_chunks():
            self.client.delete_message_batch(QueueUrl=self.queue, Entries=entries)

    def _delete_chunks(self):
        n = 10
        for i in range(0, len(self.read_messages), n):
            yield self.read_messages[i : i + n]
