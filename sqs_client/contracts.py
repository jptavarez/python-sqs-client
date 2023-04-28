from abc import ABC, abstractmethod


class SqsConnection(ABC):
    @abstractmethod
    def set_queue(self, queue_url: str):
        pass

    @abstractmethod
    def get_queue_resource(self, queue_url: str = None):
        pass

    @abstractmethod
    def _load_resource(self):
        pass

    @abstractmethod
    def _load_client(self):
        pass


class MessagePoller(ABC):
    @abstractmethod
    def start(self):
        pass


class Subscriber(ABC):
    @abstractmethod
    def set_queue(self, queue_url):
        pass

    @abstractmethod
    def receive_messages(self):
        pass

    @abstractmethod
    def chunk(self, num_messages=500, limit_seconds=30):
        pass


class Message(ABC):
    @property
    @abstractmethod
    def body(self) -> str:
        pass

    @property
    @abstractmethod
    def id(self) -> str:
        pass

    @property
    @abstractmethod
    def request_id(self) -> str:
        pass

    @property
    @abstractmethod
    def reply_queue_url(self) -> str:
        pass

    @property
    @abstractmethod
    def attributes(self) -> dict:
        pass


class MessageHandler(ABC):
    @abstractmethod
    def process_message(self, message: Message):
        pass


class RequestMessage(ABC):
    @abstractmethod
    def get_params(self) -> dict:
        pass

    @abstractmethod
    def get_response(self, timeout=10) -> Message:
        pass


class MessageList(ABC):
    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __add__(self, other_list):
        pass

    @abstractmethod
    def remove(self, message_id: int):
        pass

    @abstractmethod
    def delete(self):
        pass


class ReplyQueue(ABC):
    @abstractmethod
    def get_url(self):
        pass

    @abstractmethod
    def get_response_by_id(self, message_id: str, timeout: int = 2) -> Message:
        pass

    @abstractmethod
    def remove_queue(self):
        pass


class IdleQueueSweeper(ABC):
    @abstractmethod
    def set_name(self, name):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass


class Publisher(ABC):
    @abstractmethod
    def send_message(self, request_message: RequestMessage):
        pass
