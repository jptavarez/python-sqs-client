from sqs_client.connection import SqsConnection
from sqs_client.subscriber import Subscriber
from sqs_client.publisher import Publisher
from sqs_client.reply_queue import ReplyQueue

def build_sqs_connection(access_key, secret_key, region_name):
    return SqsConnection(
        access_key=access_key,
        secret_key=secret_key,
        region_name=region_name
    )

def build_subscriber(access_key, secret_key, region_name, queue_url):
    return Subscriber(
        sqs_connection=build_sqs_connection(access_key, secret_key, region_name),
        queue_url=queue_url
    )

def build_reply_queue(name, access_key, secret_key, region_name):
    return ReplyQueue(
        name=name,
        sqs_connection=build_sqs_connection(access_key, secret_key, region_name),
        subscriber=build_subscriber(access_key, secret_key, region_name)
    )

def build_publisher(access_key, secret_key, region_name):
    return Publisher(
        sqs_connection=build_sqs_connection(access_key, secret_key, region_name)
    )
