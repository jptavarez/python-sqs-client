import logging 

from sqs_client.factories import ReplyQueueFactory, PublisherFactory
from sqs_client.message import RequestMessage
from sqs_client.exceptions import ReplyTimeout

config = {
    "access_key": "",
    "secret_key": "",
    "queue_url": "",
    "region_name": 'us-east-1'
}

logging.basicConfig(level=logging.INFO)

reply_queue = ReplyQueueFactory(
    name='reply_queue_',
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
).build()

publisher = PublisherFactory(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
).build()

messages = []
for i in range(0, 10):
    print("Sending message...")
    message = RequestMessage(
        body='Hello world!!' + str(i),
        queue_url=config['queue_url'],
        reply_queue=reply_queue
    )
    publisher.send_message(message)
    messages.append(message)

for message in messages:
    try:
        response = message.get_response(timeout=20)
        print(response.body)
    except ReplyTimeout:
        print("Timeout")

reply_queue.remove_queue()   

