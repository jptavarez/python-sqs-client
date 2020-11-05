from sqs_client.subscriber import MessagePoller 
from sqs_client.contracts import MessageHandler
from sqs_client.factories import SubscriberFactory, PublisherFactory

config = {
    "access_key": "",
    "secret_key": "",
    "queue_url": "",
    "region_name": 'us-east-1'
}

subscriber = SubscriberFactory(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name'],
    queue_url=config['queue_url']
).build()

publisher = PublisherFactory(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
).build()

class TestHandler(MessageHandler):
    
    def process_message(self, message):
        response = message.body + ' successfully processed!! '
        print(response)
        return response

poll = MessagePoller(
    handler=TestHandler(),
    subscriber=subscriber,
    publisher=publisher
)
poll.start()
