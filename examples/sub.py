from sqs_client.subscriber import MessagePoller 
from sqs_client.contracts import MessageHandler
from sqs_client.factories import build_subscriber, build_publisher

config = {
    "access_key": "",
    "secret_key": "",
    "link": "",
    "region_name": 'us-east-1'
}

subscriber = build_subscriber(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name'],
    queue_url=config['link']
)

publisher = build_publisher(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
)

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
