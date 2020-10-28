from sqs_client.factories import build_reply_queue, build_publisher
from sqs_client.message import RequestMessage

config = {
    "access_key": "",
    "secret_key": "",
    "queue_url": "",
    "region_name": 'us-east-1'
}

reply_queue = build_reply_queue(
    name='reply_queue_',
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
)

publisher = build_publisher(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
)

def main():
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

    try:
        for message in messages:
            response = message.get_response(timeout=20)
            print(response.body)
    except Exception as e:
        print("The temporary queue created by the subscriber does not currently exist")
    finally:
        reply_queue.remove_queue()

if __name__ == "__main__":
    main()
    pass