from sqs_client.factories import build_reply_queue, build_publisher
from sqs_client.message import RequestMessage

config = {
    "access_key": "AKIAYXEGDMMFMWNKCVCE",
    "secret_key": "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",
    "queue_url": "https://sqs.us-east-1.amazonaws.com/599429178122/myqueue01",
    "region_name": "us-east-1"
}

# creates queue, deletes queue, listens to queue, receives messages from queue, adds messages to self._messages
# has a subscriber.
# is passed by reference to as a part of each message and so there is only one of them
reply_queue = build_reply_queue(
    name='reply_queue_',
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
)

# merely sends a message to SQS
publisher = build_publisher(
    access_key=config['access_key'],
    secret_key=config['secret_key'],
    region_name=config['region_name']
)
def main():
    messages = []
    for i in range(0, 5):
        print("Sending message...")
        # a message is a RequestMessage
        # RequestMessage inherits from MessageBase
        # and gets passed to a publisher
        # each message has a reference to reply_queue class of which there is only one
        message = RequestMessage(
            body='Hello world!!' + str(i),
            queue_url=config['queue_url'],
            reply_queue=reply_queue
        )
        # when send_message occurs, it triggers setup of a temporary SQS Queue (via ReplyQueue) and launches another thread
        # which listens (via a Subscriber) and pushes messages into a list called _messages on ReplyQueue (i.e. a virtual local queue).
        publisher.send_message(message)
        # keep track of everything we sent to we can look at each request id and see if it's in the return results
        messages.append(message)

    # input("Press Enter to continue...")  # used for pause code to step through the subscriber code (sub.py)

    try:
        for message in messages:
            # RequestMessage vs ReplyQueue (has the reply messages in a list and is referenced by every message)
            # RequestMessage has a ReplyQueue which has a method get_response_by_id
            # ReplyQueue has a Subscriber that gets messages.
            # message.get_response() calls get_response_by_id on RequestQueue (which has the list of received messages)
            # it returns the message with the mataching message_id
            response = message.get_response(timeout=20)
            print(response.body)
    except Exception as e:
        print("The temporary queue created by the subscriber does not currently exist")
    finally:
        reply_queue.remove_queue()
        reply_queue.stop_receive_messages_thread()

if __name__ == "__main__":
    main()
    pass
