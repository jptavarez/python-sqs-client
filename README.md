python-sqs-client
========================

High-level library for asynchronous communication using Amazon SQS. 

It enables easy building of message producers that expect responses, as well as consumers that send responses.

It's based on [amazon-sqs-java-temporary-queues-client](https://github.com/awslabs/amazon-sqs-java-temporary-queues-client), but it does not use the concept of Virtual Queues.

Each producer has its own reply queue, and responses are stored in memory.

In a web environment, for example, a worker will use only one reply queue.

It has not been used in production yet.

Please feel free to fork it and contribute.

Use cases
===========

* Asynchronous communication between microservices
* One-way messaging
* Two-way Messaging (request-response)
