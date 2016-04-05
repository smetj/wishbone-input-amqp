::

              __       __    __
    .--.--.--|__.-----|  |--|  |--.-----.-----.-----.
    |  |  |  |  |__ --|     |  _  |  _  |     |  -__|
    |________|__|_____|__|__|_____|_____|__|__|_____|
                                       version 2.1.2

    Build composable event pipeline servers with minimal effort.


    ===================
    wishbone.input.amqp
    ===================

    Version: 1.0.0

    Consumes messages from AMQP.
    ----------------------------


        Consumes messages from an AMQP message broker.
        The declared <exchange> and <queue> will be bound to each other.

        Parameters:

            - host(str)("localhost")
               | The host to connect to.

            - port(int)(5672)
               | The port to connect to.

            - vhost(str)("/")
               |  The virtual host to connect to.

            - user(str)("guest")
               |  The username to authenticate.

            - password(str)("guest")
               |  The password to authenticate.

            - exchange(str)("")
               |  The exchange to declare.

            - exchange_type(str)("direct")
               |  The exchange type to create. (direct, topic, fanout)

            - exchange_durable(bool)(false)
               |  Declare a durable exchange.

            - exchange_auto_delete(bool)(true)
               |  If set, the exchange is deleted when all queues have finished using it.

            - exchange_passive(bool)(false)
               |  If set, the server will not create the exchange. The client can use
               |  this to check whether an exchange exists without modifying the server state.

            - exchange_arguments(dict)({})
               |  Additional arguments for exchange declaration.

            - queue(str)("wishbone")
               |  The queue to declare and ultimately consume.

            - queue_durable(bool)(false)
               |  Declare a durable queue.

            - queue_exclusive(bool)(false)
               |  Declare an exclusive queue.

            - queue_auto_delete(bool)(true)
               |  Whether to autodelete the queue.

            - queue_declare(bool)(true)
               |  Whether to actually declare the queue.

            - queue_arguments(dict)({})
               |  Additional arguments for queue declaration.

            - routing_key(str)("")
               |  The routing key to use in case of a "topic" exchange.
               | When the exchange is type "direct" the routing key is always equal
               | to the <queue> value.

            - prefetch_count(int)(1)
               |  Prefetch count value to consume messages from queue.

            - no_ack(bool)(false)
               |  Override acknowledgement requirement.


        Queues:

            - outbox
               |  Messages coming from the defined broker.

            - ack
               |  Messages to acknowledge (requires the delivery_tag)

            - cancel
               |  Cancels a message acknowledgement (requires the delivery_tag)
