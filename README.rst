::
            __       __    __
  .--.--.--|__.-----|  |--|  |--.-----.-----.-----.
  |  |  |  |  |__ --|     |  _  |  _  |     |  -__|
  |________|__|_____|__|__|_____|_____|__|__|_____|


  ==================================
  wishbone_contrib.module.input.amqp
  ==================================

  Version: 3.0.3


  Consumes messages from AMQP.
  ----------------------------
  **Consumes messages from AMQP.**

      Consumes messages from an AMQP message broker.
      The declared <exchange> and <queue> will be bound to each other.

      Parameters:

          - exchange(str)("")
             |  The exchange to declare.

          - exchange_arguments(dict)({})
             |  Additional arguments for exchange declaration.

          - exchange_auto_delete(bool)(true)
             |  If set, the exchange is deleted when all queues have finished using it.

          - exchange_durable(bool)(false)
             |  Declare a durable exchange.

          - exchange_passive(bool)(false)
             |  If set, the server will not create the exchange. The client can use
             |  this to check whether an exchange exists without modifying the server state.

          - exchange_type(str)("direct")
             |  The exchange type to create. (direct, topic, fanout)

          - heartbeat(int)(0)
              | Enable AMQP heartbeat. The value is the interval in seconds.
              | 0 disables heartbeat support.

          - host(str)("localhost")
             | The host to connect to.

          - interval(float)(1)
             |  The interval in seconds between each generated event.
             |  A value of 0 means as fast as possible.

          - native_events(bool)(False)
             |  Whether to expect incoming events to be native Wishbone events

          - no_ack(bool)(false)
             |  Override acknowledgement requirement.

          - password(str)("guest")
             |  The password to authenticate.

          - port(int)(5672)
             | The port to connect to.

          - prefetch_count(int)(1)
             |  Prefetch count value to consume messages from queue.

          - queue(str)("wishbone")
             |  The queue to declare and ultimately consume.

          - queue_arguments(dict)({})
             |  Additional arguments for queue declaration.

          - queue_auto_delete(bool)(true)
             |  Whether to autodelete the queue.

          - queue_declare(bool)(true)
             |  Whether to actually declare the queue.

          - queue_durable(bool)(false)
             |  Declare a durable queue.

          - queue_exclusive(bool)(false)
             |  Declare an exclusive queue.

          - routing_key(str)("")
             |  The routing key to use in case of a "topic" exchange.
             | When the exchange is type "direct" the routing key is always equal
             | to the <queue> value.

          - ssl(bool)(False)
             |  If True expects SSL

          - user(str)("guest")
             |  The username to authenticate.

          - vhost(str)("/")
             |  The virtual host to connect to.


      Queues:

          - outbox
             |  Messages coming from the defined broker.

          - ack
             |  Messages to acknowledge (requires the delivery_tag)

          - cancel
             |  Cancels a message acknowledgement (requires the delivery_tag)

