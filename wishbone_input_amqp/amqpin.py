#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  amqpin.py
#
#  Copyright 2016 Jelle Smet <development@smetj.net>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#

from nucleon.amqp.connection import Connection
from nucleon.amqp.exceptions import ConnectionError
from nucleon.amqp.message import Message
from wishbone import Actor
from gevent import sleep
from gevent.queue import Empty
from wishbone.event import Event
from gevent.hub import Hub
from gevent.event import Event as g_Event

def print_exception(*args, **kwargs):
    pass


class AMQPIn(Actor):

    '''**Consumes messages from AMQP.**

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

        - no_ack(bool)(true)
           |  Override acknowledgement requirement.

        - heartbeat(int)(30)
           |  Sets the heartbeat interval.

    Queues:

        - outbox
           |  Messages coming from the defined broker.

        - ack
           |  Messages to acknowledge (requires the delivery_tag)

        - cancel
           |  Cancels a message acknowledgement (requires the delivery_tag)
    '''

    def __init__(self, actor_config, host="localhost", port=5672, vhost="/", user="guest", password="guest",
                 exchange="", exchange_type="direct", exchange_durable=False, exchange_auto_delete=True, exchange_passive=False,
                 exchange_arguments={},
                 queue="wishbone", queue_durable=False, queue_exclusive=False, queue_auto_delete=True, queue_declare=True,
                 queue_arguments={},
                 routing_key="", prefetch_count=1, no_ack=True, heartbeat=30):
        Actor.__init__(self, actor_config)

        self.pool.createQueue("outbox")
        self.pool.createQueue("ack")
        self.pool.createQueue("cancel")
        self.pool.queue.ack.disableFallThrough()
        self.connection = None

        self.go_connect = g_Event()
        self.go_connect.set()
        Hub.print_exception = print_exception

    def preHook(self):
        self._queue_arguments = dict(self.kwargs.queue_arguments)
        self._exchange_arguments = dict(self.kwargs.exchange_arguments)
        # self.setupInitialConnection()
        self.sendToBackground(self.setupConnectivityLoop)

        # self.sendToBackground(self.drain)
        # self.sendToBackground(self.handleAcknowledgements)
        # self.sendToBackground(self.handleAcknowledgementsCancel)

    def consume(self, message):

        if isinstance(message, Message):
            event = Event(str(message.body))
            event.set(message.delivery_tag, "@tmp.%s.delivery_tag" % (self.name))
            self.submit(event, self.pool.queue.outbox)
        else:
            if not self.loop():
                self.logging.error("Problem connecting to broker. Reason: %s" % (event.body))
                self.go_connect.set()

    def setupConnectivityLoop(self):

        while self.loop():
            self.go_connect.wait()
            try:
                self.setupConnection()
            except Exception as err:
                self.logging.error("Failed to connect to broker.  Reason %s " % (err))
                sleep(1)
            else:
                self.go_connect.clear()

    def setupConnection(self):

        url = "amqp://%s:%s@%s:%s%s" % (
            self.kwargs.user,
            self.kwargs.password,
            self.kwargs.host,
            self.kwargs.port,
            self.kwargs.vhost,
        )

        self.connection = Connection(
            amqp_url=url,
            heartbeat=self.kwargs.heartbeat
        )

        self.connection.connect()
        self.channel = self.connection.allocate_channel()
        self.channel.basic_qos(prefetch_count=self.kwargs.prefetch_count)

        if self.kwargs.exchange != "":
            self.channel.exchange_declare(
                self.kwargs.exchange,
                self.kwargs.exchange_type,
                durable=self.kwargs.exchange_durable,
                auto_delete=self.kwargs.exchange_auto_delete,
                passive=self.kwargs.exchange_passive,
                arguments=self._exchange_arguments
            )
            self.logging.debug("Declared exchange %s." % (self.kwargs.exchange))

        if self.kwargs.queue_declare:
            self.channel.queue_declare(
                self.kwargs.queue,
                durable=self.kwargs.queue_durable,
                exclusive=self.kwargs.queue_exclusive,
                auto_delete=self.kwargs.queue_auto_delete,
                arguments=self._queue_arguments
            )
            self.logging.debug("Declared queue %s." % (self.kwargs.queue))

        if self.kwargs.exchange != "":
            self.channel.queue_bind(
                self.kwargs.queue,
                self.kwargs.exchange,
                routing_key=self.kwargs.routing_key
            )
            self.logging.debug("Bound queue %s to exchange %s." % (self.kwargs.queue, self.kwargs.exchange))

        self.channel.basic_consume(
            queue=self.kwargs.queue,
            no_ack=self.kwargs.no_ack,
            callback=self.consume
        )

        self.logging.info("Connected to broker.")

    def handleAcknowledgements(self):

        while self.loop():
            event = self.pool.queue.ack.get()
            if event.has("@tmp.%s.delivery_tag" % (self.name)):
                try:
                    self.channel.basic_ack(event.get("@tmp.%s.delivery_tag" % (self.name)))
                except Exception as err:
                    self.submit(event, "ack")
                    self.logging.error("Failed to acknowledge message.  Reason: %s." % (err))
                    sleep(0.5)
            else:
                self.logging.debug("Cannot acknowledge message because '@tmp.%s.delivery_tag' is missing." % (self.name))

    def handleAcknowledgementsCancel(self):

        while self.loop():
            event = self.pool.queue.cancel.get()
            try:
                self.channel.basic_reject(event.get("@tmp.%s.delivery_tag" % (self.name)), True)
            except Exception as err:
                self.submit(event, "cancel")
                self.logging.error("Failed to cancel acknowledge message.  Reason: %s." % (err))
                sleep(0.5)

    def postHook(self):
        self.channel.close()
        self.connection.close()
