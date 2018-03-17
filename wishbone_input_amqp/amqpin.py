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

from gevent import monkey
monkey.patch_all()
from wishbone.module import InputModule
from amqp.connection import Connection as amqp_connection
from gevent import sleep


class AMQPIn(InputModule):

    '''**Consumes messages from AMQP.**

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
    '''

    def __init__(self, actor_config, native_events=False, destination="data",
                 host="localhost", port=5672, vhost="/", user="guest", password="guest", ssl=False, heartbeat=0,
                 exchange="", exchange_type="direct", exchange_durable=False, exchange_auto_delete=True, exchange_passive=False,
                 exchange_arguments={},
                 queue="wishbone", queue_durable=False, queue_exclusive=False, queue_auto_delete=True, queue_declare=True,
                 queue_arguments={},
                 routing_key="", prefetch_count=1, no_ack=False):
        InputModule.__init__(self, actor_config)

        self.pool.createQueue("outbox")
        self.pool.createQueue("ack")
        self.pool.createQueue("cancel")
        self.pool.queue.ack.disableFallThrough()
        self.connection = None
        self.connected = False

    def preHook(self):
        self._queue_arguments = dict(self.kwargs.queue_arguments)
        self._exchange_arguments = dict(self.kwargs.exchange_arguments)
        self.sendToBackground(self.drain)
        self.sendToBackground(self.handleAcknowledgements)
        self.sendToBackground(self.handleAcknowledgementsCancel)
        if self.kwargs.heartbeat > 0:
            self.logging.info("Sending heartbeat every %s seconds." % (self.kwargs.heartbeat))
            self.sendToBackground(self.heartbeat)

    def consume(self, message):
        for chunk in [message.body, None]:
            for item in self.decode(chunk):
                event = self.generateEvent(
                    item,
                    self.kwargs.destination
                )
                event.set({}, "tmp.%s" % (self.name))
                event.set(message.delivery_info["delivery_tag"], "tmp.%s.delivery_tag" % (self.name))
                self.submit(event, "outbox")

    def setupConnectivity(self):

        while self.loop():
            try:
                self.connection = amqp_connection(
                    heartbeat=self.kwargs.heartbeat,
                    host=self.kwargs.host,
                    port=self.kwargs.port,
                    virtual_host=self.kwargs.vhost,
                    userid=self.kwargs.user,
                    password=self.kwargs.password,
                    ssl=self.kwargs.ssl
                )
                self.connection.connect()
                self.channel = self.connection.channel()

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

                self.channel.basic_qos(prefetch_size=0, prefetch_count=self.kwargs.prefetch_count, a_global=False)
                self.channel.basic_consume(self.kwargs.queue, callback=self.consume, no_ack=self.kwargs.no_ack)
                self.logging.info("Connected to broker.")
            except Exception as err:
                self.logging.error("Failed to connect to broker.  Reason %s " % (err))
                sleep(1)
            else:
                self.connected = True
                break

    def drain(self):

        self.setupConnectivity()
        while self.loop():
            try:
                self.connection.drain_events()
            except Exception as err:
                self.connected = False
                self.logging.error("Problem connecting to broker.  Reason: %s" % (err))
                self.setupConnectivity()
                sleep(1)

    def heartbeat(self):

        while self.loop():
            sleep(self.kwargs.heartbeat)
            try:
                if self.connected:
                    self.connection.send_heartbeat()
            except Exception as err:
                self.logging.error("Failed to send heartbeat. Reason: %s" % (err))

    def handleAcknowledgements(self):
        while self.loop():
            event = self.pool.queue.ack.get()
            if event.has("tmp.%s.delivery_tag" % (self.name)):
                try:
                    self.channel.basic_ack(event.get("tmp.%s.delivery_tag" % (self.name)))
                except Exception as err:
                    self.pool.queue.ack.rescue(event)
                    self.logging.error("Failed to acknowledge message.  Reason: %s." % (err))
                    sleep(1)
            else:
                self.logging.debug("Cannot acknowledge message because 'tmp.%s.delivery_tag' is missing." % (self.name))

    def handleAcknowledgementsCancel(self):
        while self.loop():
            event = self.pool.queue.cancel.get()
            try:
                self.channel.basic_reject(event.get("tmp.%s.delivery_tag" % (self.name)), True)
            except Exception as err:
                self.pool.queue.ack.rescue(event)
                self.logging.error("Failed to cancel acknowledge message.  Reason: %s." % (err))
                sleep(1)

    def postHook(self):
        try:
            self.channel.close()
        except Exception as err:
            del(err)

        try:
            self.connection.close()
        except Exception as err:
            del(err)
