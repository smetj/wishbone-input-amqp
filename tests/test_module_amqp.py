#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  test_module_httpinclient.py
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

from gevent.monkey import patch_all;patch_all()
from wishbone_input_amqp import AMQPIn
from wishbone.actor import ActorConfig
from wishbone.utils.test import getter
from gevent import sleep
import unittest
from docker import Client

from amqp.connection import Connection as amqp_connection
from amqp import basic_message

class TestConsume(unittest.TestCase):

    @classmethod
    def setupAMQP(self):

        self.connection = amqp_connection()

    @classmethod
    def createContainer(self):
        self.cli = Client(base_url='unix:///var/run/docker.sock')
        self.container = self.cli.create_container(
            image='rabbitmq:3-management',
            host_config=self.cli.create_host_config(
                port_bindings={
                    15672: 15672,
                    5672: 5672
                }
            )
        )
        self.cli.start(container=self.container.get('Id'))

    def submitMessage(self, message, exchange, routing_key):

        message = basic_message.Message(
            body="test",
            delivery_mode=1
        )
        channel = self.connection.channel()
        channel.basic_publish(
            message,
            exchange=exchange,
            routing_key=routing_key
        )
        channel.close()

    @classmethod
    def setUpClass(self):

        print "Creating container"
        self.createContainer()
        sleep(5)

        self.setupAMQP()

    @classmethod
    def tearDownClass(self):

        self.cli.kill(self.container.get('Id'))
        self.cli.remove_container(self.container.get('Id'))

    def test_defaultQueueWishbone(self):

        '''
        Only default module values.
        Queue wishbone is created and bound to the default exchange with queueu name "wishbone" as routing key.
        '''

        actor_config = ActorConfig('amqp', 100, 1, {}, "")
        amqp = AMQPIn(actor_config)
        amqp.pool.queue.outbox.disableFallThrough()
        amqp.start()
        sleep(5)

        self.submitMessage("test", exchange="", routing_key="wishbone")
        self.assertTrue(getter(amqp.pool.queue.outbox).get() == "test")

    def test_declareDirectExchange(self):

        '''
        Declares a direct exchange and binds a queue to it.
        '''

        actor_config = ActorConfig('amqp', 100, 1, {}, "")
        amqp = AMQPIn(actor_config, exchange="test", exchange_type="direct", queue="test")
        amqp.pool.queue.outbox.disableFallThrough()
        amqp.start()
        sleep(5)

        self.submitMessage("test", exchange="test", routing_key="")
        self.assertTrue(getter(amqp.pool.queue.outbox).get() == "test")

    def test_declareTopicExchange(self):

        '''
        Declares a direct exchange and binds a queue to it.
        '''

        actor_config = ActorConfig('amqp', 100, 1, {}, "")
        amqp = AMQPIn(actor_config, exchange="test_topic", exchange_type="topic", queue="test_topic", routing_key="test_topic")
        amqp.pool.queue.outbox.disableFallThrough()
        amqp.start()
        sleep(5)

        self.submitMessage("test", exchange="test_topic", routing_key="test_topic")
        self.assertTrue(getter(amqp.pool.queue.outbox).get() == "test")

if __name__ == '__main__':
    unittest.main()
