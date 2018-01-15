#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  default.py
#
#  Copyright 2017 Jelle Smet <development@smetj.net>
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

from gevent import monkey; monkey.patch_all()

from wishbone.actor import ActorConfig
from wishbone.utils.test import getter
from wishbone.event import Event

from wishbone_input_httpserver import HTTPServer

from wishbone_input_amqp import AMQPIn
from amqp.connection import Connection
from amqp.exceptions import NotFound
from gevent import sleep


class AMQPConnection(object):

    def __init__(self):
        self.connection = Connection(
            host="localhost",
            port=5672,
            virtual_host="/",
            userid="guest",
            password="guest"
        )
        self.connection.connect()
        self.channel = self.connection.channel()

    def __enter__(self, *args, **kwargs):

        return self.channel

    def __exit__(self, *args, **kwargs):
        self.channel.close()
        self.connection.close()


def test_module_amqp_default_queue():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config)

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()
    sleep(1)

    with AMQPConnection() as amqp_test:
        try:
            amqp_test.queue_declare("wishbone", passive=True)
        except NotFound:
            assert False, "Queue wishbone does not exist"
        else:
            assert True

    amqp.stop()


def test_module_amqp_default_queue_binding():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="wishbone")

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()
    sleep(1)

    with AMQPConnection() as amqp_test:
        try:
            amqp_test.queue_unbind(
                "wishbone",
                "wishbone",
                routing_key="wishbone",
            )
        except NotFound:
            assert False, "Queue wishbone is not bound"
        else:
            assert True

    amqp.stop()
