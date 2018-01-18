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

from wishbone_input_amqp import AMQPIn
from amqp.connection import Connection
from amqp.exceptions import NotFound
from gevent import sleep
import requests
from requests.auth import HTTPBasicAuth
from amqp import Connection, basic_message


def test_module_amqp_create_exchange_default():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="new_exchange_direct")
    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/exchanges/%2f/new_exchange_direct", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["name"] == "new_exchange_direct"

    amqp.stop()


def test_module_amqp_create_exchange_type():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="new_exchange_fanout", exchange_type="fanout")
    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/exchanges/%2f/new_exchange_fanout", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["type"] == "fanout"

    amqp.stop()


def test_module_amqp_create_exchange_durable():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="new_exchange_direct_1", exchange_durable=True)
    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/exchanges/%2f/new_exchange_direct_1", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["durable"] is True
    amqp.stop()


def test_module_amqp_create_exchange_auto_delete():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="new_exchange_direct_2", exchange_auto_delete=False)
    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/exchanges/%2f/new_exchange_direct_2", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["auto_delete"] is False
    amqp.stop()


def test_module_amqp_default_queue():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config)

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/queues/%2f/wishbone", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["name"] == "wishbone"

    amqp.stop()


def test_module_amqp_default_queue_binding():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="wishbone")

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/bindings/%2f/", auth=HTTPBasicAuth("guest", "guest"))

    ok = False
    for binding in response.json():
        if binding["source"] == "wishbone" and binding["destination"] == "wishbone":
            ok = True

    assert ok is True
    amqp.stop()


def test_module_amqp_default_queue_durable():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, queue_durable=True)

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/queues/%2f/wishbone", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["durable"] is True
    amqp.stop()


def test_module_amqp_default_queue_exclusive():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, queue_exclusive=True)

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/queues/%2f/wishbone", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["exclusive"] is True
    amqp.stop()


def test_module_amqp_default_queue_auto_delete():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, queue_auto_delete=True)

    amqp.pool.createQueue("outbox")
    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    response = requests.get("http://localhost:15672/api/queues/%2f/wishbone", auth=HTTPBasicAuth("guest", "guest"))
    assert response.json()["auto_delete"] is True
    amqp.stop()


def test_module_amqp_submit_message():

    actor_config = ActorConfig('amqp', 100, 1, {}, "", disable_exception_handling=True)
    amqp = AMQPIn(actor_config, exchange="wishbone")

    amqp.pool.queue.outbox.disableFallThrough()
    amqp.start()

    sleep(1)
    conn = Connection()
    conn.connect()
    channel = conn.channel()
    channel.basic_publish(basic_message.Message("test"), exchange="wishbone")
    channel.close()
    conn.close()
    sleep(1)
    event = getter(amqp.pool.queue.outbox)
    assert event.get() == "test"
    amqp.stop()

