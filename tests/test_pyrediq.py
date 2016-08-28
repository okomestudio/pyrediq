# -*- coding: utf-8 -*-
from __future__ import absolute_import
import inspect
import logging
import random
import string
import threading
import time
from StringIO import StringIO

import pytest
import redis

from pyrediq.mq import Message
from pyrediq.mq import PyRediQ
from pyrediq.mq import Serializer
from pyrediq import mq


log = logging.getLogger(__name__)


def random_chars(n=12):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in xrange(n))


@pytest.fixture
def queue():
    mq = PyRediQ('__PyRediQTest_' + random_chars(), redis.StrictRedis())
    yield mq
    mq.purge()


def spawn(func, *args, **kwargs):
    th = threading.Thread(target=func, args=args, kwargs=kwargs)
    th.start()
    return th


def joinall(threads):
    for th in threads:
        # this should block
        th.join()


def message_producer(redis, queue, messages, sleep=None):
    """Simulates a function producing messages."""
    if inspect.isfunction(sleep):
        pass
    elif isinstance(sleep, (int, float)):
        time.sleep(random.random() * 0.1)
    else:
        def donothing():
            pass
        sleep = donothing

    with PyRediQ(queue, redis) as mq:
        for msg in messages:
            mq.put(**msg)
            sleep()


def message_consumer(redis, queue, message_count, timeout=None):
    with PyRediQ(queue, redis) as mq:
        with mq.consumer() as consumer:
            for _ in xrange(message_count):
                msg = consumer.get(block=True, timeout=timeout)
                # Simulate some computation after getting the message
                time.sleep(msg.payload.get('processing_time', 0))

                if msg.payload.get('reject'):
                    consumer.reject(msg)
                else:
                    consumer.ack(msg)


def test_default_message_creation():
    msg = Message()

    log.debug('Check defaults')
    assert isinstance(msg.id, str) and len(msg.id) == 32
    assert msg.payload is None
    assert msg.priority == 0


def test_message_creation():
    with pytest.raises(AssertionError) as ei:
        Message(priority='sfjei')
    assert 'must be int within' in ei.value.message

    expected = {'payload': {'test': 'value'}, 'priority': 2, '_id': '0' * 32}
    msg = Message(**expected)
    assert msg.id == expected['_id']
    for field in ['payload', 'priority']:
        assert getattr(msg, field) == expected[field]


def test_message_comparison():
    msg = Message()
    assert msg != Message()
    assert msg == Serializer.deserialize(Serializer.serialize(msg))


def test_message_serialization():
    msg = Message()
    assert msg == Serializer.deserialize(Serializer.serialize(msg))


def test_serializer_hex_conversion():
    f = StringIO(bytearray(range(248, 256) + range(0, 8)))
    for x in xrange(-8, 8):
        assert x == mq.Serializer._binary_to_priority(f.read(1))


def test_single_consumer(queue, caplog):
    caplog.setLevel(logging.WARNING, logger='redis_lock')
    caplog.setLevel(logging.DEBUG, logger='pyrediq')

    msgs = [{'payload': {'message': '{!r}'.format(i)},
             'priority': random.randint(
                 PyRediQ.min_priority, PyRediQ.max_priority)}
            for i in xrange(1)]

    threads = []
    threads.append(spawn(
        message_consumer, queue.redis_conn, queue.name, len(msgs)))

    message_producer(queue.redis_conn, queue.name, msgs)

    joinall(threads)
    for thread in threads:
        assert thread.is_alive() is False

    assert len(queue.consumers) == 0
    assert queue.is_empty()


# def test_clean_up_orphans(queue, caplog):
#     caplog.setLevel(logging.WARNING, logger='redis_lock')
#     caplog.setLevel(logging.DEBUG, logger='pyrediq')
