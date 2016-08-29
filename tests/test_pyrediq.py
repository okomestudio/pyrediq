# -*- coding: utf-8 -*-
from __future__ import absolute_import
import inspect
import logging
import random
import string
import threading
import time
from StringIO import StringIO

import mock
import pytest
import redis

from pyrediq import pyrediq
from pyrediq import PyRediQ
from pyrediq import QueueEmpty
from pyrediq.pyrediq import Message
from pyrediq.pyrediq import Serializer


log = logging.getLogger(__name__)


TEST_QUEUE_PREFIX = '__PyRediQTest_'


def random_chars(n=12):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in xrange(n))


def generate_queue_name():
    return TEST_QUEUE_PREFIX + random_chars()


@pytest.fixture
def queue():
    mq = PyRediQ(generate_queue_name(), redis.StrictRedis())
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
        assert x == pyrediq.Serializer._binary_to_priority(f.read(1))


def test_single_consumer(queue, caplog):
    caplog.setLevel(logging.WARNING, logger='redis_lock')
    caplog.setLevel(logging.DEBUG, logger='pyrediq')

    msgs = [{'payload': {'message': '{!r}'.format(i)},
             'priority': random.randint(
                 PyRediQ.MIN_PRIORITY, PyRediQ.MAX_PRIORITY)}
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


def test_queue_construction():
    queue = PyRediQ(generate_queue_name())
    assert isinstance(queue._conn, redis.StrictRedis)

    redis_conn = redis.StrictRedis()
    queue = PyRediQ(generate_queue_name(), redis_conn=redis_conn)
    assert queue._conn == redis_conn

    with pytest.raises(ValueError) as exc:
        queue = PyRediQ(generate_queue_name(), redis_conn='dummy')
    assert 'is a StrictRedis instance' in exc.value.message


def test_consumer_get_message_blocking(queue):
    with mock.patch('redis.StrictRedis.rpoplpush') as mocked:
        xs = [None] * 100

        def f(*args):
            if xs:
                return xs.pop(0)
            else:
                raise QueueEmpty('bomb')

        mocked.side_effect = f
        with pytest.raises(QueueEmpty) as cm:
            with queue.consumer() as consumer:
                consumer.get(block=True, timeout=None)
        assert 'bomb' in cm.value.message


def test_consumer_get_message_blocking_with_timeout(queue):
    t0 = time.time()
    timeout = 1.0
    with pytest.raises(QueueEmpty):
        with queue.consumer() as consumer:
            consumer.get(block=True, timeout=timeout)
    assert time.time() - t0 > timeout


def test_consumer_get_message_nonblocking(queue):
    with pytest.raises(QueueEmpty):
        with queue.consumer() as consumer:
            consumer.get(block=False)


def test_consumer_ack_invalid(queue):
    with pytest.raises(ValueError) as cm:
        with queue.consumer() as consumer:
            consumer.ack(object())
    assert 'did not find' in cm.value.message


def test_consumer_reject_invalid(queue):
    with pytest.raises(ValueError) as cm:
        with queue.consumer() as consumer:
            consumer.reject(object())
    assert 'did not find' in cm.value.message


def test_consumer_is_in_processing_queue(queue):
    for _ in xrange(2):
        queue.put(payload='test')
    with queue.consumer() as consumer:
        msg = consumer.get()
        msg_pending = consumer.get()
        consumer.ack(msg)
        with pytest.raises(ValueError) as cm:
            consumer.ack(msg)
        assert 'did not find' in cm.value.message

    with queue.consumer() as consumer:
        msg = consumer.get()
        assert msg == msg_pending


def test_basic_workflow():
    with PyRediQ(generate_queue_name(), redis.StrictRedis()) as queue:
        queue.put(payload={'ack': True}, priority=+2)
        queue.put(payload={'reject': True}, priority=-2)

        with queue.consumer() as consumer:
            msg = consumer.get()
            assert 'reject' in msg.payload
            assert msg.priority == -2
            consumer.reject(msg, requeue=True)

            msg = consumer.get()
            assert 'reject' in msg.payload
            assert msg.priority == -2
            consumer.reject(msg)

            msg = consumer.get()
            assert 'ack' in msg.payload
            assert msg.priority == +2
            consumer.ack(msg)

        queue.purge()
