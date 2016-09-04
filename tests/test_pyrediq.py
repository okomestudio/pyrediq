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

from pyrediq import PriorityQueue
from pyrediq import QueueEmpty
from pyrediq.priority_queue import Message
from pyrediq.priority_queue import Packed


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
    mq = PriorityQueue(generate_queue_name(), redis.StrictRedis())
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


def message_producer(redis_conn, queue_name, messages, sleep=None):
    """Simulates a function producing messages."""
    if inspect.isfunction(sleep):
        pass
    elif isinstance(sleep, (int, float)):
        time.sleep(random.random() * 0.1)
    else:
        def donothing():
            pass
        sleep = donothing

    queue = PriorityQueue(queue_name, redis_conn)
    for msg in messages:
        queue.put(**msg)
        sleep()


def message_consumer(redis_conn, queue_name, message_count, timeout=None):
    queue = PriorityQueue(queue_name, redis_conn)
    with queue.consumer() as consumer:
        for _ in xrange(message_count):
            msg = consumer.get(block=True, timeout=timeout)

            # trigger exception to simulate failure
            bomb = msg.payload.get('bomb', False)
            if bomb:
                raise Exception('Bomb exploded')

            # simulate some computation after getting the message
            proc_time = msg.payload.get('processing_time', 0)
            time.sleep(proc_time)

            if msg.payload.get('reject'):
                consumer.reject(msg)
            else:
                consumer.ack(msg)


def test_single_consumer(queue, caplog):
    caplog.setLevel(logging.DEBUG, logger='pyrediq')

    msgs = [{'payload': {'message': '{!r}'.format(i)},
             'priority': random.randint(
                 PriorityQueue.MIN_PRIORITY, PriorityQueue.MAX_PRIORITY)}
            for i in xrange(1)]

    threads = []
    threads.append(spawn(
        message_consumer, queue._conn, queue.name, len(msgs)))

    message_producer(queue._conn, queue.name, msgs)

    joinall(threads)
    for thread in threads:
        assert thread.is_alive() is False

    assert len(queue.consumers) == 0
    assert queue.is_empty()


def test_multiple_consumers(queue, caplog):
    caplog.setLevel(logging.WARNING, logger='redis_lock')
    caplog.setLevel(logging.DEBUG, logger='pyrediq')

    n_message = 10

    msgs = [{'payload': {'processing_time': random.random()},
             'priority': random.randint(
                 PriorityQueue.MIN_PRIORITY, PriorityQueue.MAX_PRIORITY)}
            for i in xrange(n_message)]

    threads = []
    for _ in xrange(n_message):
        threads.append(spawn(
            message_consumer, queue._conn, queue.name, 1))

    message_producer(queue._conn, queue.name, msgs)

    joinall(threads)
    for thread in threads:
        assert thread.is_alive() is False

    assert len(queue.consumers) == 0
    assert queue.is_empty()


def test_consumer_fail(queue, caplog):
    caplog.setLevel(logging.DEBUG, logger='pyrediq')

    n_message = 10

    msgs = [{'payload': {'bomb': True,
                         'processing_time': random.random()},
             'priority': random.randint(
                 PriorityQueue.MIN_PRIORITY, PriorityQueue.MAX_PRIORITY)}
            for i in xrange(n_message)]

    threads = []
    for _ in xrange(n_message):
        threads.append(spawn(
            message_consumer, queue._conn, queue.name, 1))

    message_producer(queue._conn, queue.name, msgs)

    joinall(threads)
    for thread in threads:
        assert thread.is_alive() is False

    assert len(queue.consumers) == 0
    assert queue.size() == n_message
    assert not queue.is_empty()


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
    assert msg == Packed.serialize(msg).deserialize()


def test_message_serialization():
    msg = Message()
    assert msg == Packed.serialize(msg).deserialize()


def test_serializer_hex_conversion():
    f = StringIO(bytearray(range(248, 256) + range(0, 8)))
    for x in xrange(-8, 8):
        assert x == Packed._binary_to_priority(f.read(1))


def test_queue_construction():
    queue = PriorityQueue(generate_queue_name())
    assert isinstance(queue._conn, redis.StrictRedis)

    redis_conn = redis.StrictRedis()
    queue = PriorityQueue(generate_queue_name(), redis_conn=redis_conn)
    assert queue._conn == redis_conn

    with pytest.raises(ValueError) as exc:
        queue = PriorityQueue(generate_queue_name(), redis_conn='dummy')
    assert 'is a StrictRedis instance' in exc.value.message


def test_queue_purge():
    queue = PriorityQueue(generate_queue_name())
    with queue.consumer() as consumer:
        assert consumer in queue.consumers
        with pytest.raises(RuntimeError):
            queue.purge()


def test_consumer_len(queue):
    queue.put('1')
    queue.put('2')
    with queue.consumer() as consumer:
        assert 0 == len(consumer)
        msg1 = consumer.get()
        assert 1 == len(consumer)
        msg2 = consumer.get()
        assert 2 == len(consumer)
        consumer.ack(msg1)
        assert 1 == len(consumer)
        consumer.reject(msg2)
        assert 0 == len(consumer)


def test_consumer_get_message_blocking(queue):
    with mock.patch('redis.StrictRedis.blpop') as mocked:
        def f(*args):
            time.sleep(2)
            raise Exception('bomb')
        mocked.side_effect = f

        with pytest.raises(Exception) as cm:
            with queue.consumer() as consumer:
                consumer.get(block=True, timeout=None)
        assert 'bomb' in cm.value.message


def test_consumer_get_message_blocking_with_timeout(queue):
    t0 = time.time()
    timeout = 1
    with pytest.raises(QueueEmpty):
        with queue.consumer() as consumer:
            consumer.get(block=True, timeout=timeout)
    assert time.time() - t0 > timeout


def test_consumer_get_message_nonblocking(queue):
    with pytest.raises(QueueEmpty):
        with queue.consumer() as consumer:
            consumer.get(block=False)


class MockMessage(object):
    id = 'badid'


def test_consumer_ack_invalid(queue):
    with pytest.raises(ValueError) as cm:
        with queue.consumer() as consumer:
            consumer.ack(MockMessage)
    assert 'did not find' in cm.value.message


def test_consumer_reject_invalid(queue):
    with pytest.raises(ValueError) as cm:
        with queue.consumer() as consumer:
            consumer.reject(MockMessage)
    assert 'did not find' in cm.value.message


def test_consumer_consumed_message_validation(queue):
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


def test_consumer_requeue_critical_failure(queue):
    queue.put('test')
    with queue.consumer() as consumer:
        msg = consumer.get()
        with mock.patch('redis.StrictRedis.rpush') as m:
            m.side_effect = Exception('failed')
            with pytest.raises(Exception) as exc:
                consumer.reject(msg, requeue=True)
            assert 'failed' in exc.value.message
        assert 1 == len(consumer)
    assert 1 == queue.size()


def test_consumer_get_critical_failure(queue):
    queue.put('test')
    with queue.consumer() as consumer:
        with mock.patch('redis.StrictRedis.hset') as m:
            m.side_effect = Exception('failed')
            with pytest.raises(Exception) as exc:
                consumer.get()
            assert 'failed' in exc.value.message
        assert 0 == len(consumer)
        assert 1 == queue.size()


def test_message_change_priority_within_consumer(queue):
    payload = 'test'
    queue.put(payload=payload, priority=3)
    with queue.consumer() as consumer:
        msg = consumer.get()
        assert payload == msg.payload
        assert 3 == msg.priority
        consumer.reject(msg)
        queue.put(msg.payload, priority=-3)

    with queue.consumer() as consumer:
        msg = consumer.get()
        assert payload == msg.payload
        assert -3 == msg.priority


def test_message_fifo(queue, caplog):
    caplog.setLevel(logging.DEBUG, logger='pyrediq')
    for priority in xrange(-7, 8):
        for i in xrange(10):
            payload = {'inserted': i, 'priority': priority}
            queue.put(payload=payload, priority=priority)

    with queue.consumer() as consumer:
        for priority in xrange(-7, 8):
            for i in xrange(10):
                msg = consumer.get()
                log.debug('%r', msg.payload)
                assert i == msg.payload['inserted']
                assert priority == msg.payload['priority']
                consumer.ack(msg)


def test_basic_workflow():
    with PriorityQueue(generate_queue_name(), redis.StrictRedis()) as queue:
        queue.put(payload={'ack': True}, priority=+2)
        queue.put(payload={'reject': True}, priority=-2)

        with queue.consumer() as consumer:
            assert consumer in queue.consumers

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

        assert consumer not in queue.consumers

        queue.purge()
