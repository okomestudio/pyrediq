# -*- coding: utf-8 -*-
"""Message queing via Redis.

This probably should be replaced with a real MQ system soon...

"""
from __future__ import absolute_import
import contextlib
import logging
import time
from uuid import uuid4

import gevent
import gevent.lock
import msgpack
from redis import StrictRedis


log = logging.getLogger(__name__)


class QueueEmpty(Exception):
    pass


class Message(dict):
    """Message to be passed around.

    When deserialized, it is basically a dict object with keys
    `payload`, `priority`, `_id`, and `_created` with the latter two
    automatically assigned by the constructor.

    """

    def __init__(self, payload=None, priority=0, _id=None, _created=None):
        super(Message, self).__init__(
            payload=payload, priority=priority, _id=_id, _created=_created)
        if self['_id'] is None:
            self['_id'] = uuid4().hex
        if self['_created'] is None:
            self['_created'] = time.time()

    def __repr__(self):
        return '<Message {}>'.format(self['_id'])

    def __eq__(self, other):
        return self['_id'] == other['_id']


def _serialize(message):
    return msgpack.packb(message)


def _deserialize(packed):
    return Message(**msgpack.unpackb(packed))


def _periodic(t, f, *args, **kwargs):
    # Not exactly periodic but okay...
    while 1:
        f(*args, **kwargs)
        gevent.sleep(t)


class MQ(object):
    """Priority queue implementation using multiple Redis lists."""

    _REDIS_KEY_NAME_ROOT = 'dmvmq'

    def __init__(self, name, redis=None, min_priority=-2, max_priority=+2):
        if redis is None:
            redis = StrictRedis()
        elif not isinstance(redis, StrictRedis):
            log.warn('`redis` should be a StrictRedis instance')
        self._redis = redis

        self.name = name

        self.min_priority = min_priority
        self.max_priority = max_priority

        self._queues = [self._get_queue_name(i) for i
                        in xrange(min_priority, max_priority + 1)]

        # periodically check if there are orphan consumers
        self._orphan_check = gevent.spawn(
            _periodic, MQConsumer.HEARTBEAT_INTERVAL, self._clean_up_orphans)

    @property
    def redis_key_prefix(self):
        return '{}:{}'.format(self._REDIS_KEY_NAME_ROOT, self.name)

    def _get_queue_name(self, priority):
        return '{}:p:{:+d}'.format(self.redis_key_prefix, priority)

    @property
    def _consumers(self):
        return [MQConsumer(self, id=id) for id in self._redis.hkeys(
            '{}:c:last_seen'.format(self.redis_key_prefix))]

    def _orphaned_consumers(self, idle_time=None):
        return [consumer for consumer in self._consumers
                if consumer.is_orphaned(idle_time)]

    def _clean_up_orphans(self, idle_time=None):
        for consumer in self._orphaned_consumers(idle_time):
            log.warn('found orphaned consumer {!r}'.format(consumer))
            consumer.cleanup()

    def is_empty(self):
        qs = self._redis.keys(self.redis_key_prefix + ':p:*')
        qs.extend(self._redis.keys(self.redis_key_prefix + ':c:*'))
        return not bool(qs)

    def purge(self):
        """Purge queue.

        WARNING: Before running this, make sure no threads/processes
        are running using this particular queue. Purging at a wrong
        timing may leave the info on Redis to be in a broken state.

        """
        keys = self._redis.keys(self.redis_key_prefix + ':*')
        if keys:
            self._redis.delete(*keys)

    def _get_queue_from_message(self, message):
        pri = max(self.min_priority,
                  min(message['priority'], self.max_priority))
        return self._get_queue_name(pri)

    def put(self, payload, priority=0, _id=None, _created=None):
        message = Message(
            payload=payload, priority=priority, _id=_id, _created=_created)
        queue = self._get_queue_from_message(message)
        log.debug('{!r} puts {!r}'.format(self, message))
        self._redis.lpush(queue, _serialize(message))

    @contextlib.contextmanager
    def consumer(self):
        try:
            consumer = MQConsumer(self)
            consumer.start_heartbeat()
            yield consumer
        finally:
            consumer.cleanup()


class MQConsumer(object):
    """Consumer attached to a MQ queue.

    An object of this class must always be obtained via MQ.consumer.

    """

    HEARTBEAT_INTERVAL = 30.0

    def __init__(self, mq, id=None, start_heartbeat=True):
        self._mq = mq
        self._lock = gevent.lock.Semaphore(1)
        self.id = uuid4().hex if id is None else id

        self._pq = '{}:c:{}'.format(self._mq.redis_key_prefix, self.id)
        self._last_seen_key = '{}:c:last_seen'.format(self._mq.redis_key_prefix)

    @property
    def _redis(self):
        return self._mq._redis

    def __repr__(self):
        return '<MQConsumer {}>'.format(self.id)

    def start_heartbeat(self):
        self._hb = gevent.spawn(
            _periodic, self.HEARTBEAT_INTERVAL, self._heartbeat)

    def stop_heartbeat(self):
        if hasattr(self, '_hb'):
            self._hb.kill()
        self._redis.hdel(self._last_seen_key, self.id)

    def _heartbeat(self):
        self._redis.hset(self._last_seen_key, self.id, str(time.time()))

    @property
    def _last_seen(self):
        try:
            return float(self._redis.hget(self._last_seen_key, self.id))
        except Exception as e:
            return None

    def is_orphaned(self, idle_time=None):
        if self._last_seen is None:
            return False
        dt = (self.HEARTBEAT_INTERVAL * 2. if idle_time is None else idle_time)
        return bool(time.time() - self._last_seen > dt)

    def cleanup(self):
        log.debug('cleaning up {!r}'.format(self))
        with self._lock:
            self._flush_processing_queue()
            assert self._redis.llen(self._pq) == 0
            self._redis.delete(self._pq)
            self.stop_heartbeat()

    def _flush_processing_queue(self):
        for _ in xrange(self._redis.llen(self._pq)):
            self._requeue()

    def _requeue(self, message=None):
        msg = _deserialize(self._redis.lindex(self._pq, -1))
        if message is not None:
            assert message == msg
        queue = self._mq._get_queue_from_message(msg)
        item = self._redis.rpoplpush(self._pq, queue)
        if message is not None:
            assert message == _deserialize(item)
        log.debug('{!r} requeued {!r} to {}'.format(self, msg, queue))

    def _remove(self, message):
        msg = self._redis.rpop(self._pq)
        assert message == _deserialize(msg)
        log.debug('{!r} removed {!r}'.format(self, message))

    def _is_processing(self, message):
        for _ in xrange(self._redis.llen(self._pq)):
            msg = _deserialize(self._redis.lindex(self._pq, -1))
            if msg == message:
                return True
            self._redis.rpoplpush(self._pq, self._pq)
        return False

    def get(self, block=True, timeout=None):
        t_called = time.time()
        while 1:
            for queue in self._mq._queues:
                with self._lock:
                    packed = self._redis.rpoplpush(queue, self._pq)
                gevent.sleep()
                if packed is not None:
                    message = _deserialize(packed)
                    log.debug('{!r} gets {!r}'.format(self, message))
                    return message
            else:
                dt = time.time() - t_called
                if (block is not True
                    or (timeout is not None and dt >= timeout)):
                    raise QueueEmpty
                dt = timeout - dt
                log.debug('{!r} sleeping for {} s'.format(self, dt))
                gevent.sleep(dt)

    def ack(self, message):
        with self._lock:
            if self._is_processing(message):
                log.debug('{!r} acked {!r}'.format(self, message))
                self._remove(message)
            else:
                raise ValueError('{!r} did not find {!r}'.format(self, message))

    def reject(self, message, requeue=False):
        with self._lock:
            if self._is_processing(message):
                log.debug('{!r} rejected {!r}'.format(self, message))
                if requeue:
                    self._requeue(message)
                else:
                    self._remove(message)
            else:
                raise ValueError('{!r} did not find {!r}'.format(self, message))
