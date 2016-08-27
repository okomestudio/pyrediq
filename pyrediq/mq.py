# -*- coding: utf-8 -*-
"""Priority message queue with Redis

"""
from __future__ import absolute_import
import logging
import threading
import time
from uuid import uuid4

import msgpack
import redis_lock
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
        if _id is None:
            _id = uuid4().hex
        if _created is None:
            _created = time.time()
        assert isinstance(priority, int), 'priority must be int'
        super(Message, self).__init__(
            payload=payload, priority=priority, _id=_id, _created=_created)

    def __repr__(self):
        return ('Message(payload={payload}, priority={priority}, '
                '_id={_id}, _created={_created})').format(**self)

    def __eq__(self, other):
        return self['_id'] == other['_id']


def _serialize(message):
    return msgpack.packb(message)


def _deserialize(packed):
    return Message(**msgpack.unpackb(packed))


class _OrphanedConsumerCleaner(threading.Thread):

    def __init__(self, mq, check_interval, idle_time=None):
        super(_OrphanedConsumerCleaner, self).__init__()
        self.mq = mq
        self.check_interval = check_interval
        self.idle_time = idle_time
        self._stop = threading.Event()
        log.info('%r initialized', self)

    def clean_up_orphaned_consumers(self, idle_time=None):
        idle_time = idle_time if idle_time is not None else self.idle_time
        log.info('%r is looking for orphaned consumers...', self)
        for consumer in self.mq._orphaned_consumers(idle_time):
            log.info('Found orphaned consumer %r. Cleaning up...',
                     consumer)
            consumer.cleanup()

    def stop(self):
        self._stop.set()

    def run(self):

        log.info('%r started', self)
        dt = 1.0
        time_before_next_check = -1.0
        while not self._stop.isSet():
            if time_before_next_check < 0.0:
                self.clean_up_orphaned_consumers()
                time_before_next_check = self.check_interval
            time_before_next_check -= dt
            time.sleep(dt)
        log.info('%r stopped', self)


class PyRediQ(object):
    """Priority queue implementation using multiple Redis lists."""

    _REDIS_KEY_NAME_ROOT = '__PyRediQ'

    min_priority = -2
    max_priority = +2

    def __init__(self, name, redis=None, min_priority=None, max_priority=None,
                 orphan_idle_time=None):
        if redis is None:
            redis = StrictRedis()
        elif not isinstance(redis, StrictRedis):
            log.warning('`redis` should be a StrictRedis instance')
        self._conn = redis

        self.name = name

        if min_priority is not None:
            self.min_priority = min_priority
        if max_priority is not None:
            self.max_priority = max_priority

        self._queues = [self._get_queue_name(i) for i
                        in xrange(self.min_priority, self.max_priority + 1)]

        # periodically check if there are orphan consumers
        self._orphan_check = _OrphanedConsumerCleaner(
            self, MessageConsumer.HEARTBEAT_INTERVAL,
            idle_time=orphan_idle_time)
        self._orphan_check.clean_up_orphaned_consumers(0.0)
        self._orphan_check.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._orphan_check.stop()
        self._orphan_check.join()

    @property
    def redis_conn(self):
        return self._conn

    @property
    def redis_key_prefix(self):
        return '{}:{}'.format(self._REDIS_KEY_NAME_ROOT, self.name)

    def _get_queue_name(self, priority):
        return '{}:p:{:+d}'.format(self.redis_key_prefix, priority)

    @property
    def _consumers(self):
        return [MessageConsumer(self, id=id) for id in self._conn.hkeys(
            '{}:c:last_seen'.format(self.redis_key_prefix))]

    def _orphaned_consumers(self, idle_time=None):
        return [consumer for consumer in self._consumers
                if consumer.is_orphaned(idle_time)]

    def is_empty(self):
        qs = self._conn.keys(self.redis_key_prefix + ':p:*')
        qs.extend(self._conn.keys(self.redis_key_prefix + ':c:*'))
        return not bool(qs)

    def purge(self):
        """Purge queue.

        WARNING: Before running this, make sure no threads/processes
        are running using this particular queue. Purging at a wrong
        timing may leave the info on Redis to be in a broken state.

        """
        log.warning('Purging %r...', self)
        self._orphan_check.stop()
        self._orphan_check.join()
        keys = self._conn.keys(self.redis_key_prefix + ':*')
        if keys:
            log.debug('Redis keys to be removed: %s', ' '.join(keys))
            self._conn.delete(*keys)
        log.warning('%r is purged', self)

    def _get_queue_from_message(self, message):
        pri = max(self.min_priority,
                  min(message['priority'], self.max_priority))
        return self._get_queue_name(pri)

    def put(self, payload, priority=0, _id=None, _created=None):
        message = Message(
            payload=payload, priority=priority, _id=_id, _created=_created)
        queue = self._get_queue_from_message(message)
        log.info('%r puts %r', self, message)
        self._conn.lpush(queue, _serialize(message))

    def consumer(self):
        return MessageConsumer(self)


class _HeartBeater(threading.Thread):

    def __init__(self, consumer, beat_interval):
        super(_HeartBeater, self).__init__()
        self.consumer = consumer
        self.beat_interval = beat_interval
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        dt = 1.0
        time_before_next_check = -1.0
        while not self._stop.isSet():
            if time_before_next_check < 0.0:
                log.info('%r has a heartbeat', self.consumer)
                self.consumer._conn.hset(
                    self.consumer._last_seen_key, self.consumer.id,
                    str(time.time()))
                time_before_next_check = self.beat_interval
            time_before_next_check -= dt
            time.sleep(dt)
        log.info('%r is stopped', self)


class MessageConsumer(object):
    """Consumer attached to a MQ queue.

    An object of this class must always be obtained via MQ.consumer.

    """

    HEARTBEAT_INTERVAL = 30.0

    def __init__(self, mq, id=None, start_heartbeat=True):
        self._mq = mq
        self.id = uuid4().hex if id is None else id

        self._pq = '{}:c:{}'.format(self._mq.redis_key_prefix, self.id)
        self._last_seen_key = '{}:c:last_seen'.format(
            self._mq.redis_key_prefix)

        self._redis_lock_name = ':'.join(
            [self._mq._REDIS_KEY_NAME_ROOT, self._mq.name])

        log.info('%r started', self)

    def __repr__(self):
        return '<MessageConsumer {}>'.format(self.id)

    def __enter__(self):
        self.start_heartbeat()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def _lock(self):
        return redis_lock.Lock(
            self._conn, self._redis_lock_name, expire=5, auto_renewal=True)

    @property
    def _conn(self):
        return self._mq._conn

    def start_heartbeat(self):
        self._hb = _HeartBeater(self, self.HEARTBEAT_INTERVAL)
        self._hb.start()

    def stop_heartbeat(self):
        if hasattr(self, '_hb'):
            self._hb.stop()
        self._conn.hdel(self._last_seen_key, self.id)

    @property
    def _last_seen(self):
        try:
            return float(self._conn.hget(self._last_seen_key, self.id))
        except Exception:
            return None

    def is_orphaned(self, idle_time=None):
        if self._last_seen is None:
            return False
        dt = (self.HEARTBEAT_INTERVAL * 2. if idle_time is None else idle_time)
        return bool(time.time() - self._last_seen > dt)

    def cleanup(self):
        log.info('Cleaning up %r', self)
        with self._lock():
            self._flush_processing_queue()
            assert self._conn.llen(self._pq) == 0
            self._conn.delete(self._pq)
            self.stop_heartbeat()
        log.info('Finished cleaning up %r', self)

    def _flush_processing_queue(self):
        for _ in xrange(self._conn.llen(self._pq)):
            self._requeue()

    def _requeue(self, message=None):
        msg = _deserialize(self._conn.lindex(self._pq, -1))
        if message is not None:
            assert message == msg
        queue = self._mq._get_queue_from_message(msg)
        item = self._conn.rpoplpush(self._pq, queue)
        if message is not None:
            assert message == _deserialize(item)
        log.info('%r requeued %r to %r', self, msg, queue)

    def _remove(self, message):
        msg = self._conn.rpop(self._pq)
        assert message == _deserialize(msg)
        log.info('%r removed %r', self, message)

    def _is_processing(self, message):
        for _ in xrange(self._conn.llen(self._pq)):
            msg = _deserialize(self._conn.lindex(self._pq, -1))
            if msg == message:
                return True
            self._conn.rpoplpush(self._pq, self._pq)
        return False

    def get(self, block=True, timeout=None):
        """Remove and return a message from the queue.

        If `block` is :obj:`True` and `timeout` is :obj:`None` (the
        default), block until a message is available. If timeout is a
        positive number, it blocks at most timeout seconds and raises
        :exception:`QueueEmpty` if no message was available within
        that time. Otherwise (i.e., `block` is :obj:`False`), return a
        message if one is immediately available, else raise
        :exception:`QueueEmpty` (timeout is ignored in that case).

        """
        t_called = time.time()
        while 1:
            for queue in self._mq._queues:
                with self._lock():
                    packed = self._conn.rpoplpush(queue, self._pq)
                time.sleep(0)
                if packed is not None:
                    message = _deserialize(packed)
                    log.info('%r got %r', self, message)
                    return message
            else:
                if block:
                    if timeout is None:
                        # yield to other thread, but keep blocking
                        time.sleep(0)
                        continue
                    if time.time() - t_called > timeout:
                        # timeout has expired
                        raise QueueEmpty()
                else:
                    raise QueueEmpty()

    def ack(self, message):
        with self._lock():
            if self._is_processing(message):
                log.info('%r acked and removed %r', self, message)
                self._remove(message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))

    def reject(self, message, requeue=False):
        with self._lock():
            if self._is_processing(message):
                if requeue:
                    log.info('%r rejected and requeued %r', self, message)
                    self._requeue(message)
                else:
                    log.info('%r rejected and removed %r', self, message)
                    self._remove(message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))
