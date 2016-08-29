# -*- coding: utf-8 -*-
"""Priority message queue with Redis

"""
from __future__ import absolute_import
import logging
import time
from uuid import uuid4

import msgpack
import redis_lock
from redis import StrictRedis


log = logging.getLogger(__name__)


class QueueEmpty(Exception):
    pass


class Message(object):
    """Message to be passed around.

    """

    def __init__(self, payload=None, priority=0, _id=None):
        if _id is None:
            _id = uuid4().hex
        self.payload = payload
        assert -8 <= priority <= 7, 'Priority must be int within [-8, 7]'
        self.priority = priority
        assert isinstance(_id, str) and len(_id) == 32
        self.id = _id

    def __repr__(self):
        return ('Message(payload={payload}, priority={priority}, '
                '_id={_id}').format(payload=self.payload,
                                    priority=self.priority,
                                    _id=self.id)

    def __eq__(self, other):
        return self.id == other.id


class Serializer(object):

    @staticmethod
    def _priority_to_binary(priority):
        assert -8 <= priority <= 7
        return str(bytearray([priority + 256 if priority < 0 else priority]))

    @staticmethod
    def _binary_to_priority(binary):
        x = int(binary.encode('hex'), 16)
        return x if x < 128 else x - 256

    @classmethod
    def serialize(cls, message):
        return (message.id +
                cls._priority_to_binary(message.priority) +
                msgpack.packb(message.payload))

    @classmethod
    def deserialize(cls, packed):
        log.debug('Deserializing %r', packed)
        message_id = cls.get_message_id_from_packed(packed)
        priority = cls.get_priority_from_packed(packed)
        log.debug('packed: %r', packed)
        payload = msgpack.unpackb(packed[33:])
        return Message(payload=payload, priority=priority, _id=message_id)

    @staticmethod
    def get_message_id_from_packed(packed):
        return str(packed[:32])

    @classmethod
    def get_priority_from_packed(cls, packed):
        return cls._binary_to_priority(packed[32])


class PyRediQ(object):
    """Priority queue implementation using multiple Redis lists."""

    _REDIS_KEY_NAME_ROOT = '__PyRediQ'

    MIN_PRIORITY = -8
    MAX_PRIORITY = +7

    def __init__(self, name, redis_conn=None):
        if redis_conn is None:
            redis_conn = StrictRedis()
        elif not isinstance(redis_conn, StrictRedis):
            raise ValueError('`redis_conn` is a StrictRedis instance')
        self._conn = redis_conn

        self.name = name

        self._queues = [self._get_queue_name(i) for i
                        in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1)]

        self.consumers = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __len__(self):
        n = 0
        for pri in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1):
            n += self._conn.llen(self._get_queue_name(pri))
        return n

    @property
    def redis_conn(self):
        return self._conn

    @property
    def redis_key_prefix(self):
        return '{}:{}'.format(self._REDIS_KEY_NAME_ROOT, self.name)

    def _get_queue_name(self, priority):
        return '{}:p:{:+d}'.format(self.redis_key_prefix, priority)

    def is_empty(self):
        qs = self._conn.keys(self.redis_key_prefix + ':p:*')
        log.debug('Testing for queue emptiness found: %r', qs)
        return not bool(qs)

    def purge(self):
        """Purge queue.

        WARNING: Before running this, make sure no threads/processes
        are running using this particular queue. Purging at a wrong
        timing may leave the info on Redis to be in a broken state.

        """
        log.warning('Purging %r...', self)
        keys = self._conn.keys(self.redis_key_prefix + ':*')
        if keys:
            log.debug('Redis keys to be removed: %s', ' '.join(keys))
            self._conn.delete(*keys)
        log.warning('%r is purged', self)

    def _get_queue_from_message(self, message):
        pri = max(self.MIN_PRIORITY,
                  min(message.priority, self.MAX_PRIORITY))
        return self._get_queue_name(pri)

    def put(self, payload, priority=0, _id=None):
        message = Message(payload=payload, priority=priority, _id=_id)
        queue = self._get_queue_from_message(message)
        log.info('%r puts %r', self, message)
        self._conn.lpush(queue, Serializer.serialize(message))

    def consumer(self):
        return MessageConsumer(self)


class MessageConsumer(object):
    """Consumer attached to a MQ queue.

    An object of this class must always be obtained via MQ.consumer.

    """

    def __init__(self, mq, id=None):
        self._mq = mq
        self.id = uuid4().hex if id is None else id

        # the processing queue in which currently processed (i.e.,
        # consumed but not acked/rejected) messages are stored
        self._pq = '{}:c:{}'.format(self._mq.redis_key_prefix, self.id)

        self._redis_lock_name = ':'.join(
            [self._mq._REDIS_KEY_NAME_ROOT, self._mq.name])

        log.info('%r started', self)

    def __repr__(self):
        return '<MessageConsumer {}>'.format(self.id)

    def __enter__(self):
        self._mq.consumers.add(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        self._mq.consumers.remove(self)

    def _lock(self):
        return redis_lock.Lock(
            self._conn, self._redis_lock_name, expire=5, auto_renewal=True)

    @property
    def _conn(self):
        return self._mq._conn

    def cleanup(self):
        log.info('Cleaning up %r', self)
        with self._lock():
            self._flush_processing_queue()
            assert self._conn.llen(self._pq) == 0
            self._conn.delete(self._pq)
        log.info('Finished cleaning up %r', self)

    def _flush_processing_queue(self):
        for _ in xrange(self._conn.llen(self._pq)):
            self._requeue()

    def _requeue(self):
        """Requeue the message in the processing queue back to the priority
        queue.

        """
        packed = self._conn.lindex(self._pq, -1)
        msg = Serializer.deserialize(packed)
        queue = self._mq._get_queue_from_message(msg)
        packed = self._conn.rpoplpush(self._pq, queue)
        log.info('%r requeued %r to %r', self, msg, queue)

    def _remove(self, message):
        packed = self._conn.rpop(self._pq)
        assert message.id == Serializer.get_message_id_from_packed(packed)
        log.info('%r removed %r', self, message)

    def _is_in_processing_queue(self, message):
        """Test if the message is in the processing queue. If found, the
        message will be placed at the head of processing queue when
        the method exits.

        """
        # TODO: this is very inefficient
        for _ in xrange(self._conn.llen(self._pq)):
            packed = self._conn.lindex(self._pq, -1)
            if message.id == Serializer.get_message_id_from_packed(packed):
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
                if packed is not None:
                    message = Serializer.deserialize(packed)
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
            if self._is_in_processing_queue(message):
                log.info('%r acked and removed %r', self, message)
                self._remove(message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))

    def reject(self, message, requeue=False):
        with self._lock():
            if self._is_in_processing_queue(message):
                if requeue:
                    log.info('%r rejected and requeued %r', self, message)
                    self._requeue()  # message)
                else:
                    log.info('%r rejected and removed %r', self, message)
                    self._remove(message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))
