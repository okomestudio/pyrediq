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
        log.debug('get_priority_from_packed got packed %r', packed)
        return cls._binary_to_priority(packed[32])


class PriorityQueue(object):
    """Priority queue implementation using multiple Redis lists.

    :type name: :class:`str`
    :param name: The queue name.

    :type redis_conn: :class:`redis.StrictRedis` object
    :param redis_conn: The Redis connection client.

    """

    #: The root prefix used for all redis keys related to pyrediq
    #: queues.
    _REDIS_KEY_NAME_ROOT = '__PyRediQ'

    #: The minimum priority allowed.
    MIN_PRIORITY = -8

    #: The maximum priority allowed.
    MAX_PRIORITY = +7

    def __init__(self, name, redis_conn=None):
        if redis_conn is None:
            redis_conn = StrictRedis()
        elif not isinstance(redis_conn, StrictRedis):
            raise ValueError('`redis_conn` is a StrictRedis instance')
        self._conn = redis_conn

        self._name = name

        self._queues = [self._get_queue_name(i) for i
                        in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1)]

        # active consumers
        self.consumers = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __len__(self):
        n = sum(self._conn.llen(self._get_queue_name(pri)) for pri
                in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1))
        n += sum(len(c) for c in self.consumers)
        return n

    @property
    def name(self):
        """The name of the queue."""
        return self._name

    @property
    def redis_conn(self):
        """The redis connection client."""
        return self._conn

    @property
    def redis_key_prefix(self):
        """The redis key prefix used for the queue."""
        return '{}:{}'.format(self._REDIS_KEY_NAME_ROOT, self.name)

    def _get_queue_name(self, priority):
        """Get the queue for the specified priority.

        :type priority: :class:`int`
        :param priority: The priority for which the internal queue is
            obtained.

        :rtype: :class:`str`
        :returns: The redis key.

        """
        return '{}:p:{:+d}'.format(self.redis_key_prefix, priority)

    def _get_queue_from_message(self, message):
        """Given a message, get its priority.

        :type message: :class:`Message` object
        :param message: The message for which priority is obtained.

        :rtype: :class:`int`
        :returns: The priority of the message.

        """
        pri = max(self.MIN_PRIORITY,
                  min(message.priority, self.MAX_PRIORITY))
        return self._get_queue_name(pri)

    def is_empty(self):
        """Test if the queue is empty.

        :rtype: :class:`bool`
        :returns: :obj:`True` if empty, :obj:`False` if not empty.

        """
        # qs = self._conn.keys(self.redis_key_prefix + ':p:*')
        # log.debug('Testing for queue emptiness found: %r', qs)
        return len(self) == 0

    def purge(self):
        """Purge the queue.

        :raises: :exception:`RuntimeError` when consumers are active.

        """
        if self.consumers:
            raise RuntimeError('Some consumers are still active')
        log.warning('Purging %r...', self)
        keys = self._conn.keys(self.redis_key_prefix + ':*')
        if keys:
            log.debug('Redis keys to be removed: %s', ' '.join(keys))
            self._conn.delete(*keys)
        log.warning('%r is purged', self)

    def put(self, payload, priority=0):
        """Create a message in the queue.

        :param payload: The payload for the new message.

        :type priority: :class:`int`
        :param priority: The priority for the new message.

        """
        message = Message(payload=payload, priority=priority)
        queue = self._get_queue_from_message(message)
        log.info('%r puts %r', self, message)
        self._conn.lpush(queue, Serializer.serialize(message))

    def consumer(self):
        """Get a message consumer for the queue.

        :rtype: :class:`MessageConsumer` object
        :returns: The message consumer for the queue.

        """
        return MessageConsumer(self)


class MessageConsumer(object):
    """Consumer of :class:`PriorityQueue`.

    An object of this class is obtained via
    :meth:`PriorityQueue.consumer`.

    :type queue: :class:`PriorityQueue` object
    :param queue: The queue from which the consumer consumes messages.

    """

    def __init__(self, queue):
        self._queue = queue

        self._id = uuid4().hex

        # the processing queue in which currently processed (i.e.,
        # consumed but not acked/rejected) messages by the consumer
        # are stored
        self._pq = '{}:c:{}'.format(
            self._queue.redis_key_prefix, self._id)

        self._lock = threading.Lock()

        # self._redis_lock_name = '{}:{}'.format(
        #     self._queue._REDIS_KEY_NAME_ROOT,
        #     self._queue.name)

        log.info('%r started', self)

    def __repr__(self):
        return '<MessageConsumer {}>'.format(self.id)

    def __len__(self):
        return self._conn.hlen(self._pq)

    def __enter__(self):
        self._queue.consumers.add(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        self._queue.consumers.remove(self)

    @property
    def id(self):
        """The identifier which uniquely identifies the consumer."""
        return self._id

    @property
    def _conn(self):
        return self._queue._conn

    def cleanup(self):
        log.info('Cleaning up %r', self)
        with self._lock:
            self._flush_processing_queue()
            self._conn.delete(self._pq)
        log.info('Finished cleaning up %r', self)

    def _flush_processing_queue(self):
        cursor = 0
        while 1:
            cursor, resp = self._conn.hscan(self._pq, cursor)

            for field, packed in resp.iteritems():
                # log.debug('field:%r packed:%r', field, packed)
                msg = Serializer.deserialize(packed)
                queue = self._queue._get_queue_from_message(msg)
                self._conn.lpush(queue, packed)
                self._conn.hdel(self._pq, field)
                log.info('%r requeued %r to %r', self, msg, queue)

            if cursor == 0:
                break

    def _requeue(self, message):
        """Requeue the last message in the processing queue back to the
        priority queue.

        """
        packed = self._conn.hget(self._pq, message.id)
        if packed is None:
            raise Exception('Message {!r} is not found'.format(message))
        queue = self._queue._get_queue_from_message(message)
        self._conn.lpush(queue, packed)
        self._conn.hdel(self._pq, message.id)
        log.info('%r requeued %r to %r', self, message, queue)

    def _remove(self, message):
        """Remove the last message from the processing queue.

        :rtype: :class:`str`
        :returns: The packed form of message removed from the
            processing queue.

        """
        resp = self._conn.hdel(self._pq, message.id)
        if resp != 1:
            raise ValueError(
                '{!r} could not be removed from processing queue', message)

    def _push_to_last_of_processing_queue(self, message):
        """Push the message to the last of the processing queue.

        :type message: :class:`Message`
        :param message: The message to push to the last of the
            processing queue.

        :rtype: :class:`bool`
        :returns: :obj:`True` if the message is successfully pushed to
        the last of processing queue; :obj:`False` otherwise.

        """
        try:
            return self._conn.hexists(self._pq, message.id)
        except:
            log.exception('Invalid message detected %r', message)
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
        timeout = (timeout or 0) if block else 1

        resp = self._conn.brpop(self._queue._queues, timeout)
        if resp is None:
            raise QueueEmpty()

        _, packed = resp

        message = Serializer.deserialize(packed)
        self._conn.hset(self._pq, message.id, packed)
        log.info('%r got %r', self, message)
        return message

    def ack(self, message):
        with self._lock:
            if self._push_to_last_of_processing_queue(message):
                log.info('%r acked and removed %r', self, message)
                self._remove(message)
                log.info('%r removed %r', self, message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))

    def reject(self, message, requeue=False):
        with self._lock:
            if self._push_to_last_of_processing_queue(message):
                if requeue:
                    log.info('%r rejected and requeued %r', self, message)
                    self._requeue(message)
                else:
                    log.info('%r rejected and removed %r', self, message)
                    self._remove(message)
                    log.info('%r removed %r', self, message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))
