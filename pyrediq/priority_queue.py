# -*- coding: utf-8 -*-
"""Priority message queue with Redis

"""
from __future__ import absolute_import
import contextlib
import logging
import threading
from uuid import uuid4

import msgpack
from redis import StrictRedis


log = logging.getLogger(__name__)


class QueueEmpty(Exception):
    """Raised when the priority queue is empty."""


class Message(object):
    """The unpacked representation of a message exchanged through priority
    queue.

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

    def serialize(self):
        """Serialize the message into its packed represenation."""
        return Packed.serialize(self)


class Packed(str):
    """The packed representation of a message. The :class:`str`-casted
    value is stored in Redis.

    """

    def __new__(cls, packed):
        return str.__new__(cls, packed)

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
        return cls(message.id +
                   cls._priority_to_binary(message.priority) +
                   msgpack.packb(message.payload))

    def deserialize(self):
        log.debug('Deserializing %r', self)
        message_id = self.get_message_id()
        priority = self.get_priority()
        log.debug('packed: %r', self)
        payload = msgpack.unpackb(self[33:])
        return Message(payload=payload, priority=priority, _id=message_id)

    def get_message_id(self):
        return str(self[:32])

    def get_priority(self):
        return self._binary_to_priority(self[32])


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

        self._queues = [self._get_internal_queue(i) for i
                        in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1)]

        # active consumers
        self.consumers = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __len__(self):
        n = sum(self._conn.llen(self._get_internal_queue(pri)) for pri
                in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1))
        n += sum(len(c) for c in self.consumers)
        return n

    @property
    def name(self):
        """The name of the queue."""
        return self._name

    @property
    def _redis_key_prefix(self):
        """The redis key prefix used for the queue."""
        return '{}:{}'.format(self._REDIS_KEY_NAME_ROOT, self.name)

    def _get_internal_queue(self, priority):
        """Get the queue for the specified priority.

        :type priority: :class:`int`
        :param priority: The priority for which the internal queue is
            obtained.

        :rtype: :class:`str`
        :returns: The redis key.

        """
        return '{}:p:{:+d}'.format(self._redis_key_prefix, priority)

    def size(self):
        """The number of messages in the priority queue.

        :rtype: :class:`int`

        """
        return len(self)

    def is_empty(self):
        """Test if the queue is empty.

        :rtype: :class:`bool`
        :returns: :obj:`True` if empty, :obj:`False` if not empty.

        """
        return self.size() == 0

    def purge(self):
        """Purge the queue.

        :raises: :class:`RuntimeError` when consumers are active.

        """
        if self.consumers:
            raise RuntimeError('Some consumers are still active')
        log.warning('Purging %r...', self)
        keys = self._conn.keys(self._redis_key_prefix + ':*')
        if keys:
            log.debug('Redis keys to be removed: %s', ' '.join(keys))
            self._conn.delete(*keys)
        log.warning('%r is purged', self)

    def put(self, payload, priority=0):
        """Create a message in the queue.

        :param payload: The payload for the new message.

        :type priority: :class:`int`
        :param priority: The priority for the new message. It must be
            in the range of [-8, 7], inclusive, and a value outside
            the range will be capped.

        """
        priority = max(self.MIN_PRIORITY,
                       min(priority, self.MAX_PRIORITY))
        message = Message(payload=payload, priority=priority)
        queue = self._get_internal_queue(message.priority)
        self._conn.lpush(queue, message.serialize())
        log.info('%r puts %r', self, message)

    @contextlib.contextmanager
    def consumer(self):
        """Get a message consumer for the queue. This method should be used
        with a context manager (i.e., the `with` statement) so that
        the appropriate consumer cleanup action gets run once the
        consumer is no longer needed.

        :rtype: :class:`MessageConsumer` object
        :returns: The message consumer for the queue.

        """
        with MessageConsumer(self) as consumer:
            self.consumers.add(consumer)
            try:
                yield consumer
            finally:
                self.consumers.remove(consumer)


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
        self._lock = threading.Lock()

        # redis hash hodling the messages that have been consumed but
        # have not been acked/rejected
        self._consumed = '{}:c:{}'.format(
            self._queue._redis_key_prefix, self._id)

        log.info('%r started', self)

    def __repr__(self):
        return '<MessageConsumer {}>'.format(self.id)

    def __len__(self):
        return self._conn.hlen(self._consumed)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    @property
    def id(self):
        """The identifier which uniquely identifies the consumer."""
        return self._id

    @property
    def _conn(self):
        """The Redis connection client."""
        return self._queue._conn

    def cleanup(self):
        log.info('Cleaning up %r', self)
        with self._lock:
            self._requeue_all()
            self._conn.delete(self._consumed)
        log.info('Finished cleaning up %r', self)

    def _requeue_critical(self, packed):
        message_id = packed.get_message_id()
        priority = packed.get_priority()
        queue = self._queue._get_internal_queue(priority)
        try:
            self._conn.lpush(queue, packed)
        except Exception:
            raise
        else:
            self._conn.hdel(self._consumed, message_id)
            log.info('%r requeued message %s to %r', self, message_id, queue)

    def _requeue_all(self):
        """Requeue all consumed messages back to the priority queue."""
        cursor = 0
        while 1:
            cursor, resp = self._conn.hscan(self._consumed, cursor)
            for message_id, packed in resp.iteritems():
                self._requeue_critical(Packed(packed))
            if cursor == 0:
                break

    def _requeue(self, message):
        """Requeue the message back to the priority queue.

        :type message: :class:`Message` object
        :param message: The message to be requeued.

        """
        message_id = message.id
        packed = self._conn.hget(self._consumed, message_id)
        self._requeue_critical(Packed(packed))

    def _remove(self, message):
        """Remove the message.

        :type message: :class:`Message` object
        :param message: The message to be removed.

        :rtype: :class:`str`
        :returns: The packed form of message removed from the
            processing queue.

        """
        resp = self._conn.hdel(self._consumed, message.id)
        if resp != 1:
            raise AssertionError(
                '{!r} could not be removed from processing queue', message)

    def get(self, block=True, timeout=None):
        """Get a message from the priority queue.

        :type block: :class:`bool`
        :param block: If :obj:`True` and `timeout` is :obj:`None` (the
            default), block until a message is available. If timeout
            is a positive integer, it blocks at most timeout seconds
            and raises :class:`QueueEmpty` if no message was
            available within that time. Otherwise (i.e., `block` is
            :obj:`False`), return a message if one is immediately
            available, else raise :class:`QueueEmpty` (timeout is
            ignored in that case).

        :type timeout: :class:`int` or :obj:`None`
        :param timeout: The timeout in seconds. Note that due to a
            limitation of `brpop` command of Redis, a fractional
            timeout cannot be specified, so the shortest timeout is
            one second.

        """
        timeout = (timeout or 0) if block else 1

        resp = self._conn.brpop(self._queue._queues, timeout)
        if resp is None:
            raise QueueEmpty()
        try:
            packed = Packed(resp[1])
            message = packed.deserialize()
            self._conn.hset(self._consumed, message.id, packed)
        except Exception:
            log.exception('Problem getting a message... rolling back')
            priority = packed.get_priority()
            queue = self._queue._get_internal_queue(priority)
            self._conn.lpush(queue, packed)
            raise
        log.info('%r got %r', self, message)
        return message

    def ack(self, message):
        """Ack the message and remove from the priority queue.

        :type message: :class:`Message`
        :param message: The message to ack.

        """
        with self._lock:
            if self._conn.hexists(self._consumed, message.id):
                self._remove(message)
                log.info('%r acked and removed %r', self, message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))

    def reject(self, message, requeue=False):
        """Reject the message. When `requeue` is :obj:`True`, the message will
        be requeued back to the priority queue. Otherwise
        (:obj:`False`, which is the default), it will be removed.

        :type message: :class:`Message`
        :param message: The message to reject.

        :type requeue: :class:`bool`
        :param requeue: Whether to requeue the rejected message or not.

        """
        with self._lock:
            if self._conn.hexists(self._consumed, message.id):
                if requeue:
                    self._requeue(message)
                    log.info('%r rejected and requeued %r', self, message)
                else:
                    self._remove(message)
                    log.info('%r rejected and removed %r', self, message)
            else:
                raise ValueError(
                    '{!r} did not find {!r}'.format(self, message))
