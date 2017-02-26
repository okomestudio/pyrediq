# -*- coding: utf-8 -*-
#
# The MIT License (MIT)
# Copyright (c) 2016, 2017 Taro Sato
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""priority_queue
=================

Priority message queue with Redis


PriorityQueue
-------------

.. autoclass:: PriorityQueue
   :members:


MessageConsumer
---------------

.. autoclass:: MessageConsumer
   :members:


QueueEmpty
----------

.. autoclass:: QueueEmpty
   :members:


Message
-------

.. autoclass:: Message
   :members:


Packed
------

.. autoclass:: Packed
   :members:

"""
from __future__ import absolute_import
import atexit
import logging
import re
import threading
import time
from Queue import Queue
from uuid import uuid4

import msgpack
from redis import StrictRedis
from redis.lock import LockError as RedisLockError


log = logging.getLogger(__name__)


class QueueEmpty(Exception):
    """The exception raised when a priority queue is empty."""


class ConsumerLocked(Exception):
    """Raised when a consumer to be created already exists elsewhere."""


class Message(object):
    """The unpacked representation of a message exchanged through the
    priority queue.

    :type payload: :mod:`msgpack` serializable
    :param payload: The message payload which must be serializable by
        :mod:`msgpack`.

    :type priority: :class:`int`
    :param priority: The priority of the message.

    :param _id: Do not use; this is only for internal use.

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
        """Serialize the message into its packed represenation.

        :rtype: :class:`Packed`
        :returns: The packed representation of the message.

        """
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
        """Serialize a :class:`Message` object to its :class:`Packed`
        representation.

        :type message: :class:`Message`
        :param message: The message to be packed.

        :rtype: :class:`Packed`
        :returns: The packed representation of the message.

        """
        return cls(message.id +
                   cls._priority_to_binary(message.priority) +
                   msgpack.packb(message.payload))

    def deserialize(self):
        """Deserialize :class:`Packed` object to a :class:`Message`.

        :rtype: :class:`Message`
        :returns: The unpacked representation of the object.

        """
        log.debug('Deserializing %r', self)
        message_id = self.get_message_id()
        priority = self.get_priority()
        log.debug('packed: %r', self)
        payload = msgpack.unpackb(self[33:])
        return Message(payload=payload, priority=priority, _id=message_id)

    def get_message_id(self):
        """Get the message ID from the packed representation.

        :rtype: :class:`str`
        :returns: The message ID.

        """
        return str(self[:32])

    def get_priority(self):
        """Get the message priority from the packed representation.

        :rtype: :class:`int`
        :returns: The message priority.

        """
        return self._binary_to_priority(self[32])


_RE_QUEUE_NAME = re.compile(r'^[a-zA-Z0-9_-]+$')


class PriorityQueue(object):
    """The priority queue implementation using multiple Redis lists.

    :type name: :class:`str`
    :param name: The queue name.

    :type redis_conn: :class:`redis.StrictRedis`
    :param redis_conn: The Redis connection client. If not provided, a
        new :class:`~redis.StrictRedis` client is created without any
        arguments.

    """

    #: The root prefix used for all redis keys related to pyrediq
    #: queues.
    _REDIS_KEY_NAME_ROOT = '__pyrediq'

    #: The minimum priority allowed. (This cannot be changed.)
    MIN_PRIORITY = -8

    #: The maximum priority allowed. (This cannot be changed.)
    MAX_PRIORITY = +7

    def __init__(self, name, redis_conn=None):
        if not _RE_QUEUE_NAME.match(name):
            raise ValueError(
                "Invalid queue name (must consist of alphanumeric characters"
                " or '-'/'_')")
        self._name = name

        if redis_conn is None:
            redis_conn = StrictRedis()
        elif not isinstance(redis_conn, StrictRedis):
            raise ValueError('`redis_conn` is a StrictRedis instance')
        self._conn = redis_conn

        self._queues = [self._get_internal_queue(i) for i
                        in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1)]

        orphan_consumer_cleaner.watch(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self):
        n = sum(self._conn.llen(self._get_internal_queue(pri)) for pri
                in xrange(self.MIN_PRIORITY, self.MAX_PRIORITY + 1))
        for consumer_id in self._get_consumer_ids():
            n += self._conn.llen(self._redis_key_prefix + ':c:' + consumer_id)
        return n

    def close(self):
        """Close the connection to the queue."""
        orphan_consumer_cleaner.unwatch(self)

    def _get_consumer_ids(self):
        """Get the IDs of all consumers associated with this queue.

        :rtype: generator of :class:`str`
        :returns: The IDs of consumers.

        """
        consumer_ids = set()
        for k in self._conn.keys(self._redis_key_prefix + ':c:*'):
            consumer_ids.add(k.split(':')[-1 - int(k.endswith(':lock'))])
        for id in consumer_ids:
            yield id

    @property
    def name(self):
        """The name of the priority queue."""
        return self._name

    @property
    def _redis_key_prefix(self):
        """The redis key prefix used for the priority queue."""
        return '{}:{}'.format(self._REDIS_KEY_NAME_ROOT, self.name)

    def _get_internal_queue(self, priority):
        """Get the internal queue for the specified priority.

        :type priority: :class:`int`
        :param priority: The priority for which the internal queue is
            obtained.

        :rtype: :class:`str`
        :returns: The redis key.

        """
        return '{}:p:{:+d}'.format(self._redis_key_prefix, priority)

    def size(self):
        """The number of messages in the priority queue. The message count
        includes the messages currently processed by active consumers.

        :rtype: :class:`int`
        :returns: The message count.

        """
        return len(self)

    def is_empty(self):
        """Test if the priority queue is empty. The message count used for the
        check includes the messages currently processed by active
        consumers.

        :rtype: :class:`bool`
        :returns: :obj:`True` if empty, :obj:`False` if not empty.

        """
        return self.size() == 0

    def purge(self):
        """Purge the priority queue.

        :raises: :class:`RuntimeError` when consumers are active.

        """
        try:
            for consumer_id in self._get_consumer_ids():
                consumer = MessageConsumer(self, _id=consumer_id)
                consumer.cleanup()
        except ConsumerLocked:
            raise RuntimeError('Some consumers are still active')

        log.warning('Purging %r...', self)
        keys = self._conn.keys(self._redis_key_prefix + ':*')
        if keys:
            log.debug('Redis keys to be removed: %s', ' '.join(keys))
            self._conn.delete(*keys)
        log.warning('%r is purged', self)

    def put(self, payload, priority=0):
        """Create a new message in the priority queue.

        :type payload: :mod:`msgpack` serializable
        :param payload: The payload for a new message.

        :type priority: :class:`int`
        :param priority: The priority for the new message. It must be
            in the range of [-8, 7], inclusive, and a value outside
            the range will be capped to the min/max.

        :rtype: :class:`str`
        :returns: The ID of the created message.

        """
        priority = max(self.MIN_PRIORITY,
                       min(priority, self.MAX_PRIORITY))
        message = Message(payload=payload, priority=priority)
        queue = self._get_internal_queue(message.priority)
        self._conn.rpush(queue, message.serialize())
        log.info('%r puts %r', self, message)
        return message.id

    def consumer(self):
        """Get a message consumer for the priority queue. This method should
        be used with a context manager (i.e., the `with` statement) so
        that the appropriate consumer cleanup action gets run once the
        consumer is no longer needed.

        :rtype: :class:`MessageConsumer`
        :returns: The message consumer of the priority queue.

        """
        return MessageConsumer(self)


class MessageConsumer(object):
    """The consumer of messages from :class:`PriorityQueue`.

    An object of this type is obtained via
    :meth:`PriorityQueue.consumer`.

    :type queue: :class:`PriorityQueue`
    :param queue: The queue from which the consumer consumes messages.

    """

    def __init__(self, queue, _id=None):
        self._queue = queue
        self._id = _id or uuid4().hex

        # Redis hash hodling the messages that have been consumed but
        # have not been acked/rejected
        self._consumed = '{}:c:{}'.format(
            self._queue._redis_key_prefix, self._id)

        self._lock_lifetime = 5.0
        self._lock = self._conn.lock(
            self._consumed + ':lock', timeout=self._lock_lifetime,
            thread_local=False)

        self._beat_interval = 1.0
        self._beat_time = time.time()
        if not self._lock.acquire(blocking=False):
            raise ConsumerLocked('Consumer {} already locked'.format(self._id))

        log.info('%r started', self)

        self._critical = threading.Lock()
        self._beat()

    def _beat(self):
        with self._critical:
            self._beat_is_scheduled = False

            now = time.time()
            extend_by = now - self._beat_time
            self._beat_time = now

            try:
                self._lock.extend(extend_by)
            except RedisLockError:
                log.exception('%r failed to extend Redis lock', self)
                if not self._lock.acquire(blocking=False):
                    raise ConsumerLocked(
                        'Consumer {} locked after extension failure'.format(
                            self._id))
                self._beat_time = time.time()
                log.warning('%r reclaimed lock', self)
            else:
                log.debug('%r is alive at %r, extended lock by %f',
                          self, self._beat_time, extend_by)

            self._schedule_beat()

    def _schedule_beat(self):
        if not self._beat_is_scheduled:
            t = self._beat_time + self._beat_interval
            self._beater = threading.Timer(t - time.time(), self._beat)
            self._beater.daemon = True
            self._beater.start()
            self._beat_is_scheduled = True

    def _stop_beat(self):
        self._beater.cancel()
        self._beat_is_scheduled = False
        self._lock.release()

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
        """Clean up the consumer. This means all the messages that have been
        consumed but not acked/rejected are requeued back to the
        priority queue.

        """
        log.info('Cleaning up %r', self)
        with self._critical:
            self._requeue_all()
            self._conn.delete(self._consumed)
            self._stop_beat()
        log.info('Finished cleaning up %r', self)

    def _requeue_critical(self, packed):
        message_id = packed.get_message_id()
        priority = packed.get_priority()
        queue = self._queue._get_internal_queue(priority)
        try:
            self._conn.rpush(queue, packed)
        except Exception:
            # TODO: one thing that could be added back is to requeue
            # orphaned consumed messages like it used to be done. for
            # now, let's see how this work
            log.error('Pushing to Redis queue failed; '
                      'unacked/unrejected may be lost!')
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

        :type message: :class:`Message`
        :param message: The message to be requeued.

        """
        message_id = message.id
        packed = self._conn.hget(self._consumed, message_id)
        self._requeue_critical(Packed(packed))

    def _remove(self, message):
        """Remove the message.

        :type message: :class:`Message`
        :param message: The message to be removed.

        """
        resp = self._conn.hdel(self._consumed, message.id)
        if resp != 1:
            raise AssertionError(
                '{!r} could not be removed from processing queue ({})'.format(
                    message, resp))

    def get(self, block=True, timeout=None):
        """Consume a message from the priority queue.

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
        resp = self._conn.blpop(self._queue._queues, timeout)
        if resp is None:
            raise QueueEmpty()
        self._critical.acquire()
        try:
            packed = Packed(resp[1])
            message = packed.deserialize()
            self._conn.hset(self._consumed, message.id, packed)
        except Exception:
            log.exception('Problem getting a message... rolling back')
            priority = packed.get_priority()
            queue = self._queue._get_internal_queue(priority)
            self._conn.rpush(queue, packed)
            raise
        finally:
            self._critical.release()
        log.info('%r got %r', self, message)
        return message

    def ack(self, message):
        """Ack the message and remove from the priority queue.

        :type message: :class:`Message`
        :param message: The message to ack.

        """
        with self._critical:
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
        with self._critical:
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


class ConsumerCleanerWorker(threading.Thread):

    def __init__(self, tasks):
        super(ConsumerCleanerWorker, self).__init__()
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while 1:
            queue, consumer_id = self.tasks.get()
            try:
                log.debug('%r cleaning up orphaned consumer %s',
                          self, consumer_id)
                consumer = MessageConsumer(queue, _id=consumer_id)

            except ConsumerLocked:
                log.debug('%r sees consumer %s is in use', self, consumer_id)

            else:
                consumer.cleanup()
                log.info('%r cleaned up orphaned consumer %s',
                         self, consumer_id)

            finally:
                self.tasks.task_done()


class OrphanConsumerCleaner(threading.Thread):
    """A thread periodically cleans up orphaned consumer artifacts.

    :type num_worker: :class:`int`
    :param num_worker: Number of workers tasked with clean-up.

    """

    #: Time interval in seconds between clean-up jobs
    _interval = 5

    def __init__(self, num_worker=64):
        super(OrphanConsumerCleaner, self).__init__()

        # PriorityQueue's to be watched
        self.queues = set()

        # Start worker threads
        self._tasks = Queue(num_worker)
        for _ in xrange(num_worker):
            ConsumerCleanerWorker(self._tasks)

        # For scheduled task
        self._timer = None
        self._is_running = False

        # Start as a daemon
        self.daemon = True
        self.start()

    def watch(self, queue):
        self.queues.add(queue)

    def unwatch(self, queue):
        self.queues.remove(queue)

    def run(self):
        self._run_scheduled()

    def _do_cleaning(self):
        for queue in self.queues:
            for consumer_id in queue._get_consumer_ids():
                self._tasks.put((queue, consumer_id))
        self._tasks.join()

    def _run_scheduled(self):
        self._is_running = False
        self._schedule()
        self._do_cleaning()

    def _schedule(self):
        if not self._is_running:
            self._timer = threading.Timer(self._interval, self._run_scheduled)
            self._timer.daemon = True
            self._timer.start()
            self._is_running = True

    def stop(self):
        if self._timer is not None:
            self._timer.cancel()
        self._is_running = False


orphan_consumer_cleaner = OrphanConsumerCleaner()


@atexit.register
def _cleanup():
    orphan_consumer_cleaner.stop()
