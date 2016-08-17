#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import gevent.monkey ; gevent.monkey.patch_all()

import logging
import random
import unittest

import gevent
import redis

from pyrediq.mq import MQ, MQConsumer, QueueEmpty


class GEventTester(unittest.TestCase):

    # someone needs to write tests for testing these tests sort of
    # tests for now.

    def setUp(self):
        # logging.basicConfig(level=logging.DEBUG)
        self.redis = redis.StrictRedis()

    def tearDown(self):
        pass

    def test_multiple_consumers(self):
        queue_name = 'test_multiple_consumers'
        mq = MQ(queue_name, self.redis)

        def consume(mq):
            with mq.consumer() as consumer:
                self.consumer(consumer)

        gs = []
        for i in xrange(5):
            gs.append(gevent.spawn(self.producer, mq, i, 20))
        for _ in xrange(5):
            gs.append(gevent.spawn(consume, mq))

        try:
            gevent.joinall(gs)
        except Exception:
            [g.kill() for g in gs]

        self.assertTrue(mq.is_empty())
        mq.purge()

    def test_orphan_consumers(self):
        queue_name = 'test_orphan_consumers'
        mq = MQ(queue_name, self.redis)

        def consume(mq, bomb=False):
            def ticking_time_bomb():
                raise KeyboardInterrupt
            if bomb:
                gevent.spawn_later(2., ticking_time_bomb)

            # this consumer does not do clean up action
            consumer = MQConsumer(mq)
            consumer.start_heartbeat()
            self.consumer(consumer)

        gs = []
        for i in xrange(1):
            gs.append(gevent.spawn(self.producer, mq, i, 100))

        n_consumer = 3
        for i in xrange(n_consumer):
            gs.append(gevent.spawn(consume, mq, bomb=(i == 0)))

        # this should leave some consumer queues abandoned
        with self.assertRaises(KeyboardInterrupt):
            gevent.joinall(gs)
        self.assertGreater(len(mq._orphaned_consumers(0)), 0)

        gevent.killall(gs)
        self.assertGreater(len(mq._orphaned_consumers(0)), 0)

        mq = MQ(queue_name, self.redis)
        # this should trigger clean-up and put all items in processing
        # queue back to the priority queue
        mq._clean_up_orphans(0.0)
        self.assertEqual(len(mq._orphaned_consumers(0)), 0)

        mq.purge()

    def test_orphan_clean_up(self):
        queue_name = 'test_orphan_clean_up'
        mq = MQ(queue_name, self.redis)

        def consume(mq, bomb=False):
            def ticking_time_bomb():
                raise KeyboardInterrupt
            if bomb:
                gevent.spawn_later(0.1, ticking_time_bomb)
            with mq.consumer() as consumer:
                self.consumer(consumer)

        gs = []
        for i in xrange(1):
            gs.append(gevent.spawn(self.producer, mq, i, 100))

        n_consumer = 3
        for i in xrange(n_consumer):
            gs.append(gevent.spawn(consume, mq, bomb=(i == 0)))

        # this should leave some consumer queues abandoned
        with self.assertRaises(KeyboardInterrupt):
            gevent.joinall(gs)
        self.assertGreater(len(mq._orphaned_consumers(0)), 0)

        # killing greenlets trigger their consumer cleanup action, so
        # after that there should be no orphaned consumers. but
        gevent.killall(gs)

        self.assertEqual(len(mq._orphaned_consumers(0)), 0)
        mq.purge()

    def test_single_consumer(self):
        queue_name = 'test_single_consumer'
        mq = MQ(queue_name, self.redis)

        gs = []
        for i in xrange(5):
            gs.append(gevent.spawn(self.producer, mq, i, 10))
        with mq.consumer() as consumer:
            for _ in xrange(10):
                gs.append(gevent.spawn(self.consumer, consumer))

        try:
            gevent.joinall(gs)
        except Exception:
            [g.kill() for g in gs]

        self.assertTrue(mq.is_empty())
        mq.purge()

    @staticmethod
    def producer(mq, i, n):
        for j in xrange(n):
            mq.put(payload='message {}'.format(repr((i, j))),
                   priority=random.randint(mq.min_priority, mq.max_priority))
            gevent.sleep(random.random() * 0.1)

    @staticmethod
    def consumer(consumer):
        is_alive = True
        while is_alive:
            msgs = []
            for _ in xrange(random.randint(1, 5)):
                try:
                    msg = consumer.get(block=True, timeout=0.5)
                    logging.info('{!r} got {!r}'.format(consumer, msg))
                    msgs.append(msg)
                except QueueEmpty:
                    logging.info('{!r} queue empty'.format(consumer))
                    is_alive = False
                    break

            # doing task
            for msg in msgs:
                gevent.sleep(random.random() * 0.1)
                if random.random() < 0.1:
                    consumer.reject(msg)
                else:
                    consumer.ack(msg)

        logging.info('consume for {!r} finished'.format(consumer))


if __name__ == '__main__':
    unittest.main()
