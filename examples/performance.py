#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
import gevent.monkey; gevent.monkey.patch_all()

import functools
import logging
import random
import time

import gevent.pool
from pyrediq import PriorityQueue
from pyrediq import QueueEmpty


logging.basicConfig(
    #level='WARNING',
    level='DEBUG',
    format=('%(asctime)s.%(msecs)03d '
            '%(thread)x:%(levelname)s:%(name)s %(message)s')
)
log = logging.getLogger(__name__)


TOTAL_MESSAGES = 10000


def measure(queue_name):
    def _measure(f):
        @functools.wraps(f)
        def decorated():
            print 'Starting function {}... '.format(f.__name__),
            q = PriorityQueue(queue_name)

            pool = gevent.pool.Pool(100)
            for i in xrange(TOTAL_MESSAGES):
                pool.spawn(q.put, i, random.choice(range(-8, 8)))
            pool.join()

            t0 = time.time()
            f(q)
            t1 = time.time()
            # q.purge()
            dt = t1 - t0
            print 'took {} sec ({} messages/sec)'.format(
                dt, 1. * TOTAL_MESSAGES / dt)
        return decorated
    return _measure


@measure('test1')
def single_consumer_without_threading(q):
    with q.consumer() as c:
        while 1:
            try:
                msg = c.get(block=False)
            except QueueEmpty:
                break
            c.ack(msg)


@measure('test2')
def single_consumer_with_multiple_threads(q):

    with q.consumer() as c:
        pool = gevent.pool.Pool()

        def consume(c, thread_no):
            while 1:
                gevent.sleep()
                try:
                    msg = c.get(block=False)
                except QueueEmpty:
                    break
                c.ack(msg)

        for i in xrange(16):
            pool.spawn(consume, c, i)

        pool.join()


@measure('test3')
def multiple_threads_each_with_single_consumer(q):
    def consume():
        with q.consumer() as c:
            while 1:
                gevent.sleep()
                try:
                    msg = c.get(block=False)
                except QueueEmpty:
                    break
                c.ack(msg)

    pool = gevent.pool.Pool()
    for _ in xrange(16):
        pool.spawn(consume)
    pool.join()


single_consumer_without_threading()
single_consumer_with_multiple_threads()
multiple_threads_each_with_single_consumer()
