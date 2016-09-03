#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
import gevent.monkey; gevent.monkey.patch_all()
import gevent.pool

import functools
import logging
import random
import time

from pyrediq import PriorityQueue
from pyrediq import QueueEmpty


# logging.basicConfig(level='DEBUG')
log = logging.getLogger(__name__)


TOTAL_MESSAGES = 10000


def measure(f):
    @functools.wraps(f)
    def decorated():
        print 'Starting function {}... '.format(f.__name__),
        q = PriorityQueue('test')
        for i in xrange(TOTAL_MESSAGES):
            q.put(i, random.choice(range(-8, 8)))
        t0 = time.time()
        f(q)
        t1 = time.time()
        q.purge()
        dt = t1 - t0
        print 'took {} sec ({} messages/sec)'.format(
            dt, 1. * TOTAL_MESSAGES / dt)
    return decorated


@measure
def single_consumer_without_threading(q):
    with q.consumer() as c:
        while 1:
            try:
                msg = c.get(block=False)
            except QueueEmpty:
                break
            c.ack(msg)


@measure
def single_consumer_with_multiple_threads(q):
    pool = gevent.pool.Pool()

    with q.consumer() as c:
        def consume(c):
            while 1:
                try:
                    msg = c.get(block=False)
                except QueueEmpty:
                    break
                c.ack(msg)

        for _ in xrange(16):
            pool.spawn(consume, c)

    pool.join()


@measure
def multiple_threads_each_with_single_consumer(q):
    def consume():
        with q.consumer() as c:
            while 1:
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
