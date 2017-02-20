.. image:: https://circleci.com/gh/okomestudio/pyrediq/tree/development.svg?style=shield
   :target: https://circleci.com/gh/okomestudio/pyrediq/tree/development

.. image:: https://coveralls.io/repos/github/okomestudio/pyrediq/badge.svg?branch=development
   :target: https://coveralls.io/github/okomestudio/pyrediq?branch=development

             

pyrediq
=======

`pyrediq` (py-re-ddi-ck) is a Python (2.7) library providing an
implementation of priority messaging queue using `Redis`_. The message
payload is serialized by `MessagePack`_.

`pyrediq` is licensed under the `MIT License (MIT)`_.

.. _MessagePack: http://msgpack.org/
.. _Redis: http://redis.io/
.. _MIT License (MIT): https://raw.githubusercontent.com/okomestudio/pyrediq/development/LICENSE.txt


Basic Usage
-----------

.. code-block:: python

   import redis
   from pyrediq import PriorityQueue, QueueEmpty

   redis_conn = redis.StrictRedis()

   # create a queue
   queue = PriorityQueue('myqueue', redis_conn)

   # enqueue a message to the queue
   queue.put({'mykey': 'myvalue'}, priority=-1)

   # create a consumer
   with queue.consumer() as consumer:
       try:
           msg = consumer.get(block=False)
       except QueueEmpty:
           raise

       success = do_task(msg.payload)

       # the message is either acked or rejected after task is done.
       # a rejected message can optionally be requeued
       if success:
           consumer.ack(msg)
       else:
           consumer.reject(msg, requeue=True)

   queue.close()
           
To remove all message from a queue and remove the queue itself, run
`PriorityQueue.purge` method:

.. code-block:: python

   queue.purge()


Installation
------------            

To install `pyrediq` using `pip`, run this command on the shell::

  pip install pyrediq

To use `pyrediq`, Redis needs to be installed and running on the
computer. For example, on a Debian box, running this command should
suffice::
  
  sudo apt-get install redis-server

Consult the `official Redis distribution site`_ for the install
procedure.

.. _official Redis distribution site: http://redis.io/
