.. pyrediq documentation master file, created by
   sphinx-quickstart on Sat Sep  3 14:28:44 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyrediq's documentation!
===================================


Basic Usage
-----------

.. code-block:: python

    import redis
    from pyrediq import PriorityQueue, QueueEmpty
                
    redis_conn = redis.StrictRedis()
    queue = PriorityQueue('myqueue', redis_conn)

    queue.put({'mykey': 'myvalue'}, priority=-1)

    with queue.consumer() as consumer:
        try:
            msg = consumer.get(block=False)
        except QueueEmpty:
            raise
        success = do_task(msg.payload)
        if success:
            consumer.ack(msg)
        else:
            consumer.reject(msg, requeue=True)
        

API
---
            
.. autoclass:: pyrediq.priority_queue.PriorityQueue
   :members:

.. autoclass:: pyrediq.priority_queue.MessageConsumer
   :members:

.. autoclass:: pyrediq.priority_queue.QueueEmpty
   :members:




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

