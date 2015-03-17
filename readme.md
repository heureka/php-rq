[![Travis CI](https://img.shields.io/travis/heureka/php-rq.svg)](https://travis-ci.org/heureka/php-rq)
[![Packagist](https://img.shields.io/packagist/v/heureka/php-rq.svg)](https://packagist.org/packages/heureka/php-rq)

PhpRQ
======

This library is set of php files and Lua scripts which enables you to easily implement queuing system based on Redis.
The library is completely tested using integration tests (because of heavy usage of Lua scripts). There are multiple
types of queues implemented in this library so you can choose the one that fits your needs best.

All the queues works well in multi-threaded environment. The only thing you have to keep in mind is that with multiple
consumers the order of the items is impossible to preserve. E.g. if multiple consumers exits unexpectedly and then you
use re-enqueue method to get the items back to the queue then you will most probably lose the order of the items.
If you want to rely on the order of the items then you are required to use only one consumer at a time, reject whole
batch after failure and re-enqueue everything before getting another chunk of items.

Queue
=====
-----

This is the most basic type of queue. If you don't require uniqueness of the items in the queue then you probably
want to use Queue.

There is **no possible way to guarantee order of the items** if you reject them randomly or if the queue is used
in multi-threaded environment. The Queue tries to maintain the order of the items as best as it can but you have to
keep in mind that it is just the **best effort** method. The order of the items is guaranteed only if you use single
consumer, reject whole batch after failure and re-enqueue failed items before getting the new ones.

Queue - General usage
---------------------

You can add items to the queue using **addItem** and **addItems** methods. If you want to get items from the queue then
you can use **getItems** and **getAllItems** methods. When you have the items from the queue you must either
acknowledge (success; **ackItem**, **ackItems**) or reject (failure; **rejectItem**, **rejectItems**, **rejectBatch**)
them. If you reject an item it is then moved on the head of the queue. If you miss to acknowledge or revoke an item
then it will stay in the processing queue until you clear it using **reEnqueueTimedOutItems**, **reEnqueueAllItems**,
**dropTimedOutItems** or **dropAllItems** (depends on your requirements). You must clear the failed processing
queues somehow, otherwise the forgotten data will fill your Redis.

Queue::getRedisClient
---------------------

Returns the used instance of a Redis client (PhpRQ\ClientInterface). This is useful in multi-threaded environment
when you want to reconnect the connection.

Queue::getCount
---------------

Returns the number of items in the queue.

Queue::addItem
--------------

**param:** *$item* Anything that can be converted to string

Adds an item to the queue.

Queue::addItems
---------------

**param:** (array) *$items* Array containing anything that can be converted to string

Adds multiple items to the queue.

Queue::getItems
---------------

**param:** (int) *$size* Number of items you want to return from the queue

Returns *$size* elements from the queue. You can set the *$size* to any number you like - the items are fetched from
the queue by chunks with a fixed safe size so the server isn't overwhelmed.

Items that are fetched from the queue are added to a processing queue. That way if the process exits unexpectedly
the items fetched from the queue can be preserved using re-enqueue methods.

Queue::getAllItems
------------------

Returns all items from the queue. The items are fetched from the queue by chunks of a fixed safe size so the
server isn't overwhelmed.

Items that are fetched from the queue are added to a processing queue. That way if the process exits unexpectedly
the items fetched from the queue can be preserved using re-enqueue methods.

Queue::ackItem
--------------

**param:** *$item* Anything that can be converted to string

Acknowledges the item - removes the item from the processing queue.

Queue::ackItems
---------------

**param:** (array) *$items* Array containing anything that can be converted to string

Acknowledges multiple items - removes them from the processing queue.

Queue::rejectItem
-----------------

**param:** *$item* Anything that can be converted to string

Revokes the item - removes the item from the processing queue and puts it back at the head of the queue (i.e. it
will be the first item to fetch)

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if you reject some items and then acknowledge
another then the order is lost.

Queue::rejectItems
------------------

**param:** (array) *$items* Array containing anything that can be converted to string

Revokes multiple items - removes the items from the processing queue and puts them back at the head of the queue
in the reversed order (i.e. they will be the first items to fetch, in the same order as they were fetched before).

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if you reject some items and then acknowledge
another then the order is lost.

Queue::rejectBatch
------------------

Revokes all the remaining items in the processing queue and puts them back at the head of the queue in the reversed
order (i.e. they will be the first items to fetch, in the same order as they were fetched before).

Queue::reEnqueueTimedOutItems
-----------------------------

**param:** (int) *$timeout* Number of seconds after which the processing queue and all the items in it are
considered as failed

You should call this method (or the reEnqueueAllItems) periodically (or before fetching items from the queue) to
put the timed out items from the processing queues back to the head of the queue in the reversed order (i.e. they will
be the first items to fetch, in the same order as they were fetched before).

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if the first batch fails, the seconds
successes and the third fails again, the order of the items cannot be preserved because the first batch now
"gets after the second one"

Queue::reEnqueueAllItems
------------------------

You should call this method before fetching items from the queue but only if you have a single consumer. This method
puts the items for all the processing queues back to the head of the queue in the reversed order (i.e. they will be
the first items to fetch, in the same order as they were fetched before). Keep in mind that this method can
re-enqueue still valid processing queues (i.e. not the failed ones).

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if the first batch fails, the second
successes and the third fails again, the order of the items cannot be preserved because the first batch now
"gets after the seconds one"

Queue::dropTimedOutItems
------------------------

**param:** (int) *$timeout* Number of seconds after which the processing queue and all the items in it are
considered as failed

You should call this method (or the dropAllItems) periodically (or before fetching items from the queue) to drop the
timed out items from the processing queues.

Queue::dropAllItems
-------------------

You should call this method before fetching items from the queue but only if you have a single consumer.
This method drops the items for from all the processing queues. Keep in mind that this method can drop still
valid processing queues (i.e. not the failed ones).

Queue::clearQueue
-----------------

Removes all items from the queue and it's processing lists. Useful for testing purposes.

UniqueQueue
===========
-----------

This type of queue is useful if you don't need all the items you push into the queue but just one of each "type".
Let's say that you want to build a queue for refreshing your cached products per category. In that case you will
probably want to send category IDs to the queue. If the processes which handles refreshing of the cache is too slow
then it may happen that you will send multiple update requests (category IDs) into the queue. In that case it isn't
necessary to refresh cache two times - you only need it once but the process just didn't get to your item yet.
The solution is to ignore the second item. And this is exactly what UniqueQueue is intended for. It guarantees you
the uniqueness of items in the queue.

There is **no possible way to guarantee order of the items** if you reject them randomly or if the queue is used
in multi-threaded environment. The UniqueQueue tries to maintain the order of the items as best as it can but you
have to keep in mind that it is just the **best effort** method. The order of the items is guaranteed only if you
use single consumer, reject whole batch after failure and re-enqueue failed items before getting the new ones.

UniqueQueue - General usage
---------------------------

You can add items to the queue using **addItem** and **addItems** methods. If you want to get items from the queue then
you can use **getItems** and **getAllItems** methods. When you have the items from the queue you must either
acknowledge (success; **ackItem**, **ackItems**) or reject (failure; **rejectItem**, **rejectItems**, **rejectBatch**)
them. If you reject an item it is then moved on the head of the queue. If you miss to acknowledge or revoke an item
then it will stay in the processing queue until you clear it using **reEnqueueTimedOutItems**, **reEnqueueAllItems**,
**dropTimedOutItems** or **dropAllItems** (depends on your requirements). You must clear the failed processing queues
somehow, otherwise the forgotten data will fill your Redis.

UniqueQueue::getRedisClient
---------------------------

Returns the used instance of a Redis client (PhpRQ\ClientInterface). This is useful in multi-threaded environment
when you want to reconnect the connection.

UniqueQueue::getCount
---------------------

Returns the number of items in the queue.

UniqueQueue::addItem
--------------------

**param:** *$item* Anything that can be converted to string

Adds an item to the queue.

UniqueQueue::addItems
---------------------

**param:** (array) *$items* Array containing anything that can be converted to string

Adds multiple items to the queue.

UniqueQueue::getItems
---------------------

**param:** (int) *$size* Number of items you want to return from the queue

Returns *$size* elements from the queue. You can set the *$size* to any number you like - the items are fetched from
the queue by chunks with a fixed safe size so the server isn't overwhelmed. 

Items that are fetched from the queue are added to a processing queue. That way if the process exits unexpectedly
the items fetched from the queue can be preserved using re-enqueue methods.

UniqueQueue::getAllItems
------------------------

Returns all items from the queue. The items are fetched from the queue by chunks of a fixed safe size so the
server isn't overwhelmed.

Items that are fetched from the queue are added to a processing queue. That way if the process exits unexpectedly
the items fetched from the queue can be preserved using re-enqueue methods.

UniqueQueue::ackItem
--------------------

**param:** *$item* Anything that can be converted to string

Acknowledges the item - removes the item from the processing queue.

UniqueQueue::ackItems
---------------------

**param:** (array) *$items* Array containing anything that can be converted to string

Acknowledges multiple items - removes them from the processing queue.

UniqueQueue::rejectItem
-----------------------

**param:** *$item* Anything that can be converted to string

Revokes the item - removes the item from the processing queue and puts it back at the head of the queue (i.e. it
will be the first item to fetch)

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if you reject some items and then acknowledge
another then the order is lost.

UniqueQueue::rejectItems
------------------------

**param:** (array) *$items* Array containing anything that can be converted to string

Revokes multiple items - removes the items from the processing queue and puts them back at the head of the queue
in the reversed order (i.e. they will be the first items to fetch, in the same order as they were fetched before).

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if you reject some items and then acknowledge
another then the order is lost.

UniqueQueue::rejectBatch
------------------------

Revokes all the remaining items in the processing queue and puts them back at the head of the queue in the reversed
order (i.e. they will be the first items to fetch, in the same order as they were fetched before).

UniqueQueue::reEnqueueTimedOutItems
-----------------------------------

**param:** (int) *$timeout* Number of seconds after which the processing queue and all the items in it are
considered as failed

You should call this method (or the reEnqueueAllItems) periodically (or before fetching items from the queue) to
put the timed out items from the processing queues back to the head of the queue in the reversed order (i.e. they will
be the first items to fetch, in the same order as they were fetched before).

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if the first batch fails, the seconds
successes and the third fails again, the order of the items cannot be preserved because the first batch now
"gets after the second one"

UniqueQueue::reEnqueueAllItems
------------------------------

You should call this method before fetching items from the queue but only if you have a single consumer. This method
puts the items for all the processing queues back to the head of the queue in the reversed order (i.e. they will be
the first items to fetch, in the same order as they were fetched before). Keep in mind that this method can
re-enqueue still valid processing queues (i.e. not the failed ones).

**Be aware:** Order is guaranteed only by the *best effort* method, e.g. if the first batch fails, the second
successes and the third fails again, the order of the items cannot be preserved because the first batch now
"gets after the seconds one"

UniqueQueue::dropTimedOutItems
------------------------------

**param:** (int) *$timeout* Number of seconds after which the processing queue and all the items in it are
considered as failed

You should call this method (or the dropAllItems) periodically (or before fetching items from the queue) to drop the
timed out items from the processing queues.

UniqueQueue::dropAllItems
-------------------------

You should call this method before fetching items from the queue but only if you have a single consumer.
This method drops the items for from all the processing queues. Keep in mind that this method can drop still
valid processing queues (i.e. not the failed ones).

UniqueQueue::clearQueue
-----------------------

Removes all items from the queue and it's processing lists. Useful for testing purposes.

Pool
====
----

If you have a certain unique actions that needs to be processed for example each day then Pool is great tool for you.
Items in the pool gets processed by the time mark they have assigned. When an item is successfully processed it's time
mark gets increased by the set period of time (5 minutes, hour, day, week, ...) and it won't be processed again until
it's time mark is lower than the actual time.

This is useful if the items in the pool are constant most of the time. Then it is wise to use Pool instead of a Queue

Pool - General usage
--------------------

You can add items to the pool using **addItem** and **addItems** methods. If you want to get items from the pool then
you can use **getItems** and **getAllItems** methods. When you have the items from the pool you can acknowledge
(success; **ackItem**, **ackItems**) or just ignore them. If you no longer wish to process certain items then you can
remove them from the pool with **removeItem** and **removeItems**. You can also clear the whole pool
with the **clearPool** method.

Pool::getRedisClient
--------------------

Returns the used instance of a Redis client (PhpRQ\ClientInterface). This is useful in multi-threaded environment
when you want to reconnect the connection.

Pool::getCount
--------------

Returns the number of items in the pool.

Pool::getCountToProcess
-----------------------

Returns the number of items in the pool which needs to be processed

Pool::isInPool
--------------

**param:** (array|mixed) *$item* Single item or array of items which can be converted to string

Checks if the given item (or multiple items) is in the pool - returns boolean. For multiple items returns array of
booleans indexed by the items.

Pool::addItem
-------------

**param:** *$item* Anything that can be converted to string

Adds an item to the pool.

Pool::addItems
--------------

**param:** (array) *$items* Array containing anything that can be converted to string

Adds multiple items to the pool.

Pool::getItems
--------------

**param:** (int) *$size* Number of items you want to return from the pool

Returns *$size* elements from the pool that should be processed. You can set the *$size* to any number you like - the
items are fetched from the pool one by one so the server isn't overwhelmed. 

Time mark of the items that are fetched from the pool is changed to a float number (a "processing" tag). This way you
can be sure that all the items gets processed (for example if the process crashes).

Pool::getAllItems
-----------------

Returns all items from the pool that should be processed. The items are fetched from the pool one by one so the server
isn't overwhelmed.

Time mark of the items that are fetched from the pool is changed to a float number (a "processing" tag). This way you
can be sure that all the items gets processed (for example if the process crashes).

Pool::ackItem
-------------

**param:** *$item* Anything that can be converted to string

Acknowledges the item - increases the time mark of the item.

Pool::ackItems
--------------

**param:** (array) *$items* Array containing anything that can be converted to string

Acknowledges multiple items - increases the time mark of the items.

Pool::removeItem
----------------

**param:** *$item* Anything that can be converted to string

Removes the given item from the pool

Pool::removeItems
-----------------

**param:** (array) *$items* Array containing anything that can be converted to string

Removes the given items from the pool

Pool::clearPool
---------------

Removes all the items from the pool. Useful for testing purposes.
