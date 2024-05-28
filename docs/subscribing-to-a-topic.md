# Subscribing to a topic

To receive messages for a topic, we need to subscribe to it. Incoming messages are queued internally. You can use the `Client.messages` generator to iterate over incoming messages. This is a minimal working example that listens for messages to the `temperature/#` wildcard:

```python
import asyncio
import aiomqtt


async def main():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        async for message in client.messages:
            print(message.payload)


asyncio.run(main())
```

Now you can use the [minimal publisher example](publishing-a-message.md) to publish a message to `temperature/outside` and see it appear in the console.

```{tip}
You can set the [Quality of Service](publishing-a-message.md#quality-of-service-qos) of the subscription by passing the `qos` parameter to `Client.subscribe()`.
```

## Filtering messages

Imagine that we measure temperature and humidity on the outside and inside. We want to receive all types of measurements but handle them differently.

You can filter messages with `Topic.matches()`. Similar to `Client.subscribe()`, `Topic.matches()` also accepts wildcards (e.g. `temperature/#`):

```python
import asyncio
import aiomqtt


async def main():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        await client.subscribe("humidity/#")
        async for message in client.messages:
            if message.topic.matches("humidity/inside"):
                print("A:", message.payload)
            if message.topic.matches("+/outside"):
                print("B:", message.payload)
            if message.topic.matches("temperature/#"):
                print("C:", message.payload)


asyncio.run(main())
```

```{note}
In our example, messages to `temperature/outside` are handled twice!
```

```{tip}
For details on the `+` and `#` wildcards and what topics they match, see the [OASIS specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241).
```

## The message queue

Messages are queued internally and returned sequentially from `Client.messages`.

The default queue is `asyncio.Queue` which returns messages on a FIFO ("first in first out") basis. You can pass [other types of asyncio queues](https://docs.python.org/3/library/asyncio-queue.html) as `queue_type` to the `Client` to modify the order in which messages are returned, e.g. `asyncio.LifoQueue`.

You can subclass `asyncio.PriorityQueue` to queue based on priority. Messages are returned ascendingly by their priority values. In the case of ties, messages with lower message identifiers are returned first.

Let's say we measure temperature and humidity again, but we want to prioritize humidity:

```python
import asyncio
import aiomqtt


class CustomPriorityQueue(asyncio.PriorityQueue):
    def _put(self, item):
        priority = 2
        if item.topic.matches("humidity/#"):  # Assign priority
            priority = 1
        super()._put((priority, item))

    def _get(self):
        return super()._get()[1]


async def main():
    async with aiomqtt.Client(
        "test.mosquitto.org", queue_type=CustomPriorityQueue
    ) as client:
        await client.subscribe("temperature/#")
        await client.subscribe("humidity/#")
        async for message in client.messages:
            print(message.payload)


asyncio.run(main())
```

```{tip}
By default, the size of the queue is unlimited. You can set a limit through the client's `max_queued_incoming_messages` argument.
```

## Processing concurrently

Messages are queued internally and returned sequentially from `Client.messages`. If a message takes a long time to handle, it blocks the handling of other messages.

You can handle messages concurrently by using an `asyncio.TaskGroup` like so:

```python
import asyncio
import aiomqtt


async def handle(message):
    await asyncio.sleep(5)  # Simulate some I/O-bound work
    print(message.payload)


async def main():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        # Use a task group to manage and await all tasks
        async with asyncio.TaskGroup() as tg:
            async for message in client.messages:
                tg.create_task(handle(message))  # Spawn new coroutine


asyncio.run(main())
```

```{important}
Coroutines only make sense if your message handling is I/O-bound. If it's CPU-bound, you should spawn multiple processes instead.
```

## Multiple queues

The code snippet above handles each message in a new coroutine. Sometimes, we want to handle messages from different topics concurrently, but sequentially inside a single topic.

The idea here is to implement a "distributor" that sorts incoming messages into multiple asyncio queues. Each queue is then processed by a different coroutine. Let's see how this works for our temperature and humidity messages:

```python
import asyncio
import aiomqtt


async def temperature_consumer():
    while True:
        message = await temperature_queue.get()
        print(f"[temperature/#] {message.payload}")


async def humidity_consumer():
    while True:
        message = await humidity_queue.get()
        print(f"[humidity/#] {message.payload}")


temperature_queue = asyncio.Queue()
humidity_queue = asyncio.Queue()


async def distributor(client):
    # Sort messages into the appropriate queues
    async for message in client.messages:
        if message.topic.matches("temperature/#"):
            temperature_queue.put_nowait(message)
        elif message.topic.matches("humidity/#"):
            humidity_queue.put_nowait(message)


async def main():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        await client.subscribe("humidity/#")
        # Use a task group to manage and await all tasks
        async with asyncio.TaskGroup() as tg:
            tg.create_task(distributor(client))
            tg.create_task(temperature_consumer())
            tg.create_task(humidity_consumer())


asyncio.run(main())
```

```{tip}
You can use [different queue types](#the-message-queue) for these queues to e.g. handle temperature in FIFO and humidity in LIFO order.
```

## Implicit message dispatching

The code snippets so far read a global message queue and dispatch messages by comparing topics. The problem is that this is duplicate work: the server already did the comparison, so why should the client do that too?

Another problem with the "global queue" approach is that when you extend your program, you also need to modify the central distributor task. That's not particularly modular.

Fortunately the client can do the work for you, by directly dispatching the message to a local queue. This also simplifies your code:

```python
import asyncio
import aiomqtt


async def temperature_consumer(client):
    async with client.subscription("temperature/#") as messages:
        async for message in messages:
            print(f"[temperature/#] {message.payload}")


async def humidity_consumer(client):
    async with client.subscription("humidity/#") as messages:
        async for message in messages:
            print(f"[humidity/#] {message.payload}")

async def main():
    async with aiomqtt.Client("test.mosquitto.org") as client, \
            asyncio.TaskGroup() as tg:
        tg.create_task(temperature_consumer(client))
        tg.create_task(humidity_consumer(client))

asyncio.run(main())
```

This method adds an ID to each subscription. The server adds the IDs to each message it sends to us. Then we simply use the IDs to send the message to all queues it's destined for.

You can freely mix and match this approach with the central-distributor model.

```{tip}
MQTT clients cannot subscribe to two identical topic patterns. One possible workaround is to use MQTTv5's "shared subscription" feature (using different unique share names). See section 4.8.2 of [the MQTT standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Shared_Subscriptions) for details.
```

## Queue overrun

Message queues are not infinite. When a reading task gets too far behind, ultimately we can't queue another message. Oops.

Local subscriptions handle this situation by closing the queue. The reader will get an `anyio.IncompleteReadError` exception after consuming the remaining data. You then need to restart the subscription:

```python

from asyncio.queue import Queue

async def temperature_consumer(client):
    while True:
        try:
            q = Queue(5)
            async with client.subscription("temperature/#", queue=q) as messages:
                async for message in messages:
                    print(f"[temperature/#] {message.payload}")
                    await asyncio.sleep(2)  # some async processing of this message

        except anyio.IncompleteReadError:
            print("Too many temperatures too fast! I could not keep up!")
```

Local queues are limited to 100 messages by default.

The global queue's length defaults to 10000. If the global queue is full,
new messages are silently dropped.

```{tip}
MQTT's "retained message" feature means that when you subscribe to a wildcard, the server might flood you with retained state for all matching topics. Your queues need to be large enough to handle this.
```

## Listening without blocking

When you run the minimal example for subscribing and listening for messages, you'll notice that the program doesn't finish. Waiting for messages through the `Client.messages()` generator blocks the execution of everything that comes afterward.

In case you want to run other code after starting your listener, this is not very practical.

You can use `asyncio.TaskGroup` (or `asyncio.gather` for Python `<3.11`) to safely run other tasks alongside the MQTT listener:

```python
import asyncio
import aiomqtt


async def sleep(seconds):
    # Some other task that needs to run concurrently
    await asyncio.sleep(seconds)
    print(f"Slept for {seconds} seconds!")


async def listen():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        async for message in client.messages:
            print(message.payload)


async def main():
    # Use a task group to manage and await all tasks
    async with asyncio.TaskGroup() as tg:
        tg.create_task(sleep(2))
        tg.create_task(listen())  # Start the listener task
        tg.create_task(sleep(3))
        tg.create_task(sleep(1))


asyncio.run(main())
```

In case task groups are not an option (e.g. because you run aiomqtt [alongside a web framework](alongside-fastapi-and-co.md)) you can start the listener in a fire-and-forget way. The idea is to use asyncio's `create_task` without awaiting the created task:

```{caution}
You need to be a bit careful with this approach. Exceptions raised in asyncio tasks are propagated only when we `await` the task. In this case, we explicitly don't.

You need to handle all possible exceptions _inside_ the fire-and-forget task. Unhandled exceptions will be silently ignored until the program exits.
```

```python
import asyncio
import aiomqtt


async def listen():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        async for message in client.messages:
            print(message.payload)


background_tasks = set()


async def main():
    loop = asyncio.get_event_loop()
    # Listen for mqtt messages in an (unawaited) asyncio task
    task = loop.create_task(listen())
    # Save a reference to the task so it doesn't get garbage collected
    background_tasks.add(task)
    task.add_done_callback(background_tasks.remove)

    # Infinitely do something else (e.g. handling HTTP requests)
    while True:
        await asyncio.sleep(1)


asyncio.run(main())
```

```{important}
You [need to keep a reference to the task](https://docs.python.org/3/library/asyncio-task.html#creating-tasks) so it doesn't get garbage collected mid-execution. That's what we're using the `background_tasks` set in our example for.
```

## Stop listening

You might want to have a listener task running alongside other code, and then stop it when you're done. You can use [`asyncio.Task.cancel()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel) for this:

```python
import asyncio
import aiomqtt


async def listen():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        async for message in client.messages:
            print(message.payload)


async def main():
    loop = asyncio.get_event_loop()
    # Listen for MQTT messages in (unawaited) asyncio task
    task = loop.create_task(listen())
    # Do something else for a while
    await asyncio.sleep(5)
    # Cancel the task
    task.cancel()
    # Wait for the task to be cancelled
    try:
        await task
    except asyncio.CancelledError:
        pass


asyncio.run(main())
```

## Stop listening after timeout

For use cases where you only want to listen to messages for a certain amount of time, we can use [asyncio's timeouts](https://docs.python.org/3/library/asyncio-task.html#timeouts), which were introduced in Python `3.11`:

```python
import asyncio
import aiomqtt


async def listen():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        await client.subscribe("temperature/#")
        async for message in client.messages:
            print(message.payload)


async def main():
    try:
        # Cancel the listener task after 5 seconds
        async with asyncio.timeout(5):
            await listen()
    # Ignore the resulting TimeoutError
    except asyncio.TimeoutError:
        pass


asyncio.run(main())
```
