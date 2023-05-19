from __future__ import annotations

import logging
import ssl
import sys
from pathlib import Path

import anyio
import anyio.abc
import paho.mqtt.client as mqtt
import pytest

from asyncio_mqtt import Client, ProtocolVersion, TLSParameters, Topic, Wildcard, Will
from asyncio_mqtt.error import MqttReentrantError
from asyncio_mqtt.types import PayloadType

pytestmark = pytest.mark.anyio

HOSTNAME = "test.mosquitto.org"
OS_PY_VERSION = sys.platform + "_" + ".".join(map(str, sys.version_info[:2]))
TOPIC_HEADER = OS_PY_VERSION + "/tests/asyncio_mqtt/"


async def test_topic_validation() -> None:
    """Test that Topic raises Exceptions for invalid topics."""
    with pytest.raises(TypeError):
        Topic(True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Topic(1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Topic(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Topic([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("a/b/#")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("a/+/c")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("#")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("a" * 65536)


async def test_wildcard_validation() -> None:
    """Test that Wildcard raises Exceptions for invalid wildcards."""
    with pytest.raises(TypeError):
        Wildcard(True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Wildcard(1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Wildcard(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Wildcard([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a/#/c")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a/b+/c")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a/b/#c")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a" * 65536)


async def test_topic_matches() -> None:
    """Test that Topic.matches() does and doesn't match some test wildcards."""
    topic = Topic("a/b/c")
    assert topic.matches("a/b/c")
    assert topic.matches("a/+/c")
    assert topic.matches("+/+/+")
    assert topic.matches("+/#")
    assert topic.matches("#")
    assert topic.matches("$share/group/a/b/c")
    assert topic.matches("$share/group/a/b/+")
    assert not topic.matches("abc")
    assert not topic.matches("a/b")
    assert not topic.matches("a/b/c/d")
    assert not topic.matches("a/b/z")
    assert not topic.matches("$share/a/b/c")
    assert not topic.matches("$test/group/a/b/c")


async def test_multiple_messages_generators() -> None:
    """Test that multiple Client.messages() generators can be used at the same time."""
    topic = TOPIC_HEADER + "multiple_messages_generators"

    async def handler(tg: anyio.abc.TaskGroup) -> None:
        async with client.messages() as messages:
            async for message in messages:
                assert str(message.topic) == topic
                tg.cancel_scope.cancel()

    async with Client(HOSTNAME) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic)
            tg.start_soon(handler, tg)
            tg.start_soon(handler, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic)


async def test_client_filtered_messages() -> None:
    topic_header = TOPIC_HEADER + "filtered_messages/"
    good_topic = topic_header + "good"
    bad_topic = topic_header + "bad"

    async def handle_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.filtered_messages(good_topic) as messages:
            async for message in messages:
                assert message.topic == good_topic
                tg.cancel_scope.cancel()

    async with Client(HOSTNAME) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic_header + "#")
            tg.start_soon(handle_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(bad_topic, 2)
            await client.publish(good_topic, 2)


async def test_client_unfiltered_messages() -> None:
    topic_header = TOPIC_HEADER + "unfiltered_messages/"
    topic_filtered = topic_header + "filtered"
    topic_unfiltered = topic_header + "unfiltered"

    async def handle_unfiltered_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.unfiltered_messages() as messages:
            async for message in messages:
                assert message.topic == topic_unfiltered
                tg.cancel_scope.cancel()

    async def handle_filtered_messages() -> None:
        async with client.filtered_messages(topic_filtered) as messages:
            async for message in messages:
                assert message.topic == topic_filtered

    async with Client(HOSTNAME) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic_header + "#")
            tg.start_soon(handle_filtered_messages)
            tg.start_soon(handle_unfiltered_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic_filtered, 2)
            await client.publish(topic_unfiltered, 2)


async def test_client_unsubscribe() -> None:
    topic_header = TOPIC_HEADER + "unsubscribe/"
    topic1 = topic_header + "1"
    topic2 = topic_header + "2"

    async def handle_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.unfiltered_messages() as messages:
            is_first_message = True
            async for message in messages:
                if is_first_message:
                    assert message.topic == topic1
                    is_first_message = False
                else:
                    assert message.topic == topic2
                    tg.cancel_scope.cancel()

    async with Client(HOSTNAME) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic1)
            await client.subscribe(topic2)
            tg.start_soon(handle_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic1, 2)
            await client.unsubscribe(topic1)
            await client.publish(topic1, 2)
            await client.publish(topic2, 2)


@pytest.mark.parametrize(
    "protocol, length",
    [(ProtocolVersion.V31, 22), (ProtocolVersion.V311, 0), (ProtocolVersion.V5, 0)],
)
async def test_client_id(protocol: ProtocolVersion, length: int) -> None:
    client = Client(HOSTNAME, protocol=protocol)
    assert len(client.id) == length


async def test_client_will() -> None:
    topic = TOPIC_HEADER + "will"
    event = anyio.Event()

    async def launch_client() -> None:
        with anyio.CancelScope(shield=True) as cs:
            async with Client(HOSTNAME) as client:
                await client.subscribe(topic)
                event.set()
                async with client.filtered_messages(topic) as messages:
                    async for message in messages:
                        assert message.topic == topic
                        cs.cancel()

    async with anyio.create_task_group() as tg:
        tg.start_soon(launch_client)
        await event.wait()
        async with Client(HOSTNAME, will=Will(topic)) as client:
            client._client._sock_close()  # type: ignore[attr-defined]


async def test_client_tls_context() -> None:
    topic = TOPIC_HEADER + "tls_context"

    async def handle_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.filtered_messages(topic) as messages:
            async for message in messages:
                assert message.topic == topic
                tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME,
        8883,
        tls_context=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS),
    ) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic)
            tg.start_soon(handle_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic)


async def test_client_tls_params() -> None:
    topic = TOPIC_HEADER + "tls_params"

    async def handle_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.filtered_messages(topic) as messages:
            async for message in messages:
                assert message.topic == topic
                tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME,
        8883,
        tls_params=TLSParameters(
            ca_certs=str(Path.cwd() / "tests" / "mosquitto.org.crt")
        ),
    ) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic)
            tg.start_soon(handle_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic)


async def test_client_username_password() -> None:
    topic = TOPIC_HEADER + "username_password"

    async def handle_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.filtered_messages(topic) as messages:
            async for message in messages:
                assert message.topic == topic
                tg.cancel_scope.cancel()

    async with Client(HOSTNAME, username="", password="") as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic)
            tg.start_soon(handle_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic)


async def test_client_logger() -> None:
    logger = logging.getLogger("asyncio-mqtt")
    async with Client(HOSTNAME, logger=logger) as client:
        assert logger is client._client._logger  # type: ignore[attr-defined]


async def test_client_max_concurrent_outgoing_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topic = TOPIC_HEADER + "max_concurrent_outgoing_calls"

    class MockPahoClient(mqtt.Client):
        def subscribe(
            self,
            topic: str
            | tuple[str, mqtt.SubscribeOptions]
            | list[tuple[str, mqtt.SubscribeOptions]]
            | list[tuple[str, int]],
            qos: int = 0,
            options: mqtt.SubscribeOptions | None = None,
            properties: mqtt.Properties | None = None,
        ) -> tuple[int, int]:
            assert client._outgoing_calls_sem is not None
            assert client._outgoing_calls_sem.locked()
            return super().subscribe(topic, qos, options, properties)

        def unsubscribe(
            self, topic: str | list[str], properties: mqtt.Properties | None = None
        ) -> tuple[int, int]:
            assert client._outgoing_calls_sem is not None
            assert client._outgoing_calls_sem.locked()
            return super().unsubscribe(topic, properties)

        def publish(
            self,
            topic: str,
            payload: PayloadType | None = None,
            qos: int = 0,
            retain: bool = False,
            properties: mqtt.Properties | None = None,
        ) -> mqtt.MQTTMessageInfo:
            assert client._outgoing_calls_sem is not None
            assert client._outgoing_calls_sem.locked()
            return super().publish(topic, payload, qos, retain, properties)

    monkeypatch.setattr(mqtt, "Client", MockPahoClient)

    async with Client(HOSTNAME, max_concurrent_outgoing_calls=1) as client:
        await client.subscribe(topic)
        await client.unsubscribe(topic)
        await client.publish(topic)


async def test_client_websockets() -> None:
    topic = TOPIC_HEADER + "websockets"

    async def handle_messages(tg: anyio.abc.TaskGroup) -> None:
        async with client.filtered_messages(topic) as messages:
            async for message in messages:
                assert message.topic == topic
                tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME,
        8080,
        transport="websockets",
        websocket_path="/",
        websocket_headers={"foo": "bar"},
    ) as client:
        async with anyio.create_task_group() as tg:
            await client.subscribe(topic)
            tg.start_soon(handle_messages, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic)


@pytest.mark.parametrize("pending_calls_threshold", [10, 20])
async def test_client_pending_calls_threshold(
    pending_calls_threshold: int, caplog: pytest.LogCaptureFixture
) -> None:
    topic = TOPIC_HEADER + "pending_calls_threshold"

    async with Client(HOSTNAME) as client:
        client.pending_calls_threshold = pending_calls_threshold
        nb_publish = client.pending_calls_threshold + 1

        async with anyio.create_task_group() as tg:
            for _ in range(nb_publish):
                tg.start_soon(client.publish, topic)

        assert caplog.record_tuples == [
            (
                "mqtt",
                logging.WARNING,
                f"There are {nb_publish} pending publish calls.",
            )
        ]


@pytest.mark.parametrize("pending_calls_threshold", [10, 20])
async def test_client_no_pending_calls_warnings_with_max_concurrent_outgoing_calls(
    pending_calls_threshold: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    topic = (
        TOPIC_HEADER + "no_pending_calls_warnings_with_max_concurrent_outgoing_calls"
    )

    async with Client(HOSTNAME, max_concurrent_outgoing_calls=1) as client:
        client.pending_calls_threshold = pending_calls_threshold
        nb_publish = client.pending_calls_threshold + 1

        async with anyio.create_task_group() as tg:
            for _ in range(nb_publish):
                tg.start_soon(client.publish, topic)

        assert caplog.record_tuples == []


async def test_client_not_reentrant() -> None:
    client = Client(HOSTNAME)

    with pytest.raises(MqttReentrantError):  # noqa: PT012
        async with client:
            async with client:
                ...


async def test_client_reusable() -> None:
    client = Client(HOSTNAME)

    async with client:
        await client.publish("task/a", "task_a")

    async with client:
        await client.publish("task/b", "task_b")


async def test_client_connect_disconnect() -> None:
    client = Client(HOSTNAME)

    await client.connect()
    await client.publish("connect", "connect")
    await client.disconnect()


async def test_client_reusable_message() -> None:
    custom_client = Client(HOSTNAME)
    publish_client = Client(HOSTNAME)

    async def task_a_customer() -> None:
        async with custom_client:
            async with custom_client.messages() as messages:
                await custom_client.subscribe("task/a")
                async for message in messages:
                    assert message.payload == b"task_a"
                    return

    async def task_b_customer() -> None:
        async with custom_client:
            async with custom_client.messages() as messages:
                await custom_client.subscribe("task/b")
                async for message in messages:
                    assert message.payload == b"task_b"
                    return

    async def task_a_publisher() -> None:
        async with publish_client:
            await publish_client.publish("task/a", "task_a")

    async with anyio.create_task_group() as tg:
        tg.start_soon(task_a_customer)
        await anyio.sleep(1)
        tg.start_soon(task_a_publisher)

    with pytest.raises(MqttReentrantError):  # noqa: PT012
        async with anyio.create_task_group() as tg:
            tg.start_soon(task_a_customer)
            tg.start_soon(task_b_customer)
            await anyio.sleep(1)
            tg.start_soon(task_a_publisher)


async def test_client_use_connect_disconnect_multiple_message() -> None:
    custom_client = Client(HOSTNAME)
    publish_client = Client(HOSTNAME)

    await custom_client.connect()
    await publish_client.connect()

    async def task_a_customer() -> None:
        await custom_client.subscribe("a/b/c")
        async with custom_client.messages() as messages:
            async for message in messages:
                assert message.payload == b"task_a"
                return

    async def task_b_customer() -> None:
        num = 0
        await custom_client.subscribe("qwer")
        async with custom_client.messages() as messages:
            async for message in messages:
                assert message.payload in [b"task_a", b"task_b"]
                num += 1
                if num == 2:  # noqa: PLR2004
                    return

    async def task_publisher(topic: str, payload: PayloadType) -> None:
        await publish_client.publish(topic, payload)

    async with anyio.create_task_group() as tg:
        tg.start_soon(task_a_customer)
        tg.start_soon(task_b_customer)
        await anyio.sleep(1)
        tg.start_soon(task_publisher, "a/b/c", "task_a")
        tg.start_soon(task_publisher, "qwer", "task_b")

    await custom_client.disconnect()
    await publish_client.disconnect()
