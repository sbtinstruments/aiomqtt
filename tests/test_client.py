from __future__ import annotations

import logging
import pathlib
import ssl
import sys

import anyio
import anyio.abc
import paho.mqtt.client as mqtt
import pytest
from anyio import TASK_STATUS_IGNORED
from anyio.abc import TaskStatus
from paho.mqtt.enums import MQTTErrorCode
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from aiomqtt import (
    Client,
    MqttError,
    MqttReentrantError,
    ProtocolVersion,
    TLSParameters,
    Will,
)
from aiomqtt.types import PayloadType

# This is the same as marking all tests in this file with @pytest.mark.anyio
pytestmark = pytest.mark.anyio

HOSTNAME = "test.mosquitto.org"
OS_PY_VERSION = sys.platform + "_" + ".".join(map(str, sys.version_info[:2]))
TOPIC_PREFIX = OS_PY_VERSION + "/tests/aiomqtt/"


@pytest.mark.network
async def test_client_unsubscribe() -> None:
    """Test that messages are no longer received after unsubscribing from a topic."""
    topic_1 = TOPIC_PREFIX + "test_client_unsubscribe/1"
    topic_2 = TOPIC_PREFIX + "test_client_unsubscribe/2"

    async def handle(tg: anyio.abc.TaskGroup) -> None:
        is_first_message = True
        async for message in client.messages:
            if is_first_message:
                assert message.topic.value == topic_1
                is_first_message = False
            else:
                assert message.topic.value == topic_2
                tg.cancel_scope.cancel()

    async with Client(HOSTNAME) as client, anyio.create_task_group() as tg:
        await client.subscribe(topic_1)
        await client.subscribe(topic_2)
        tg.start_soon(handle, tg)
        await anyio.wait_all_tasks_blocked()
        await client.publish(topic_1, None)
        await client.unsubscribe(topic_1)
        await client.publish(topic_1, None)
        # Test that other subscriptions still receive messages
        await client.publish(topic_2, None)


@pytest.mark.parametrize(
    "protocol, length",
    [(ProtocolVersion.V31, 22), (ProtocolVersion.V311, 0), (ProtocolVersion.V5, 0)],
)
async def test_client_id(protocol: ProtocolVersion, length: int) -> None:
    client = Client(HOSTNAME, protocol=protocol)
    assert len(client.identifier) == length


@pytest.mark.network
async def test_client_will() -> None:
    topic = TOPIC_PREFIX + "test_client_will"
    event = anyio.Event()

    async def launch_client() -> None:
        with anyio.CancelScope(shield=True) as cs:
            async with Client(HOSTNAME) as client:
                await client.subscribe(topic)
                event.set()
                async for message in client.messages:
                    assert message.topic.value == topic
                    cs.cancel()

    async with anyio.create_task_group() as tg:
        tg.start_soon(launch_client)
        await event.wait()
        async with Client(HOSTNAME, will=Will(topic)) as client:
            client._client._sock_close()


@pytest.mark.network
async def test_client_tls_context() -> None:
    topic = TOPIC_PREFIX + "test_client_tls_context"

    async def handle(tg: anyio.abc.TaskGroup) -> None:
        async for message in client.messages:
            assert message.topic.value == topic
            tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME,
        8883,
        tls_context=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS),
    ) as client, anyio.create_task_group() as tg:
        await client.subscribe(topic)
        tg.start_soon(handle, tg)
        await anyio.wait_all_tasks_blocked()
        await client.publish(topic)


@pytest.mark.network
async def test_client_tls_params() -> None:
    topic = TOPIC_PREFIX + "tls_params"

    async def handle(tg: anyio.abc.TaskGroup) -> None:
        async for message in client.messages:
            assert message.topic.value == topic
            tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME,
        8883,
        tls_params=TLSParameters(
            ca_certs=str(pathlib.Path.cwd() / "tests" / "mosquitto.org.crt")
        ),
    ) as client, anyio.create_task_group() as tg:
        await client.subscribe(topic)
        tg.start_soon(handle, tg)
        await anyio.wait_all_tasks_blocked()
        await client.publish(topic)


@pytest.mark.network
async def test_client_username_password() -> None:
    topic = TOPIC_PREFIX + "username_password"

    async def handle(tg: anyio.abc.TaskGroup) -> None:
        async for message in client.messages:
            assert message.topic.value == topic
            tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME, username="", password=""
    ) as client, anyio.create_task_group() as tg:
        await client.subscribe(topic)
        tg.start_soon(handle, tg)
        await anyio.wait_all_tasks_blocked()
        await client.publish(topic)


@pytest.mark.network
async def test_client_logger() -> None:
    logger = logging.getLogger("aiomqtt")
    async with Client(HOSTNAME, logger=logger) as client:
        assert logger is client._client._logger


@pytest.mark.network
async def test_client_max_concurrent_outgoing_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topic = TOPIC_PREFIX + "max_concurrent_outgoing_calls"

    class MockPahoClient(mqtt.Client):
        def subscribe(
            self,
            topic: str
            | tuple[str, int]
            | tuple[str, SubscribeOptions]
            | list[tuple[str, int]]
            | list[tuple[str, SubscribeOptions]],
            qos: int = 0,
            options: SubscribeOptions | None = None,
            properties: Properties | None = None,
        ) -> tuple[MQTTErrorCode, int | None]:
            assert client._outgoing_calls_sem is not None
            assert client._outgoing_calls_sem.locked()
            return super().subscribe(topic, qos, options, properties)

        def unsubscribe(
            self, topic: str | list[str], properties: Properties | None = None
        ) -> tuple[MQTTErrorCode, int | None]:
            assert client._outgoing_calls_sem is not None
            assert client._outgoing_calls_sem.locked()
            return super().unsubscribe(topic, properties)  # type: ignore[arg-type]

        def publish(  # noqa: PLR0913
            self,
            topic: str,
            payload: PayloadType | None = None,
            qos: int = 0,
            retain: bool = False,
            properties: Properties | None = None,
        ) -> mqtt.MQTTMessageInfo:
            assert client._outgoing_calls_sem is not None
            assert client._outgoing_calls_sem.locked()
            return super().publish(topic, payload, qos, retain, properties)

    monkeypatch.setattr(mqtt, "Client", MockPahoClient)

    async with Client(HOSTNAME, max_concurrent_outgoing_calls=1) as client:
        await client.subscribe(topic)
        await client.unsubscribe(topic)
        await client.publish(topic)


@pytest.mark.network
async def test_client_websockets() -> None:
    topic = TOPIC_PREFIX + "websockets"

    async def handle(tg: anyio.abc.TaskGroup) -> None:
        async for message in client.messages:
            assert message.topic.value == topic
            tg.cancel_scope.cancel()

    async with Client(
        HOSTNAME,
        8080,
        transport="websockets",
        websocket_path="/",
        websocket_headers={"foo": "bar"},
    ) as client:
        await client.subscribe(topic)
        async with anyio.create_task_group() as tg:
            tg.start_soon(handle, tg)
            await anyio.wait_all_tasks_blocked()
            await client.publish(topic)


@pytest.mark.network
@pytest.mark.parametrize("pending_calls_threshold", [10, 20])
async def test_client_pending_calls_threshold(
    pending_calls_threshold: int, caplog: pytest.LogCaptureFixture
) -> None:
    topic = TOPIC_PREFIX + "pending_calls_threshold"

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


@pytest.mark.network
@pytest.mark.parametrize("pending_calls_threshold", [10, 20])
async def test_client_no_pending_calls_warnings_with_max_concurrent_outgoing_calls(
    pending_calls_threshold: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    topic = (
        TOPIC_PREFIX + "no_pending_calls_warnings_with_max_concurrent_outgoing_calls"
    )

    async with Client(HOSTNAME, max_concurrent_outgoing_calls=1) as client:
        client.pending_calls_threshold = pending_calls_threshold
        nb_publish = client.pending_calls_threshold + 1

        async with anyio.create_task_group() as tg:
            for _ in range(nb_publish):
                tg.start_soon(client.publish, topic)

        assert caplog.record_tuples == []


@pytest.mark.network
async def test_client_context_is_reusable() -> None:
    """Test that a client context manager instance is reusable."""
    topic = TOPIC_PREFIX + "test_client_is_reusable"
    client = Client(HOSTNAME)
    async with client:
        await client.publish(topic, "foo")
    async with client:
        await client.publish(topic, "bar")


@pytest.mark.network
async def test_client_context_is_not_reentrant() -> None:
    """Test that a client context manager instance is not reentrant."""
    client = Client(HOSTNAME)
    async with client:
        with pytest.raises(MqttReentrantError):
            async with client:
                pass


@pytest.mark.network
async def test_client_reusable_message() -> None:
    custom_client = Client(HOSTNAME)
    publish_client = Client(HOSTNAME)

    async def task_a_customer(
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
    ) -> None:
        async with custom_client:
            await custom_client.subscribe("task/a")
            task_status.started()
            async for message in custom_client.messages:
                assert message.payload == b"task_a"
                return

    async def task_b_customer() -> None:
        async with custom_client:
            ...

    async def task_a_publisher() -> None:
        async with publish_client:
            await publish_client.publish("task/a", "task_a")

    async with anyio.create_task_group() as tg:
        await tg.start(task_a_customer)
        tg.start_soon(task_a_publisher)

    with pytest.raises(MqttReentrantError):  # noqa: PT012
        async with anyio.create_task_group() as tg:
            await tg.start(task_a_customer)
            tg.start_soon(task_b_customer)
            await anyio.sleep(1)
            tg.start_soon(task_a_publisher)


@pytest.mark.network
async def test_aenter_state_reset_connect_failure() -> None:
    """Test that internal state is reset on CONNECT failure in ``aenter``."""
    client = Client(hostname="invalid")
    with pytest.raises(MqttError):
        await client.__aenter__()
    assert not client._lock.locked()
    assert not client._connected.done()


@pytest.mark.network
async def test_aenter_state_reset_connack_timeout() -> None:
    """Test that internal state is reset on CONNACK timeout in ``aenter``."""
    client = Client(HOSTNAME, timeout=0)
    with pytest.raises(MqttError):
        await client.__aenter__()
    assert not client._lock.locked()
    assert not client._connected.done()


@pytest.mark.network
async def test_aenter_state_reset_connack_negative() -> None:
    """Test that internal state is reset on negative CONNACK in ``aenter``."""
    client = Client(HOSTNAME, username="invalid")
    with pytest.raises(MqttError):
        await client.__aenter__()
    assert not client._lock.locked()
    assert not client._connected.done()


@pytest.mark.network
async def test_aexit_without_prior_aenter() -> None:
    """Test that ``aexit`` without prior (or unsuccessful) ``aenter`` runs cleanly."""
    client = Client(HOSTNAME)
    await client.__aexit__(None, None, None)


@pytest.mark.network
async def test_aexit_consecutive_calls() -> None:
    """Test that ``aexit`` runs cleanly when it was already called before."""
    async with Client(HOSTNAME) as client:
        await client.__aexit__(None, None, None)


@pytest.mark.network
async def test_aexit_client_is_already_disconnected_success() -> None:
    """Test that ``aexit`` runs cleanly if client is already cleanly disconnected."""
    async with Client(HOSTNAME) as client:
        client._disconnected.set_result(None)


@pytest.mark.network
async def test_aexit_client_is_already_disconnected_failure() -> None:
    """Test that ``aexit`` reraises if client is already disconnected with an error."""
    client = Client(HOSTNAME)
    await client.__aenter__()
    client._disconnected.set_exception(RuntimeError)
    with pytest.raises(RuntimeError):
        await client.__aexit__(None, None, None)


@pytest.mark.xfail
@pytest.mark.network
async def test_messages_generator_is_reusable() -> None:
    """Test that the messages generator is reusable after dis- and reconnection."""
    topic = TOPIC_PREFIX + "test_messages_generator_is_reusable"
    client = Client(HOSTNAME)
    async with client:
        client._disconnected.set_result(None)
        with pytest.raises(MqttError):
            # TODO(felix): Switch to anext function from Python 3.10
            await client.messages.__anext__()
    async with client:
        await client.subscribe(topic)
        await client.publish(topic, "foo")
        # TODO(felix): Switch to anext function from Python 3.10
        message = await client.messages.__anext__()
        assert message.payload == b"foo"
