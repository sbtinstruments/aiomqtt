# SPDX-License-Identifier: BSD-3-Clause
from __future__ import annotations

import asyncio
import dataclasses
import enum
import functools
import logging
import math
import socket
import ssl
import sys
from contextlib import asynccontextmanager, contextmanager
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Iterable,
    Iterator,
    Literal,
    TypeVar,
    cast,
)

import anyio
from paho.mqtt.client import MQTT_CLEAN_START_FIRST_ONLY
from paho.mqtt.enums import (
    CallbackAPIVersion,
    ConnackCode,
    MQTTErrorCode,
    MQTTProtocolVersion,
)
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.subscribeoptions import SubscribeOptions

from . import paho as mqtt
from .event import ValueEvent
from .exceptions import MqttCodeError, MqttConnectError, MqttError, MqttReentrantError
from .message import Message
from .queue import Queue
from .topic import Subscription, Subscriptions, SubscriptionTree
from .types import (
    P,
    PayloadType,
    SocketOption,
    SubscribeTopic,
    T,
    WebSocketHeaders,
    _PahoSocket,
    extract_topics,
)

if sys.version_info >= (3, 11):
    from typing import Concatenate, Self
elif sys.version_info >= (3, 10):
    from typing import Concatenate

    from typing_extensions import Self
else:
    from typing_extensions import Concatenate, Self


MQTT_LOGGER = logging.getLogger("mqtt")
MQTT_LOGGER.setLevel(logging.WARNING)

ClientT = TypeVar("ClientT", bound="Client")


class ProtocolVersion(enum.IntEnum):
    """Map paho-mqtt protocol versions to an Enum for use in type hints."""

    V31 = MQTTProtocolVersion.MQTTv31
    V311 = MQTTProtocolVersion.MQTTv311
    V5 = MQTTProtocolVersion.MQTTv5

@dataclasses.dataclass(frozen=True)
class _GlobalSub:
    queue: Queue

    def enqueue(self, message: Message):
        self.queue.put_nowait(message)


@dataclasses.dataclass(frozen=True)
class TLSParameters:
    ca_certs: str | None = None
    certfile: str | None = None
    keyfile: str | None = None
    cert_reqs: ssl.VerifyMode | None = None
    tls_version: Any | None = None
    ciphers: str | None = None
    keyfile_password: str | None = None


class ProxySettings:
    def __init__(  # noqa: PLR0913
        self,
        *,
        proxy_type: int,
        proxy_addr: str,
        proxy_rdns: bool | None = True,
        proxy_username: str | None = None,
        proxy_password: str | None = None,
    ) -> None:
        self.proxy_args = {
            "proxy_type": proxy_type,
            "proxy_addr": proxy_addr,
            "proxy_rdns": proxy_rdns,
            "proxy_username": proxy_username,
            "proxy_password": proxy_password,
        }


# TODO(frederik): Simplify the logic that surrounds `self._outgoing_calls_sem` with
# `nullcontext` when we support Python 3.10 (`nullcontext` becomes async-aware in
# 3.10). See: https://docs.python.org/3/library/contextlib.html#contextlib.nullcontext
def _outgoing_call(
    method: Callable[Concatenate[ClientT, P], Coroutine[Any, Any, T]],
) -> Callable[Concatenate[ClientT, P], Coroutine[Any, Any, T]]:
    @functools.wraps(method)
    async def decorated(self: ClientT, /, *args: P.args, **kwargs: P.kwargs) -> T:
        if not self._outgoing_calls_sem:
            return await method(self, *args, **kwargs)

        async with self._outgoing_calls_sem:
            return await method(self, *args, **kwargs)

    return decorated


@dataclasses.dataclass(frozen=True)
class Will:
    topic: str
    payload: PayloadType | None = None
    qos: int = 0
    retain: bool = False
    properties: Properties | None = None


class Client:
    """The async context manager that manages the connection to the broker.

    Args:
        hostname: The hostname or IP address of the remote broker.
        port: The network port of the remote broker.
        username: The username to authenticate with.
        password: The password to authenticate with.
        logger: Custom logger instance.
        identifier: The client identifier. Generated automatically if ``None``.
        queue_type: The class to use for the queue.
        protocol: The version of the MQTT protocol.
        will: The will message to publish if the client disconnects unexpectedly.
        clean_session: If ``True``, the broker will remove all information about this
            client when it disconnects. If ``False``, the client is a persistent client
            and subscription information and queued messages will be retained when the
            client disconnects.
        transport: The transport protocol to use. Either ``"tcp"`` or ``"websockets"``.
        timeout: The default timeout for all communication with the broker in seconds.
        keepalive: The keepalive timeout for the client in seconds.
        bind_address: The IP address of a local network interface to bind this client
            to.
        bind_port: The network port to bind this client to.
        clean_start: (MQTT v5.0 only) Set the clean start flag always, never, or only
            on the first successful connection to the broker.
        max_queued_incoming_messages: Restricts the incoming message queue size. If the
            queue is full, further incoming messages are discarded. ``0`` or less means
            unlimited (the default).
        max_queued_outgoing_messages: Resticts the outgoing message queue size. If the
            queue is full, further outgoing messages are discarded. ``0`` means
            unlimited (the default).
        max_inflight_messages: The maximum number of messages with QoS > ``0`` that can
            be part way through their network flow at once.
        max_concurrent_outgoing_calls: The maximum number of concurrent outgoing calls.
        properties: (MQTT v5.0 only) The properties associated with the client.
        tls_context: The SSL/TLS context.
        tls_params: The SSL/TLS configuration to use.
        tls_insecure: Enable/disable server hostname verification when using SSL/TLS.
        proxy: Configure a proxy for the connection.
        socket_options: Options to pass to the underlying socket.
        websocket_path: The path to use for websockets.
        websocket_headers: The headers to use for websockets.

    Attributes:
        messages (typing.AsyncGenerator[aiomqtt.client.Message, None]):
            Async generator that yields messages from the underlying message queue.
        identifier (str):
            The client identifier.
    """

    def __init__(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        hostname: str,
        port: int = 1883,
        *,
        username: str | None = None,
        password: str | None = None,
        logger: logging.Logger | None = None,
        identifier: str | None = None,
        queue_type: type[Queue[Message]] | None = None,
        protocol: ProtocolVersion | None = None,
        will: Will | None = None,
        clean_session: bool | None = None,
        transport: Literal["tcp", "websockets"] = "tcp",
        timeout: float | None = None,
        keepalive: int = 60,
        bind_address: str = "",
        bind_port: int = 0,
        clean_start: mqtt.CleanStartOption = MQTT_CLEAN_START_FIRST_ONLY,
        max_queued_incoming_messages: int | None = None,
        max_queued_outgoing_messages: int | None = None,
        max_inflight_messages: int | None = None,
        max_concurrent_outgoing_calls: int | None = None,
        properties: Properties | None = None,
        tls_context: ssl.SSLContext | None = None,
        tls_params: TLSParameters | None = None,
        tls_insecure: bool | None = None,
        proxy: ProxySettings | None = None,
        socket_options: Iterable[SocketOption] | None = None,
        websocket_path: str | None = None,
        websocket_headers: WebSocketHeaders | None = None,
    ) -> None:
        self._hostname = hostname
        self._port = port
        self._keepalive = keepalive
        self._bind_address = bind_address
        self._bind_port = bind_port
        self._clean_start = clean_start
        self._properties = properties

        # Connection state
        self._connected: ValueEvent = ValueEvent()
        self._disconnected: ValueEvent = ValueEvent()

        # Pending subscribe, unsubscribe, and publish calls
        self._pending_subscribes: dict[
            int, ValueEvent[ReasonCode]
        ] = {}
        self._pending_unsubscribes: dict[int, Event] = {}
        self._pending_publishes: dict[int, Event] = {}
        self.pending_calls_threshold: int = 10

        # Queue that holds incoming messages
        if queue_type is None:
            queue_type = Queue
        self._queue_type = queue_type

        if max_queued_incoming_messages is None:
            max_queued_incoming_messages = 10000
        self._max_queued_incoming_messages = max_queued_incoming_messages

        # Semaphore to limit the number of concurrent outgoing calls
        self._outgoing_calls_sem: anyio.Semaphore | None
        if max_concurrent_outgoing_calls is not None:
            self._outgoing_calls_sem = anyio.Semaphore(max_concurrent_outgoing_calls)
        else:
            self._outgoing_calls_sem = None

        if protocol is None:
            protocol = ProtocolVersion.V311

        # Create the underlying paho-mqtt client instance
        self._client: mqtt.Client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=identifier,  # type: ignore[arg-type]
            protocol=protocol,
            clean_session=clean_session,
            transport=transport,
            reconnect_on_failure=False,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_subscribe = self._on_subscribe
        self._client.on_unsubscribe = self._on_unsubscribe
        self._client.on_message = self._on_message
        self._client.on_publish = self._on_publish

        if max_inflight_messages is not None:
            self._client.max_inflight_messages_set(max_inflight_messages)
        if max_queued_outgoing_messages is not None:
            self._client.max_queued_messages_set(max_queued_outgoing_messages)

        if logger is None:
            logger = MQTT_LOGGER
        self._logger = logger
        self._client.enable_logger(logger)

        if username is not None:
            self._client.username_pw_set(username=username, password=password)

        if tls_context is not None:
            self._client.tls_set_context(tls_context)

        if tls_params is not None:
            self._client.tls_set(
                ca_certs=tls_params.ca_certs,
                certfile=tls_params.certfile,
                keyfile=tls_params.keyfile,
                cert_reqs=tls_params.cert_reqs,
                tls_version=tls_params.tls_version,
                ciphers=tls_params.ciphers,
                keyfile_password=tls_params.keyfile_password,
            )

        if tls_insecure is not None:
            self._client.tls_insecure_set(tls_insecure)

        if proxy is not None:
            self._client.proxy_set(**proxy.proxy_args)

        if websocket_path is not None:
            self._client.ws_set_options(path=websocket_path, headers=websocket_headers)

        if will is not None:
            self._client.will_set(
                will.topic, will.payload, will.qos, will.retain, will.properties
            )

        if socket_options is None:
            socket_options = ()
        self._socket_options = tuple(socket_options)

        if timeout is None:
            timeout = 10
        self.timeout = timeout

    @property
    def identifier(self) -> str:
        """Return the client identifier.

        Note that paho-mqtt stores the client ID as `bytes` internally. We assume that
        the client ID is a UTF8-encoded string and decode it first.
        """
        return self._client._client_id.decode()  # noqa: SLF001

    @property
    def _pending_calls(self) -> Generator[int, None, None]:
        """Yield all message IDs with pending calls."""
        yield from self._pending_subscribes.keys()
        yield from self._pending_unsubscribes.keys()
        yield from self._pending_publishes.keys()

    @_outgoing_call
    async def _subscribe(  # noqa: PLR0913
        self,
        /,
        global_queue: bool,
        topic: SubscribeTopic,
        qos: int = 0,
        options: SubscribeOptions | None = None,
        properties: Properties | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[int, ...] | list[ReasonCode]:
        result, mid = self._client.subscribe(
            topic, qos, options, properties, *args, **kwargs
        )
        # Early out on error
        if result != MQTTErrorCode.MQTT_ERR_SUCCESS or mid is None:
            raise MqttCodeError(result, "Could not subscribe to topic")

        if global_queue:
            for top in extract_topics(topic):
                if top in self._subscriptions:
                    continue
                sub = Subscription(top, self._queue)
                self._tree.attach(sub)
                self._subscriptions[top] = sub

        # Create future for when the on_subscribe callback is called
        callback_result = ValueEvent()
        with self._pending_call(mid, callback_result, self._pending_subscribes):
            # Wait for callback_result
            return await callback_result.wait()


    @asynccontextmanager
    async def subscription(
        self,
        /,
        topic: SubscribeTopic,
        qos: int = 0,
        options: SubscribeOptions | None = None,
        properties: Properties | None = None,
        *args: Any,

        queue: Queue = None,
        **kwargs: Any,
    ):
        """Subscribe to a topic or wildcard.

        Args:
            topic: The topic or wildcard to subscribe to.
            qos: The requested QoS level for the subscription.
            options: (MQTT v5.0 only) Optional paho-mqtt subscription options.
            properties: (MQTT v5.0 only) Optional paho-mqtt properties.
            *args: Additional positional arguments to pass to paho-mqtt's subscribe
                method.
            **kwargs: Additional keyword arguments to pass to paho-mqtt's subscribe
                method.

            queue (Queue):
                A queue to send the messages to. By default a local queue
                is generated.

        This is a context manager. Iterate on the result to read the
        messages::

            async with client.subscription(topic) as sub:
                async for msg in sub:
                    ...
        """
        with Subscriptions(topic, queue=queue).subscribed_to(self._tree) as sub:
            try:
                self._sub_q[sub.sub_id] = sub
                if self._with_sub_ids:
                    if properties is None:
                        properties = Properties(PacketTypes.SUBSCRIBE)
                    properties.SubscriptionIdentifier = sub.sub_id
                await self._subscribe(False, topic, qos, options, properties, *args, **kwargs)
                yield sub
            finally:
                del self._sub_q[sub.sub_id]
                await self.unsubscribe(list(extract_topics(topic)))


    async def subscribe(
        self,
        /,
        topic: SubscribeTopic,
        qos: int = 0,
        options: SubscribeOptions | None = None,
        properties: Properties | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[int, ...] | list[ReasonCode]:
        """Subscribe to a topic or wildcard.

        Args:
            topic: The topic or wildcard to subscribe to.
            qos: The requested QoS level for the subscription.
            options: (MQTT v5.0 only) Optional paho-mqtt subscription options.
            properties: (MQTT v5.0 only) Optional paho-mqtt properties.
            *args: Additional positional arguments to pass to paho-mqtt's subscribe
                method.
            **kwargs: Additional keyword arguments to pass to paho-mqtt's subscribe
                method.

        The desired messages will be delivered via the `Client.messages` iterator.
        """
        if self._with_sub_ids:
            if properties is None:
                properties = Properties(PacketTypes.SUBSCRIBE)
            properties.SubscriptionIdentifier = 1
        return await self._subscribe(True, topic, qos, options,
                properties, *args, **kwargs)

    @_outgoing_call
    async def unsubscribe(
        self,
        /,
        topic: str | list[str],
        properties: Properties | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Unsubscribe from a topic or wildcard.

        Args:
            topic: The topic(s) or wildcard(s) to unsubscribe from.
            properties: (MQTT v5.0 only) Optional paho-mqtt properties.
            *args: Additional positional arguments to pass to paho-mqtt's unsubscribe
                method.
            **kwargs: Additional keyword arguments to pass to paho-mqtt's unsubscribe
                method.
        """
        result, mid = self._client.unsubscribe(topic, properties, *args, **kwargs)  # type: ignore[arg-type]
        # Early out on error
        if result != MQTTErrorCode.MQTT_ERR_SUCCESS or mid is None:
            raise MqttCodeError(result, "Could not unsubscribe from topic")
        # Create event for when the on_unsubscribe callback is called

        if isinstance(topic,str):
            topic = [topic]
        for top in topic:
            sub = self._subscriptions.pop(top, None)
            if sub is not None:
                self._tree.detach(sub)

        # Create future for when the on_unsubscribe callback is called
        confirmation = anyio.Event()
        with self._pending_call(mid, confirmation, self._pending_unsubscribes):
            # Wait for confirmation
            await confirmation.wait()

    @_outgoing_call
    async def publish(  # noqa: PLR0913
        self,
        /,
        topic: str,
        payload: PayloadType = None,
        qos: int = 0,
        retain: bool = False,
        properties: Properties | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Publish a message to the broker.

        Args:
            topic: The topic to publish to.
            payload: The message payload.
            qos: The QoS level to use for publication.
            retain: If set to ``True``, the message will be retained by the broker.
            properties: (MQTT v5.0 only) Optional paho-mqtt properties.
            *args: Additional positional arguments to pass to paho-mqtt's publish
                method.
            **kwargs: Additional keyword arguments to pass to paho-mqtt's publish
                method.
        """
        # There may be no `await` before the one that awaits confirmation
        info = self._client.publish(
            topic, payload, qos, retain, properties, *args, **kwargs
        )
        # Early out on error
        if info.rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
            raise MqttCodeError(info.rc, "Could not publish message")
        # Early out on immediate success
        if info.is_published():
            return
        # Create event for when the on_publish callback is called
        confirmation = anyio.Event()
        with self._pending_call(info.mid, confirmation, self._pending_publishes):
            # Wait for confirmation
            await confirmation.wait()

    @property
    async def messages(self) -> AsyncGenerator[Message, None]:
        """Async generator that yields messages from the common message queue.

        Raises `anyio.IncompleteRead` on overflow.

        Usage::

            async with aclosing(client.messages) as msgs:
                async for msg in msgs:
                    await process(msg)
        """
        while True:
            try:
                yield await self._queue.get()
            except StopAsyncIteration:
                raise anyio.IncompleteRead()


    @contextmanager
    def _pending_call(
        self, mid: int, value: T, pending_dict: dict[int, T]
    ) -> Iterator[None]:
        if mid in self._pending_calls:
            msg = f'There already exists a pending call for message ID "{mid}"'
            raise RuntimeError(msg)
        pending_dict[mid] = value  # [1]
        try:
            # Log a warning if there is a concerning number of pending calls
            pending = len(list(self._pending_calls))
            if pending > self.pending_calls_threshold:
                self._logger.warning("There are %d pending publish calls.", pending)
            # Back to the caller (run whatever is inside the with statement)
            yield
        finally:
            # The normal procedure is:
            #  * We add the item at [1]
            #  * A callback will remove the item
            #
            # However, if the callback doesn't get called (e.g., due to a
            # network error) we still need to remove the item from the dict.
            try:
                del pending_dict[mid]
            except KeyError:
                pass

    def _on_connect(  # noqa: PLR0913
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, int],
        rc: ReasonCode,
        properties: Properties | None = None,
    ) -> None:
        """Called when we receive a CONNACK message from the broker."""
        # Return early if already connected. Sometimes, paho-mqtt calls _on_connect
        # multiple times. Maybe because we receive multiple CONNACK messages
        # from the server. In any case, we return early so that we don't set
        # self._connected twice

        self._with_sub_ids = getattr(properties,"SubscriptionIdentifierAvailable",1)
        if self._connected.is_set():
            return
        if rc.value < 6:
            self._connected.set(None)
        else:
            # We received a negative CONNACK response
            self._connected.set_error(MqttConnectError(rc))

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: int,
        rc: ReasonCode | None,
        properties: Properties | None = None,
    ) -> None:
        # Return early if the disconnect is already acknowledged.
        # Sometimes (e.g., due to timeouts), paho-mqtt calls _on_disconnect
        # twice. We return early to avoid setting self._disconnected twice
        if self._disconnected.is_set():
            return

        if rc is None or rc.value < 6:
            self._disconnected.set(None)
        else:
            self._disconnected.set_error(
                MqttCodeError(rc, "Unexpected disconnection")
            )

        self._connected = ValueEvent()

    def _on_subscribe(  # noqa: PLR0913
        self,
        client: mqtt.Client,
        userdata: Any,
        mid: int,
        granted_qos: tuple[int, ...] | list[ReasonCode],
        properties: Properties | None = None,
    ) -> None:
        """Called when we receive a SUBACK message from the broker."""
        try:
            fut = self._pending_subscribes.pop(mid)
            fut.set(granted_qos)
        except KeyError:
            self._logger.exception(
                'Unexpected message ID "%d" in on_subscribe callback', mid
            )

    def _on_unsubscribe(  # noqa: PLR0913
        self,
        client: mqtt.Client,
        userdata: Any,
        mid: int,
        properties: Properties | None = None,
        reason_codes: list[ReasonCode] | ReasonCode | None = None,
    ) -> None:
        """Called when we receive an UNSUBACK message from the broker."""
        try:
            self._pending_unsubscribes.pop(mid).set()
        except KeyError:
            self._logger.exception(
                'Unexpected message ID "%d" in on_unsubscribe callback', mid
            )

    def _on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        # Convert the paho.mqtt message into our own Message type
        m = Message._from_paho_message(message)  # noqa: SLF001
        # Put the message in the message queue
        if self._with_sub_ids:
            for sub_id in (1,) if m.properties is None or not m.properties.SubscriptionIdentifier else m.properties.SubscriptionIdentifier:
                try:
                    self._sub_q[sub_id].enqueue(m)
                except (anyio.WouldBlock, asyncio.QueueFull):
                    # the global queue discards
                    if id != 1:
                        self._sub_q[id].close_writer()
                        del self._sub_q[id]
                except KeyError:
                    pass
        else:
            self._tree.dispatch(m)

    def _on_publish(self, client: mqtt.Client, userdata: Any, mid: int,
            reason: ReasonCode, props: Properties) -> None:
        try:
            self._pending_publishes.pop(mid).set()
        except KeyError:
            # Do nothing since [2] may call on_publish before it even returns.
            # That is, the message may already be published before we even get a
            # chance to set up the 'pending_call' logic.
            pass

    __ctx = None

    async def __aenter__(self):
        if self.__ctx is not None:
            raise MqttReentrantError
        ctx = self._ctx()
        res = await ctx.__aenter__()
        self.__ctx = ctx
        return res

    def __aexit__(self, *tb):
        try:
            return self.__ctx.__aexit__(*tb)
        finally:
            self.__ctx = None

    @asynccontextmanager
    async def _ctx(self):
        if self._disconnected.is_set():
            self._disconnected = ValueEvent()
        self._connected = ValueEvent()

        exc = None

        self._queue = self._queue_type(self._max_queued_incoming_messages)
        self._tree = SubscriptionTree()
        self._subscriptions: dict[str, Subscription] = {}
        self._sub_q:dict[int,_GlobalSub|Subscriptions] = {1: _GlobalSub(self._queue)}

        try:
            with anyio.fail_after(self.timeout) as timer:
                async with self._client.connect(
                        self._hostname,
                        self._port,
                        self._keepalive,
                        self._bind_address,
                        self._bind_port,
                        self._clean_start,
                        self._properties,
                        ):

                    try:
                        await self._connected.get()
                    except Exception as err:
                        exc = err
                        raise

                    timer.deadline = float('inf')
                    yield self

                    rc = self._client.disconnect()
                    if rc == MQTTErrorCode.MQTT_ERR_SUCCESS:
                        # Wait for acknowledgement
                        await self._disconnected.get()

        except ExceptionGroup as exc2:
            # If the original error has been wrapped in an exception group,
            # raise it instead.
            #
            # This typically happens when the server closes the connection
            # after sending an error. We thus get the MQTT error from the
            # disconnect packet *and* the EOFError because it closed the
            # connection.

            if exc is not None:
                raise exc
            raise

        finally:
            self._queue.close_reader()
            self._queue.close_writer()





def _set_client_socket_defaults(
    client_socket: _PahoSocket | None, socket_options: Iterable[SocketOption]
) -> None:
    # Note that socket may be None if, e.g., the username and
    # password combination didn't work. In this case, we return early.
    if client_socket is None:
        return
    # Furthermore, paho sometimes gives us a socket wrapper instead of
    # the raw socket. E.g., for WebSocket-based connections.
    if not isinstance(client_socket, socket.socket):
        return
    # At this point, we know that we got an actual socket. We change
    # some of the default options.
    for socket_option in socket_options:
        client_socket.setsockopt(*socket_option)
