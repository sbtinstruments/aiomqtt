# SPDX-License-Identifier: BSD-3-Clause
from __future__ import annotations

import logging
import math
import ssl
import sys
import time
from contextlib import asynccontextmanager, nullcontext
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
import anyio.streams.buffered
import anyio.streams.stapled
import paho.mqtt.client as mqtt
from paho.mqtt.enums import (
    CallbackAPIVersion,
    MessageType,
    MQTTErrorCode,
    _ConnectionState,
)
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from .event import ValueEvent
from .exceptions import MqttCodeError, MqttConnectError, MqttError
from .queue import Queue

if sys.version_info >= (3, 11):
    from typing import Concatenate, Self
elif sys.version_info >= (3, 10):
    from typing import Concatenate

    from typing_extensions import Self
else:
    from typing_extensions import Concatenate, Self

_null_ctx = nullcontext()

logger = logging.getLogger(__name__)

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time

class _UngroupExc:
    """A sync+async context manager that unwraps single-element
    exception groups.
    """
    def __call__(self):
        "Singleton. Returns itself."
        return self

    def one(self, e):
        "Convert the exceptiongroup @e to a single exception"
        while isinstance(e, BaseExceptionGroup):
            if len(e.exceptions) != 1:
                break
            e = e.exceptions[0]
        return e

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self

    def __exit__(self, c, e, t):
        if e is None:
            return
        e = self.one(e)
        raise e

    async def __aexit__(self, c, e, t):
        return self.__exit__(c, e, t)

ungroup_exc = _UngroupExc()


class MQTTMessageInfo(mqtt.MQTTMessageInfo):
    __slots__ = 'mid', '_published', '_condition', 'rc', '_iterpos', '_event'


    def __init__(self, mid:int):
        self.mid = mid
        self._published = False
        self._condition = _null_ctx
        self._event = anyio.Event()
        self._iterpos = 0

    async def wait(self):
        await self._event.wait()

    def wait_for_publish(self, timeout=None):
        raise RuntimeError("Superseded")

    def _set_as_published(self) -> None:
        self._published = True
        self._event.set()

mqtt.MQTTMessageInfo = MQTTMessageInfo


class Client(mqtt.Client):
    """This is a subclass of the paho.mqtt client that uses async tasks
    instead of threads.

    See `paho.mqtt.client.Client` for usage details.
    """
    _close_ok = False
    _want_reconnect = False
    _did_work = False

    def __init__(self,*a,**k):
        super().__init__(*a,**k)

        del self._msgtime_mutex
        del self._reconnect_delay_mutex
        del self._thread
        del self._out_packet

        self._mid_generate_mutex = _null_ctx
        self._in_callback_mutex = _null_ctx
        self._out_message_mutex = _null_ctx
        self._in_message_mutex = _null_ctx

    @asynccontextmanager
    async def _connect_once(self):
        self._connected = ValueEvent()

        context = self._ssl_context if self._ssl else None
        client = None
        rc = MQTTErrorCode.MQTT_ERR_NO_CONN

        self._last_msg_in = self._last_msg_out = time_func()

        # Put messages in progress in a valid state.
        self._messages_reconnect_reset()

        on_pre_connect = self.on_pre_connect
        if on_pre_connect:
            try:
                on_pre_connect(self, self._userdata)
            except Exception as err:
                self._easy_log(
                    MQTT_LOG_ERR, 'Caught exception in on_pre_connect: %s', err)
                if not self.suppress_exceptions:
                    raise

        def _conn():
            if self._transport == "unix":
                if ssl_context is not None:
                    # Trio could do it *sigh*
                    raise RuntimeError("we cannot use SSL over a Unix socket")
                return anyio.connect_unix(self._host)
            else:
                # local port is not supported *sigh*
                if self._bind_port > 0:
                    raise RuntimeError("Bind to a specific port is not supported")
                return anyio.connect_tcp(self._host, self._port, local_host=self._bind_address, ssl_context=context)

        try:
            async with (
                    ungroup_exc,
                    await _conn() as client,
                    anyio.create_task_group() as tg,
                    ):
                if self._transport == "websockets":
                    ctx = WebSock(client, self._host, self._websocket_path, self._websocket_extra_headers)
                elif self._transport in ("tcp","unix"):
                    ctx = nullcontext(anyio.streams.stapled.StapledByteStream(
                        client,
                        anyio.streams.buffered.BufferedByteReceiveStream(client),
                        ))
                else:
                    raise ValueError("Transport must be 'websockets' or 'tcp'")
                async with ctx as client:
                    self._call_socket_open(client)
                    self._sock = client
                    tg.start_soon(self._loop_read)
                    tg.start_soon(self._loop_write)

                    self._send_connect(self._keepalive)
                    await self._connected.get()
                    self._did_work = True
                    yield self
        except (anyio.EndOfStream, EOFError) as err:
            if self._state != _ConnectionState.MQTT_CS_DISCONNECTING:
                raise
            rc = MQTTErrorCode.MQTT_ERR_SUCCESS
        except TimeoutError:
            rc = MQTTErrorCode.MQTT_ERR_KEEPALIVE
        else:
            rc = MQTTErrorCode.MQTT_ERR_SUCCESS
        finally:
            self._do_on_disconnect(packet_from_broker=False, v1_rc=rc)
            if client is not None:
                self._call_socket_close(client)
            self._sock = None

    async def wait_connected(self):
        await self._connected.get()

    async def loop_reconnect(self, task_status=anyio.TASK_STATUS_IGNORED):
        while True:
            try:
                async with self._connect_once():
                    task_status.started()
                    task_status = anyio.TASK_STATUS_IGNORED
                    await anyio.sleep_forever()

            except (anyio.BrokenResourceError,OSError,EOFError,anyio.EndOfStream) as exc:
                # flush packet queue
                if self._want_reconnect:
                    self._want_reconnect = False
                    continue
                if not self._did_work:
                    raise

                pkts = []
                while True:
                    try:
                        pkt = self._out_q.get_nowait()
                    except anyio.WouldBlock:
                        break
                    else:
                        if pkt["command"] & 0xF0 == PUBLISH and pkt["qos"] == 0 and pkt["info"] is not None:
                            pkt["info"].rc = MQTT_ERR_CONN_LOST
                            pkt["info"]._set_as_published()
                        else:
                            pkts.append(pkt)

                for pkt in pkts:
                    self._out_q.put_nowait(pkt)

            if self._state == _ConnectionState.MQTT_CS_DISCONNECTING:
                break
            if self._state == _ConnectionState.MQTT_CS_DISCONNECTED:
                break

            await self._reconnect_wait()


    async def _reconnect_wait(self) -> None:
        # See reconnect_delay_set for details
        if self._reconnect_delay is None:
            self._reconnect_delay = self._reconnect_min_delay
        else:
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._reconnect_max_delay,
            )
        await anyio.sleep(self._reconnect_delay)


    def _handle_connack(self):
        res = super()._handle_connack()
        self._connected.set(res)

    @asynccontextmanager
    async def connect(self, *a, **k):
        self._out_q = Queue(999)
        self._out_p = None  # packet to retransmit
        self._connected = ValueEvent()

        try:
            self._close_ok = True
            self.connect_async(*a, **k)
        finally:
            self._close_ok = False
        try:
            async with anyio.create_task_group() as tg:
                await tg.start(self.loop_reconnect)
                yield self
                tg.cancel_scope.cancel()
        except BaseExceptionGroup as exc:
            m,r = exc.split(GeneratorExit)
            raise r
        finally:
            self._out_q.close_writer()
            self._out_q.close_reader()


    def _sock_close(self):
        if not self._close_ok:
            raise RuntimeError("Superseded")

    def _reset_sockets(self):
        # called from __del__ thus no error raised
        # raise RuntimeError("Superseded")
        pass

    def _packet_read(self):
        raise RuntimeError("Superseded")

    def _packet_write(self):
        raise RuntimeError("Superseded")

    def _sock_send(self, data):
        raise RuntimeError("Superseded")

    def _packet_read(self):
        raise RuntimeError("Superseded")

    def loop_read(self, max_packets=1):
        raise RuntimeError("Superseded")

    def loop_write(self):
        pass  # no-op

    def loop_forever(self):
        raise RuntimeError("Superseded")

    def reconnect(self):
        self._want_reconnect = True

    def _packet_queue(
        self,
        command: int,
        packet: bytes,
        mid: int,
        qos: int,
        info: MQTTMessageInfo | None = None,
    ) -> MQTTErrorCode:
        mpkt: _OutPacket = {
            "command": command,
            "mid": mid,
            "qos": qos,
            "packet": packet,
            "info": info,
        }
        self._out_q.put_nowait(mpkt)
        return MQTTErrorCode.MQTT_ERR_SUCCESS

    async def _recv_one(self):
        pkt = {
            "command": 0,
            "remaining_length": 0,
            "packet": bytearray(b""),
            "pos": 0,
        }
        pkt['command'] = (await self._sock.receive(1))[0]

        length = 0
        while True:
            byte = (await self._sock.receive(1))[0]
            length = (length<<7) | (byte & 127)
            if not (byte & 128):
                break

        pkt_data = []
        pkt['remaining_length'] = length

        while length:
            data = await self._sock.receive(length)
            pkt_data.append(data)
            length -= len(data)

        pkt['packet'] = b''.join(pkt_data)

        self._packet_seen = time_func()
        return pkt


    async def _loop_read(self):
        while True:
            self._in_packet = await self._recv_one()
            self._packet_handle()


    async def _loop_write(self):
        if self._out_p is not None:
            packet,self._out_p = self._out_p,None
            rc = await self._send_one(packet)
            if rc is not None:
                return rc

        self._ping_t = 0
        while True:
            try:
                with anyio.fail_after(self._keepalive):
                    packet = await self._out_q.get()
            except TimeoutError:
                if self._ping_t:
                    raise
                self._send_pingreq()
                self._ping_t = time_func()
            else:
                rc = await self._send_one(packet)
                if rc is not None:
                    return rc

    async def _send_one(self, packet):
        try:
            await self._sock.send(packet['packet'])
        except OSError as err:
            self._out_p = packet
            self._easy_log(
                    MQTT_LOG_ERR, 'failed to receive on socket: %s', err)
            return MQTTErrorCode.MQTT_ERR_CONN_LOST
        else:
            if (packet['command'] & 0xF0) == MessageType.PUBLISH and packet['qos'] == 0:
                on_publish = self.on_publish

                if on_publish:
                    try:
                        if self._callback_api_version == CallbackAPIVersion.VERSION1:
                            on_publish = cast(mqtt.CallbackOnPublish_v1, on_publish)

                            on_publish(self, self._userdata, packet["mid"])
                        elif self._callback_api_version == CallbackAPIVersion.VERSION2:
                            on_publish = cast(mqtt.CallbackOnPublish_v2, on_publish)

                            on_publish(
                                self,
                                self._userdata,
                                packet["mid"],
                                ReasonCode(PacketTypes.PUBACK),
                                Properties(PacketTypes.PUBACK),
                            )
                        else:
                            raise RuntimeError("Unsupported callback API version")
                    except Exception as err:
                        raise
                        self._easy_log(
                            MQTT_LOG_ERR, 'Caught exception in on_publish: %s', err)
                        if not self.suppress_exceptions:
                            raise

                # TODO: Something is odd here. I don't see why packet["info"] can't beNone.
                # A packet could be produced by _handle_connack with qos=0 and no info
                # (around line 3645). Ignore the mypy check for now but I feel there is a bug
                # somewhere.
                packet['info']._set_as_published()  # type: ignore

            return None


    async def loop_forever(
        self,
        retry_first_connection: bool = False,
    ) -> MQTTErrorCode:
        """This function calls the network loop functions for you in an
        infinite blocking loop. It is useful for the case where you only want
        to run the MQTT client loop in your program.

        loop_forever() will handle reconnecting for you if reconnect_on_failure is
        true (this is the default behavior). If you call `disconnect()` in a callback
        it will return.

        :param bool retry_first_connection: Should the first connection attempt be retried on failure.
          This is independent of the reconnect_on_failure setting.

        :raises OSError: if the first connection fail unless retry_first_connection=True
        """
        run = True

        while run:
            if self._state == _ConnectionState.MQTT_CS_CONNECT_ASYNC:
                try:
                    self.reconnect()
                except OSError:
                    self._handle_on_connect_fail()
                    if not retry_first_connection:
                        raise
                    self._easy_log(
                        MQTT_LOG_DEBUG, "Connection failed, retrying")
                    await self._reconnect_wait()
            else:
                break

        def should_exit() -> bool:
            return self._state in (_ConnectionState.MQTT_CS_DISCONNECTING, _ConnectionState.MQTT_CS_DISCONNECTED)

        while True:
            rc = MQTTErrorCode.MQTT_ERR_SUCCESS
            while rc == MQTTErrorCode.MQTT_ERR_SUCCESS:
                rc = await self._loop()

            if should_exit() or not self._reconnect_on_failure:
                break

            await self._reconnect_wait()

            if should_exit():
                break
            try:
                await self.reconnect()
            except OSError:
                self._handle_on_connect_fail()
                self._easy_log(MQTT_LOG_DEBUG, "Connection failed, retrying")

        return rc


wsproto = None
WSConnection = None

class WebSock(anyio.abc.ByteStream):
    def __init__(self, sock, host, path = "/ws", headers = {}):
        self._sock = sock
        self._host = host
        self._path = path
        self._headers = headers
        self._rbuf = bytearray()
        self._lock = anyio.Lock()
        self._connected = None

        global wsproto
        if wsproto is None:
            import wsproto

    async def _xmit(self, pkt):
        async with self._lock:
            await self._sock.send(self._ws.send(pkt))

    async def _evts(self):
        data = await self._sock.receive()
        self._ws.receive_data(data)

        for event in self._ws.events():
            if isinstance(event, wsproto.events.AcceptConnection):
                self._connected = True
            elif isinstance(event, wsproto.events.RejectConnection):
                self._connected = False
                logger.warning("Rejected: %s", event.data)
            elif isinstance(event, wsproto.events.TextMessage):
                print(f"Received message: {event.data}")
            elif isinstance(event, wsproto.events.BytesMessage):
                self._rbuf += event.data
                print(f"Received bytes: {event.data}")
            elif isinstance(event, wsproto.events.Ping):
                print(f"Received ping: {event.payload!r}")
            elif isinstance(event, wsproto.events.Pong):
                print(f"Received pong: {event.payload!r}")
            elif isinstance(event, wsproto.events.CloseConnection):
                if self._connected is not None:
                    self._connected = None
                    await self._sock.send(self._ws.send(event.response()))
                await self._sock.aclose()
                self._sock = None
                return
            else:
                raise RuntimeError("Do not know how to handle event: " + str(event))

            if hasattr(event, 'response'):
                await self._ws.send(event.response())

    async def __aenter__(self):
        self._ws = wsproto.WSConnection(wsproto.connection.CLIENT)
        #self._ws.initiate_upgrade_connection(headers=self._headers, path=self._path)

        await self._xmit(wsproto.events.Request(host=self._host,
            target=self._path, extra_headers=list(self._headers.items()),
            subprotocols=["mqtt"]))

        while self._connected is None:
            await self._evts()

        if not self._connected:
            raise anyio.BrokenResourceError

        return self

    async def __aexit__(self, *err):
        await self.aclose()

    async def send(self, bytes):
        await self._sock.send(self._ws.send(wsproto.events.BytesMessage(data=bytes)))

    async def receive(self, max_bytes=None):
        while not self._rbuf:
            if self._sock is None:
                raise EOFError
            await self._evts()

        res = self._rbuf[:max_bytes]
        self._rbuf[:max_bytes] = b''
        return res

    async def aclose(self):
        if self._sock is None:
            return

        self._connected = None
        await self._sock.send(self._ws.send(wsproto.events.CloseConnection(code=0)))
        while self._sock is not None:
            await self._evts()

    async def send_eof(self):
        pass

