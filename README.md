<h1 align="center">The idiomatic async MQTT client 🙌</h1>
<p align="center"><em>(formerly known as asyncio-mqtt)</em></p>
<p align="center">
    <a href="https://github.com/sbtinstruments/aiomqtt/blob/main/LICENSE"><img alt="License: BSD-3-Clause" src="https://img.shields.io/github/license/sbtinstruments/aiomqtt"></a>
    <a href="https://pypi.org/project/aiomqtt"><img alt="PyPI version" src="https://img.shields.io/pypi/v/aiomqtt"></a>
    <a href="https://pypi.org/project/aiomqtt"><img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/aiomqtt.svg"></a>
    <a href="https://pypi.org/project/aiomqtt"><img alt="PyPI downloads" src="https://img.shields.io/pypi/dm/aiomqtt"></a>
    <a href="https://github.com/sbtinstruments/aiomqtt/actions/workflows/test.yml"><img alt="test" src="https://github.com/sbtinstruments/aiomqtt/actions/workflows/test.yml/badge.svg"></a>
    <a href="https://github.com/sbtinstruments/aiomqtt/actions/workflows/docs.yml"><img alt="docs" src="https://github.com/sbtinstruments/aiomqtt/actions/workflows/docs.yml/badge.svg"></a>
    <a href="https://codecov.io/gh/sbtinstruments/aiomqtt"><img alt="Coverage" src="https://img.shields.io/codecov/c/github/sbtinstruments/aiomqtt"></a>
    <a href="https://github.com/sbtinstruments/aiomqtt"><img alt="Typing: strict" src="https://img.shields.io/badge/typing-strict-green.svg"></a>
    <a href="https://github.com/sbtinstruments/aiomqtt"><img alt="Code style: Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/format.json"></a>
    <a href="https://github.com/astral-sh/ruff"><img alt="Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
</p>

<!-- pitch start -->

Write code like this:

**Publish**

```python
async with Client("test.mosquitto.org") as client:
    await client.publish("humidity/outside", payload=0.38)
```

**Subscribe**

```python
async with Client("test.mosquitto.org") as client:
    await client.subscribe("humidity/#")
    async for message in client.messages:
        print(message.payload)
```

aiomqtt combines the stability of the time-proven [paho-mqtt](https://github.com/eclipse/paho.mqtt.python) library with an idiomatic async interface:

- No more callbacks! 👍
- No more return codes (welcome to the `MqttError`)
- Graceful disconnection (forget about `on_unsubscribe`, `on_disconnect`, etc.)
- Supports MQTT versions 5.0, 3.1.1 and 3.1
- Optionally: separate queues per subscription. No more overhead for dispatching.
- Fully type-hinted
- Did we mention no more callbacks?
- Works with asyncio *and* trio! No workarounds required!

<!-- pitch end -->

---

**[Read the documentation at sbtinstruments.github.io/aiomqtt](https://sbtinstruments.github.io/aiomqtt)**

---

<!-- documentation start -->

## Installation

aiomqtt can be installed via `pip install aiomqtt`. The only dependencies are [paho-mqtt](https://github.com/eclipse/paho.mqtt.python) and [anyio](https://github.com/agronholm/anyio).

If you can't wait for the latest version, you can install aiomqtt directly from GitHub with:

`pip install git+https://github.com/sbtinstruments/aiomqtt`

## License

This project is licensed under the [BSD 3-clause License](https://opensource.org/licenses/BSD-3-Clause).

Note that the underlying paho-mqtt library is dual-licensed. One of the licenses is the [Eclipse Distribution License v1.0](https://www.eclipse.org/org/documents/edl-v10.php), which is almost identical to the BSD 3-clause License. The only differences are:

- One use of "COPYRIGHT OWNER" (EDL) instead of "COPYRIGHT HOLDER" (BSD)
- One use of "Eclipse Foundation, Inc." (EDL) instead of "copyright holder" (BSD)

## Contributing

We're very happy about contributions to aiomqtt! ✨ You can get started by reading [CONTRIBUTING.md](https://github.com/sbtinstruments/aiomqtt/blob/main/CONTRIBUTING.md).

## Versioning

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Breaking changes will only occur in major `X.0.0` releases.

## Changelog

The changelog lives in [CHANGELOG.md](https://github.com/sbtinstruments/aiomqtt/blob/main/CHANGELOG.md). It follows the principles of [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Related projects

Is aiomqtt not what you're looking for? There are a few other clients you can try:

- [paho-mqtt](https://github.com/eclipse/paho.mqtt.python): Synchronous client
- [micropython-mqtt](https://github.com/peterhinch/micropython-mqtt): Asynchronous client for microcontrollers in MicroPython
- [gmqtt](https://github.com/wialon/gmqtt): Asynchronous client
- [fastapi-mqtt](https://github.com/sabuhish/fastapi-mqtt): Asynchronous wrapper around gmqtt; Simplifies integration with FastAPI
- [amqtt](https://github.com/Yakifo/amqtt): Asynchronous client; Includes a broker
- [trio-paho-mqtt](https://github.com/bkanuka/trio-paho-mqtt): Asynchronous wrapper around paho-mqtt; Based on trio instead of asyncio
