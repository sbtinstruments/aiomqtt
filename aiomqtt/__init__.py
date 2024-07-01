# SPDX-License-Identifier: BSD-3-Clause
from .client import (
    Client,
    ProtocolVersion,
    ProxySettings,
    TLSParameters,
    Will,
)
from .exceptions import MqttCodeError, MqttError, MqttReentrantError
from .message import Message
from .router import Router
from .topic import Topic, TopicLike, Wildcard, WildcardLike

# These are placeholders that are managed by poetry-dynamic-versioning
__version__ = "0.0.0"
__version_tuple__ = (0, 0, 0)

__all__ = [
    "__version__",
    "__version_tuple__",
    "Client",
    "Message",
    "Router",
    "ProtocolVersion",
    "ProxySettings",
    "TLSParameters",
    "Topic",
    "TopicLike",
    "Wildcard",
    "WildcardLike",
    "Will",
    "MqttCodeError",
    "MqttReentrantError",
    "MqttError",
]
