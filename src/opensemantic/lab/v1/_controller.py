"""v1 controller classes for opensemantic.lab.

Composes mixin methods with v1 OpcUaDataChannel and OpcUaServer models.
"""

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from pydantic import ConfigDict, PrivateAttr

from opensemantic.lab._controller import ControllerMode, ControllerState  # noqa: F401
from opensemantic.lab._controller_mixin import OpcUaDataChannelMixin, OpcUaServerMixin
from opensemantic.lab.v1._model import OpcUaDataChannel as _OpcUaDataChannel
from opensemantic.lab.v1._model import OpcUaServer as _OpcUaServer

_logger = logging.getLogger(__name__)


class OpcUaDataChannel(OpcUaDataChannelMixin, _OpcUaDataChannel):
    """Enhanced v1 OpcUaDataChannel with uuid5 generation and helper methods."""

    subchannels: Optional[List["OpcUaDataChannel"]] = None


try:
    from asyncua import Client, Server

    from opensemantic.lab._controller import SubscriptionHandler  # noqa: F401

    class OpcUaServer(OpcUaServerMixin, _OpcUaServer):
        """v1 OPC UA server/client controller."""

        model_config = ConfigDict(arbitrary_types_allowed=True)

        url: Optional[str] = None
        subdevices: Optional[List["OpcUaServer"]] = []
        archive_database: Optional[Any] = None
        auto_archive: bool = False
        reset_opcua_connection_on_error: bool = False

        _mode: ControllerMode = PrivateAttr(default=ControllerMode.client)
        _state: ControllerState = PrivateAttr(default=ControllerState.idle)
        _server: Optional[Server] = PrivateAttr(default=None)
        _client: Optional[Client] = PrivateAttr(default=None)
        _subscription_handler: Optional[Any] = PrivateAttr(default=None)
        _channel_datachange_notification_callback: Optional[
            Callable[..., Awaitable[Any]]
        ] = PrivateAttr(default=None)
        _channel_dict: Dict[str, OpcUaDataChannel] = PrivateAttr(default_factory=dict)

except ImportError:
    pass
