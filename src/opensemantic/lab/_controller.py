"""v2 controller classes for opensemantic.lab.

Composes mixin methods with v2 OpcUaDataChannel and OpcUaServer models.
"""

import logging
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, PrivateAttr

from opensemantic.lab._controller_mixin import OpcUaDataChannelMixin, OpcUaServerMixin
from opensemantic.lab._model import OpcUaClientMode  # noqa: F401
from opensemantic.lab._model import OpcUaDataChannel as _OpcUaDataChannel
from opensemantic.lab._model import OpcUaServer as _OpcUaServer

_logger = logging.getLogger(__name__)


class OpcUaDataChannel(OpcUaDataChannelMixin, _OpcUaDataChannel):
    """Enhanced v2 OpcUaDataChannel with uuid5 generation and helper methods."""

    subchannels: Optional[List["OpcUaDataChannel"]] = None


class ControllerMode(str, Enum):
    server = "server"
    client = "client"


class ControllerState(str, Enum):
    idle = "idle"
    running = "running"
    connecting = "connecting"
    connected = "connected"
    reset = "reset"
    stopping = "stopping"
    stopped = "stopped"
    error = "error"


try:
    from asyncua import Client, Node, Server
    from asyncua.common.subscription import DataChangeNotif

    class SubscriptionHandler(BaseModel):
        """Handles OPC UA subscription callbacks."""

        model_config = ConfigDict(arbitrary_types_allowed=True)

        datachange_notification_callback: Optional[
            Callable[[Node, Any, DataChangeNotif], Awaitable[Any]]
        ] = None

        async def datachange_notification(self, node, val, data):
            if self.datachange_notification_callback:
                await self.datachange_notification_callback(node, val, data)

        async def event_notification(self, event):
            pass

        async def status_change_notification(self, status):
            _logger.warning("status_notification %s", status)

    class OpcUaServer(OpcUaServerMixin, _OpcUaServer):
        """v2 OPC UA server/client controller."""

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
        _subscription_handler: Optional[SubscriptionHandler] = PrivateAttr(default=None)
        _channel_datachange_notification_callback: Optional[
            Callable[..., Awaitable[Any]]
        ] = PrivateAttr(default=None)
        _channel_dict: Dict[str, OpcUaDataChannel] = PrivateAttr(default_factory=dict)

except ImportError:
    pass
