"""Mixin classes for OPC UA controller methods.

OpcUaServerMixin inherits generic DataTool features (channel mgmt, archiving,
identity) from DataToolMixin and adds OPC UA-specific protocol logic.
"""

import asyncio
import datetime
import logging
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from opensemantic.base._controller_mixin import DataToolMixin
from opensemantic.lab._controller_logic import (
    calculate_queue_size,
    get_interval_ms,
    get_opcua_initial_value,
    group_channels_by_interval,
)

_logger = logging.getLogger(__name__)

_OPCUA_UUID_NAMESPACE = UUID("35a708e3-aee9-44d7-abe7-fe3f85362def")


class OpcUaDataChannelMixin:
    """Mixin for OpcUaDataChannel controller methods."""

    def __init__(self, **data):
        if "uuid" not in data:
            raise ValueError(
                "uuid is required for OpcUaDataChannel. It must be set explicitly "
                "or computed deterministically from a stable identifier.\n"
                "Examples:\n"
                "  # Scoped to a server (recommended):\n"
                "  from opensemantic import compute_scoped_uuid\n"
                "  uuid = compute_scoped_uuid(server_uuid, node_id)\n"
                "\n"
                "  # Or using the server URL as namespace:\n"
                "  from uuid import NAMESPACE_URL, uuid5\n"
                "  uuid = uuid5(uuid5(NAMESPACE_URL,\n"
                "    'opc.tcp://myserver:4840'), 'ns=2;s=Temp')\n"
                "\n"
                "  # Or from a fixed namespace\n"
                "  # (only if node_ids are globally unique):\n"
                "  uuid = uuid5(NAMESPACE_URL, node_id)\n"
            )
        if isinstance(data["uuid"], UUID):
            data["uuid"] = str(data["uuid"])
        if "osw_id" not in data:
            data["osw_id"] = f"OSW{data['uuid'].replace('-', '')}"
        super().__init__(**data)

    # get_osw_id() inherited from OswBaseModel via the model base class

    def __str__(self):
        return f"{self.name} ({self.opcua_data_type})"


class OpcUaServerMixin(DataToolMixin):
    """OPC UA-specific controller methods.

    Inherits generic DataTool features (channel mgmt, archiving, identity)
    from DataToolMixin. Adds OPC UA client/server lifecycle, read/write
    channel operations, subscriptions, and browsing.
    """

    # -- OPC UA-specific inner classes --

    class RunAsClientParams(BaseModel):
        channel_datachange_notification_callback: Optional[
            Callable[..., Awaitable[Any]]
        ] = None
        auto_archive: bool = False
        read_all_channels_on_startup: bool = False

    class RunAsServerParams(BaseModel):
        get_channel_value_callback: Optional[Callable[..., Awaitable[Any]]] = None

    class WriteChannelParams(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        channel: Any = None
        value: Any = None
        set_source_timestamp: bool = False
        set_server_timestamp: bool = False

    class WriteChannelResult(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        channel: Any = None
        value: Any = None
        status_code: Any = None

    class ReadChannelParams(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        channel: Any = None

    class ReadChannelResult(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        channel: Any = None
        value: Any = None
        timestamp: Optional[datetime.datetime] = None

    class GetChannelValueCallbackParams(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        channel: Any = None
        old_value: Any = None
        timestamp: Optional[float] = None
        old_timestamp: Optional[float] = None

    class BrowseParams(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        node: Optional[Any] = None
        depth: int = 1
        print: bool = False

    # -- OPC UA-specific hook overrides --

    def _on_archive_error(self):
        from opensemantic.lab._controller import ControllerState

        if self.reset_opcua_connection_on_error:
            self._state = ControllerState.reset

    async def configure_auto_archive(self, params):
        from opensemantic.lab._controller import ControllerMode

        if self._mode != ControllerMode.client:
            raise ValueError("Auto archiving is only supported in client mode")
        await super().configure_auto_archive(params)

    async def stop(self):
        from opensemantic.lab._controller import ControllerState

        await super().stop()
        self._state = ControllerState.stopping

    # -- OPC UA protocol methods --

    def _get_ua_data_value(
        self, channel, value, set_source_timestamp, set_server_timestamp
    ):
        from asyncua import ua

        source_timestamp = None
        server_timestamp = None
        variant_type = None
        if channel.opcua_data_type is not None:
            variant_type = ua.VariantType[channel.opcua_data_type.value]
        if set_source_timestamp:
            source_timestamp = datetime.datetime.now(datetime.timezone.utc)
        if set_server_timestamp:
            server_timestamp = datetime.datetime.now(datetime.timezone.utc)
        return ua.DataValue(
            Value=ua.Variant(value, variant_type),
            SourceTimestamp=source_timestamp,
            ServerTimestamp=server_timestamp,
        )

    async def write_channel(self, params):
        from asyncua import ua

        from opensemantic.lab._controller import ControllerMode

        value = self._get_ua_data_value(
            channel=params.channel,
            value=params.value,
            set_source_timestamp=params.set_source_timestamp,
            set_server_timestamp=params.set_server_timestamp,
        )
        if self._mode == ControllerMode.server and self._server is not None:
            await self._server.write_attribute_value(
                ua.NodeId.from_string(params.channel.node_id), value
            )
        elif self._mode == ControllerMode.client and self._client is not None:
            node = self._client.get_node(ua.NodeId.from_string(params.channel.node_id))
            await node.write_value(value)
        else:
            raise ValueError("No server or client available")
        return type(self).WriteChannelResult(
            channel=params.channel,
            value=params.value,
            status_code=ua.StatusCode(value=ua.status_codes.StatusCodes.Good),
        )

    async def write_channels(self, params):
        from asyncua import ua

        from opensemantic.lab._controller import ControllerMode

        soc = self._server if self._mode == ControllerMode.server else self._client
        if soc is None:
            raise ValueError("No server or client available")
        if not params:
            return []
        nodes = [soc.get_node(ua.NodeId.from_string(p.channel.node_id)) for p in params]
        values = [
            self._get_ua_data_value(
                p.channel, p.value, p.set_source_timestamp, p.set_server_timestamp
            )
            for p in params
        ]
        status_codes = await soc.write_values(nodes, values)
        return [
            type(self).WriteChannelResult(
                channel=p.channel, value=p.value, status_code=sc
            )
            for p, sc in zip(params, status_codes)
        ]

    async def read_channel(self, params):
        from asyncua import ua

        from opensemantic.lab._controller import ControllerMode

        if self._mode == ControllerMode.server and self._server is not None:
            node = self._server.get_node(ua.NodeId.from_string(params.channel.node_id))
        elif self._mode == ControllerMode.client and self._client is not None:
            node = self._client.get_node(ua.NodeId.from_string(params.channel.node_id))
        else:
            raise ValueError("No server or client available")
        dv = await node.read_data_value()
        return type(self).ReadChannelResult(
            channel=params.channel,
            value=dv.Value.Value,
            timestamp=dv.SourceTimestamp
            or dv.ServerTimestamp
            or datetime.datetime.now(datetime.timezone.utc),
        )

    async def read_channels(self, channels):
        from asyncua import ua

        from opensemantic.lab._controller import ControllerMode

        soc = self._server if self._mode == ControllerMode.server else self._client
        if soc is None:
            raise ValueError("No server or client available")
        if not channels:
            return []
        nodes = [soc.get_node(ua.NodeId.from_string(ch.node_id)) for ch in channels]
        results = await soc.read_attributes(nodes, ua.AttributeIds.Value)
        return [
            type(self).ReadChannelResult(
                channel=ch,
                value=dv.Value.Value,
                timestamp=dv.SourceTimestamp
                or dv.ServerTimestamp
                or datetime.datetime.now(datetime.timezone.utc),
            )
            for ch, dv in zip(channels, results)
        ]

    async def browse(self, params=None):
        from asyncua.tools import _lsprint_long

        from opensemantic.lab._controller import ControllerMode

        if params is None:
            params = type(self).BrowseParams()
        if params.node is None:
            if self._mode == ControllerMode.server and self._server is not None:
                params.node = self._server.nodes.objects
            elif self._mode == ControllerMode.client and self._client is not None:
                params.node = self._client.nodes.objects
            else:
                raise ValueError("No server or client available")
        if params.print:
            await _lsprint_long(params.node, params.depth)

    async def _handle_datachange_notification(self, node, val, data):
        node_id = node.nodeid.to_string()
        if node_id not in self._channel_dict:
            return
        channel = self._channel_dict[node_id]
        if channel.subchannels and len(channel.subchannels) > 0:
            srs = await self.read_channels(channel.subchannels)
            _val = val.isoformat() if isinstance(val, datetime.datetime) else val
            val = {channel.name: _val}
            for sr in srs:
                _v = (
                    sr.value.isoformat()
                    if isinstance(sr.value, datetime.datetime)
                    else sr.value
                )
                val[sr.channel.name] = _v

        ts = datetime.datetime.now(datetime.timezone.utc)
        if data.monitored_item.Value.ServerTimestamp is not None:
            ts = data.monitored_item.Value.ServerTimestamp
        if data.monitored_item.Value.SourceTimestamp is not None:
            ts = data.monitored_item.Value.SourceTimestamp

        if not hasattr(self, "_last_notified_values"):
            self._last_notified_values = {}
        if channel.uuid in self._last_notified_values:
            if self._last_notified_values[channel.uuid].value == val:
                _logger.warning("Duplicate notification for %s, ignoring", channel.name)
                return
        self._last_notified_values[channel.uuid] = type(
            self
        ).ChannelDataChangeNotificationParams(channel=channel, value=val, timestamp=ts)
        await self._handle_data_change(
            type(self).ChannelDataChangeNotificationParams(
                channel=channel, value=val, timestamp=ts
            )
        )

    async def run_as_client(self, params=None):
        from asyncua import Client, ua

        from opensemantic.lab._controller import (
            ControllerMode,
            ControllerState,
            OpcUaClientMode,
            SubscriptionHandler,
        )

        self._mode = ControllerMode.client
        self._state = ControllerState.idle
        if params is None:
            params = type(self).RunAsClientParams()
        if self.url is None:
            raise ValueError("No URL provided for the OPC UA server")

        self._subscription_handler = None
        if params.channel_datachange_notification_callback is not None:
            self._subscription_handler = SubscriptionHandler(
                datachange_notification_callback=self._handle_datachange_notification
            )
            self._channel_datachange_notification_callback = (
                params.channel_datachange_notification_callback
            )

        await self.configure_auto_archive(
            type(self).AutoArchiveParams(enable=params.auto_archive)
        )

        while self._state == ControllerState.idle:
            _logger.warning("Connecting")
            self._state = ControllerState.connecting
            self._client = Client(url=self.url)
            try:
                async with self._client:
                    _logger.warning("Connected")
                    self._state = ControllerState.connected

                    if params.read_all_channels_on_startup:
                        if self.reset_opcua_connection_on_error:
                            params.read_all_channels_on_startup = False
                        results = await self.read_channels(self.get_all_channels())
                        for r in results:
                            await self._handle_data_change(
                                type(self).ChannelDataChangeNotificationParams(
                                    channel=r.channel,
                                    value=r.value,
                                    timestamp=r.timestamp,
                                )
                            )

                    subscribe_groups = group_channels_by_interval(
                        self.get_all_channels(), mode_filter="Subscription"
                    )
                    read_groups = group_channels_by_interval(
                        self.get_all_channels(), mode_filter="Read"
                    )

                    for interval_ms, channels in subscribe_groups.items():
                        subscription = await self._client.create_subscription(
                            interval_ms, self._subscription_handler
                        )
                        for channel in channels:
                            variable = self._client.get_node(
                                ua.NodeId.from_string(channel.node_id)
                            )
                            sampling_ms = (
                                get_interval_ms(channel.sampling_interval) or 0
                            )
                            queue_size = calculate_queue_size(interval_ms, sampling_ms)
                            if self._subscription_handler is not None:
                                await subscription.subscribe_data_change(
                                    variable,
                                    queuesize=queue_size,
                                    sampling_interval=sampling_ms,
                                )

                    for interval_ms, channels in read_groups.items():
                        for channel in channels:
                            if channel.client_mode == OpcUaClientMode.RegisteredRead:
                                variable = self._client.get_node(
                                    ua.NodeId.from_string(channel.node_id)
                                )
                                await variable.register()
                                channel.node_id = variable.nodeid.to_string()

                    last_block_read_time = {ms: 0 for ms in read_groups.keys()}
                    last_connection_check = asyncio.get_event_loop().time()

                    while self._state == ControllerState.connected:
                        for interval_ms, channels in read_groups.items():
                            current_time = asyncio.get_event_loop().time()
                            if current_time >= last_block_read_time[interval_ms] + (
                                interval_ms / 1000
                            ):
                                last_block_read_time[interval_ms] = current_time
                                if channels:
                                    results = await self.read_channels(channels)
                                    for r in results:
                                        await self._handle_data_change(
                                            type(
                                                self
                                            ).ChannelDataChangeNotificationParams(
                                                channel=r.channel,
                                                value=r.value,
                                                timestamp=r.timestamp,
                                            )
                                        )
                        if asyncio.get_event_loop().time() - last_connection_check > 3:
                            last_connection_check = asyncio.get_event_loop().time()
                            await self._client.check_connection()
                        await asyncio.sleep(0.001)

                    if self._state == ControllerState.reset:
                        _logger.warning("Resetting connection")
                        self._state = ControllerState.idle
            except (ConnectionError, ua.UaError) as e:
                _logger.error("Connection error: %s", e)
                _logger.warning("Reconnecting in 2 seconds")
                self._state = ControllerState.idle
                await asyncio.sleep(2)
        self._state = ControllerState.stopped

    async def run_as_server(self, params=None):
        from asyncua import Server, ua

        from opensemantic.lab._controller import ControllerMode, ControllerState

        self._mode = ControllerMode.server
        self._state = ControllerState.running
        if params is None:
            params = type(self).RunAsServerParams()
        if self.url is None:
            raise ValueError("No URL provided for the OPC UA server")

        self._server = Server()
        await self._server.init()
        self._server.set_endpoint(self.url)
        self._server.set_server_name("OPC UA Server")
        idx = await self._server.register_namespace("http://example.org")
        objects = self._server.nodes.objects
        my_object = await objects.add_object(idx, "MyObject")

        variables = {}
        for channel in self.get_all_channels():
            if channel.opcua_data_type is None:
                continue
            variant_type = ua.VariantType[channel.opcua_data_type.value]
            initial_value = get_opcua_initial_value(channel.opcua_data_type.value)
            node_id = ua.NodeId.from_string(channel.node_id)
            variable = await my_object.add_variable(
                node_id, channel.name, initial_value, variant_type
            )
            await variable.set_writable()
            variables[channel.node_id] = variable

        last_write_time = {ch.node_id: 0 for ch in self.get_all_channels()}
        last_values = {ch.node_id: 0 for ch in self.get_all_channels()}

        async with self._server:
            _logger.warning("Server started at %s", self.url)
            self._state = ControllerState.running
            while self._state == ControllerState.running:
                current_time = asyncio.get_event_loop().time()
                for channel in self.get_all_channels():
                    interval_ms = get_interval_ms(channel.refresh_interval)
                    if interval_ms is None:
                        continue
                    if current_time >= last_write_time[channel.node_id] + (
                        interval_ms / 1000
                    ):
                        if params.get_channel_value_callback is None:
                            continue
                        value = await params.get_channel_value_callback(
                            type(self).GetChannelValueCallbackParams(
                                channel=channel,
                                old_value=last_values[channel.node_id],
                                timestamp=current_time,
                                old_timestamp=last_write_time[channel.node_id],
                            )
                        )
                        if value is not None:
                            await self.write_channel(
                                type(self).WriteChannelParams(
                                    channel=channel,
                                    value=value,
                                    set_source_timestamp=True,
                                    set_server_timestamp=True,
                                )
                            )
                            last_write_time[channel.node_id] = current_time
                            last_values[channel.node_id] = value
                await asyncio.sleep(0.1)
        self._state = ControllerState.stopped
