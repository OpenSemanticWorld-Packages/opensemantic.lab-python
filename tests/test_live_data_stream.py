"""Tests for OPC UA live data streaming and LiveDataToolView callback wiring."""

import asyncio
import datetime as dt
import math
import random
from uuid import NAMESPACE_URL, uuid5

import pytest

from opensemantic import compute_scoped_uuid
from opensemantic.base.ui._data_cache import ChannelDataCache
from opensemantic.base.v1 import Database
from opensemantic.characteristics.quantitative.v1 import (
    Pressure,
    Temperature,
    Time,
    TimeUnit,
)
from opensemantic.core.v1 import Label

try:
    from opensemantic.lab.v1 import (
        OpcUaClientMode,
        OpcUaDataChannel,
        OpcUaDataType,
        OpcUaServer,
    )

    HAS_ASYNCUA = True
except ImportError:
    HAS_ASYNCUA = False

pytestmark = pytest.mark.skipif(not HAS_ASYNCUA, reason="asyncua not installed")


SERVER_UUID = uuid5(NAMESPACE_URL, "test-live-stream")
OPC_URL = "opc.tcp://localhost:48499"


def _make_channel(name, node_id, characteristic_cls):
    return OpcUaDataChannel(
        uuid=compute_scoped_uuid(SERVER_UUID, node_id),
        node_id=node_id,
        name=name,
        label=[Label(text=name.capitalize())],
        opcua_data_type=OpcUaDataType.Float,
        client_mode=OpcUaClientMode.Subscription,
        sampling_interval=Time(value=50, unit=TimeUnit.milli_second),
        refresh_interval=Time(value=200, unit=TimeUnit.milli_second),
        characteristic=characteristic_cls.get_cls_iri(),
    )


@pytest.fixture
def server_and_client():
    """Create an OPC UA server and client pair."""
    server = OpcUaServer(
        uuid=SERVER_UUID,
        name="TestSensor",
        label=[Label(text="Test Sensor")],
        url=OPC_URL,
        data_channels=[
            _make_channel("temperature", "ns=2;s=Temp", Temperature),
            _make_channel("pressure", "ns=2;s=Press", Pressure),
        ],
        storage_locations=[Database(name="test_stream_srv", label=[Label(text="S")])],
    )

    client = OpcUaServer(
        uuid=SERVER_UUID,
        name="TestSensor",
        label=[Label(text="Test Sensor")],
        url=OPC_URL,
        data_channels=[
            _make_channel("temperature", "ns=2;s=Temp", Temperature),
            _make_channel("pressure", "ns=2;s=Press", Pressure),
        ],
        storage_locations=[Database(name="test_stream_cli", label=[Label(text="C")])],
        auto_archive=True,
    )
    return server, client


class TestOpcUaDataStream:
    """Test OPC UA server/client data streaming."""

    def test_server_client_data_flow(self, server_and_client):
        """Server generates data, client receives via subscription."""
        server, client = server_and_client
        received = []

        async def on_data(params):
            received.append(
                {
                    "channel": params.channel.name,
                    "value": params.value,
                    "timestamp": params.timestamp,
                }
            )

        _t0 = None

        async def generate(params):
            nonlocal _t0
            if _t0 is None:
                _t0 = params.timestamp
            elapsed = params.timestamp - _t0
            if params.channel.name == "temperature":
                return 22.0 + math.sin(elapsed) + random.uniform(-0.1, 0.1)
            return 1013.0 + random.uniform(-1, 1)

        async def run():
            # Start server
            server_task = asyncio.ensure_future(
                server.run_as_server(
                    OpcUaServer.RunAsServerParams(
                        get_channel_value_callback=generate,
                    )
                )
            )
            await asyncio.sleep(1)

            # Start client with callback
            client_task = asyncio.ensure_future(
                client.run_as_client(
                    OpcUaServer.RunAsClientParams(
                        channel_datachange_notification_callback=on_data,
                        auto_archive=True,
                    )
                )
            )
            # Wait for data to accumulate
            await asyncio.sleep(5)

            # Stop
            server._state = None  # breaks the server loop
            client._state = None
            server_task.cancel()
            client_task.cancel()
            try:
                await server_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await client_task
            except (asyncio.CancelledError, Exception):
                pass

        asyncio.run(run())

        assert len(received) > 0, "Should have received data changes"
        channels_seen = {r["channel"] for r in received}
        assert "temperature" in channels_seen
        assert "pressure" in channels_seen

        # Check archive has data
        async def check_archive():
            from opensemantic.base._controller_mixin import TSDCMixin

            raw = await client.archive_database.read_tool_channel_raw(
                TSDCMixin.ReadToolChannelRawParams(
                    tool_osw_id=client.get_osw_id(),
                )
            )
            return raw

        archive_rows = asyncio.run(check_archive())
        assert len(archive_rows) > 0, "Archive should have data"

    def test_callback_replacement(self, server_and_client):
        """Replacing _channel_datachange_notification_callback works mid-stream."""
        server, client = server_and_client
        received_phase1 = []
        received_phase2 = []

        async def cb1(params):
            received_phase1.append(params.channel.name)

        async def cb2(params):
            received_phase2.append(params.channel.name)

        async def generate(params):
            return random.uniform(10, 30)

        async def run():
            server_task = asyncio.ensure_future(
                server.run_as_server(
                    OpcUaServer.RunAsServerParams(
                        get_channel_value_callback=generate,
                    )
                )
            )
            await asyncio.sleep(1)

            client_task = asyncio.ensure_future(
                client.run_as_client(
                    OpcUaServer.RunAsClientParams(
                        channel_datachange_notification_callback=cb1,
                        auto_archive=False,
                    )
                )
            )
            await asyncio.sleep(3)

            # Replace callback mid-stream
            client._channel_datachange_notification_callback = cb2
            await asyncio.sleep(3)

            server._state = None
            client._state = None
            server_task.cancel()
            client_task.cancel()
            try:
                await server_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await client_task
            except (asyncio.CancelledError, Exception):
                pass

        asyncio.run(run())

        assert len(received_phase1) > 0, "Phase 1 should have received data"
        assert (
            len(received_phase2) > 0
        ), "Phase 2 should have received data after callback swap"

    def test_cache_with_archived_stream_data(self, server_and_client):
        """Data streamed via OPC UA and archived can be read back via cache."""
        server, client = server_and_client

        async def generate(params):
            return random.uniform(20, 25)

        async def run():
            server_task = asyncio.ensure_future(
                server.run_as_server(
                    OpcUaServer.RunAsServerParams(
                        get_channel_value_callback=generate,
                    )
                )
            )
            await asyncio.sleep(1)

            async def _noop(params):
                pass

            client_task = asyncio.ensure_future(
                client.run_as_client(
                    OpcUaServer.RunAsClientParams(
                        channel_datachange_notification_callback=_noop,
                        auto_archive=True,
                    )
                )
            )
            await asyncio.sleep(5)

            # Read back via cache
            ch = client.get_channel_by_name("temperature")
            cache = ChannelDataCache(enabled=True)
            start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)
            end = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1)
            rows = await cache.get_data(client, ch, start, end, 1000)

            server._state = None
            client._state = None
            server_task.cancel()
            client_task.cancel()
            try:
                await server_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await client_task
            except (asyncio.CancelledError, Exception):
                pass

            return rows

        rows = asyncio.run(run())
        assert len(rows) > 0, "Cache should return archived stream data"
        pt = rows[0]
        assert isinstance(pt.timestamp, (dt.datetime, str))
        assert pt.value is not None
