"""Example: Live DataTool dashboard with embedded OPC UA server.

Starts an OPC UA server that generates random temperature and pressure
values, then connects a copy as client to subscribe, archive, and display
data in a Panelini dashboard with archive and live tabs.

Run with: panel serve examples/live_dashboard.py --port 5010
"""

import asyncio
import copy
import math
import random
from uuid import NAMESPACE_URL, uuid5

import panel as pn

from opensemantic import compute_scoped_uuid
from opensemantic.base.ui._config import LiveConfig, LiveDashboardConfig, PlotConfig
from opensemantic.base.v1 import Database
from opensemantic.characteristics.quantitative.v1 import (
    ForcePerAreaUnit,
    Pressure,
    Temperature,
    TemperatureUnit,
    Time,
    TimeUnit,
)
from opensemantic.core.v1 import Label
from opensemantic.lab.ui import LiveDataToolView
from opensemantic.lab.v1 import (
    OpcUaClientMode,
    OpcUaDataChannel,
    OpcUaDataType,
    OpcUaServer,
)

pn.extension()

SERVER_UUID = uuid5(NAMESPACE_URL, "live-dashboard-example")
OPC_URL = "opc.tcp://localhost:48411"

# -- Define OPC UA server --

sensor = OpcUaServer(
    uuid=SERVER_UUID,
    name="DemoSensor",
    label=[Label(text="Demo Sensor", lang="en"), Label(text="Demo-Sensor", lang="de")],
    url=OPC_URL,
    data_channels=[
        OpcUaDataChannel(
            uuid=compute_scoped_uuid(SERVER_UUID, "ns=2;s=Temperature"),
            node_id="ns=2;s=Temperature",
            name="temperature",
            label=[
                Label(text="Temperature", lang="en"),
                Label(text="Temperatur", lang="de"),
            ],
            opcua_data_type=OpcUaDataType.Float,
            client_mode=OpcUaClientMode.Subscription,
            sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
            refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
            characteristic=Temperature.get_cls_iri(),
            unit=TemperatureUnit.Celsius.value,
        ),
        OpcUaDataChannel(
            uuid=compute_scoped_uuid(SERVER_UUID, "ns=2;s=Pressure"),
            node_id="ns=2;s=Pressure",
            name="pressure",
            label=[Label(text="Pressure", lang="en"), Label(text="Druck", lang="de")],
            opcua_data_type=OpcUaDataType.Float,
            client_mode=OpcUaClientMode.Subscription,
            sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
            refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
            characteristic=Pressure.get_cls_iri(),
            unit=ForcePerAreaUnit.hecto_pascal.value,
        ),
    ],
    storage_locations=[
        Database(name="live_demo_archive", label=[Label(text="Archive")])
    ],
    auto_archive=True,
)

# Client is a copy of the server (same channels, own archive DB)
client = copy.deepcopy(sensor)


# -- Simulated sensor data generator --

_t0 = None


async def generate_value(params):
    global _t0
    if _t0 is None:
        _t0 = params.timestamp
    elapsed = params.timestamp - _t0
    ch = params.channel
    if ch.name == "temperature":
        return Temperature(
            value=22.0 + 3.0 * math.sin(elapsed * 0.2) + random.uniform(-0.3, 0.3),
            unit=TemperatureUnit.Celsius,
        )
    elif ch.name == "pressure":
        return Pressure(
            value=1013.0 + 5.0 * math.sin(elapsed * 0.05) + random.uniform(-0.5, 0.5),
            unit=ForcePerAreaUnit.hecto_pascal,
        )
    return 0.0


# -- Build dashboard using the client --

config = LiveDashboardConfig(
    lang="en",
    plot=PlotConfig(auto_fetch=True, row_limit=10000),
    live=LiveConfig(buffer_size=500, update_interval_ms=500, history_seconds=30),
)

view = LiveDataToolView(
    controllers=[client],
    config=config,
    title="Live DataTool Dashboard",
)

view.servable()


# -- Start OPC UA server + client on Panel's event loop --


async def start_opcua():
    import sys

    asyncio.ensure_future(
        sensor.run_as_server(
            OpcUaServer.RunAsServerParams(
                get_channel_value_callback=generate_value,
            )
        )
    )
    print("[setup] OPC UA server starting...", file=sys.stderr, flush=True)
    await asyncio.sleep(2)

    async def _noop_cb(params):
        pass

    asyncio.ensure_future(
        client.run_as_client(
            OpcUaServer.RunAsClientParams(
                channel_datachange_notification_callback=_noop_cb,
                auto_archive=True,
            )
        )
    )
    print("[setup] OPC UA client connecting...", file=sys.stderr, flush=True)


pn.state.execute(start_opcua)
