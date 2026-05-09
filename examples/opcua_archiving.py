"""Example: OPC UA Server with data archiving.

Demonstrates:
1. Declare an OpcUaServer with channels and a storage location
2. Run server + client with auto-archiving to SQLite
3. Read back archived data using store/load channel API
"""

import asyncio
import logging
import os
import random
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from opensemantic import compute_scoped_uuid
from opensemantic.base.v1 import Database
from opensemantic.characteristics.quantitative.v1 import (
    Temperature,
    TemperatureUnit,
    Time,
    TimeUnit,
)
from opensemantic.core.v1 import Label
from opensemantic.lab.v1 import (
    OpcUaClientMode,
    OpcUaDataChannel,
    OPCUADataType,
    OpcUaServer,
)

logging.basicConfig(level=logging.WARNING)

SERVER_UUID = uuid5(NAMESPACE_URL, "Example OPC UA Server")
OPC_URL = "opc.tcp://localhost:48500"
DB_PATH = Path(__file__).parent / "archive_data.sqlite"

# -- Alternative: load from OpenSemanticLab instead of declaring locally --
# from osw.express import OswExpress
# osw = OswExpress(domain="your-domain.org", cred_filepath="accounts.pwd.yaml")
# server = OpcUaServer["Item:OSW<your-server-osw-id>"]
# client = OpcUaServer(server, url="opc.tcp://...", auto_archive=True)

# -- 1. Define channels and server entity --
channels = [
    OpcUaDataChannel(
        uuid=compute_scoped_uuid(SERVER_UUID, "ns=2;s=Example.Temperature"),
        node_id="ns=2;s=Example.Temperature",
        name="temperature",
        label=[Label(text="Temperature")],
        opcua_data_type=OPCUADataType.Float,
        client_mode=OpcUaClientMode.Subscription,
        sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
        refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
        characteristic=Temperature.get_cls_iri(),
        unit=TemperatureUnit.Celsius,
    ),
    OpcUaDataChannel(
        uuid=compute_scoped_uuid(SERVER_UUID, "ns=2;s=Example.Pressure"),
        node_id="ns=2;s=Example.Pressure",
        name="pressure",
        label=[Label(text="Pressure")],
        opcua_data_type=OPCUADataType.Float,
        client_mode=OpcUaClientMode.Subscription,
        sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
        refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
    ),
]

# -- 2. Create server (no archiving) and client (with archiving) --
server = OpcUaServer(
    uuid=SERVER_UUID,
    name="example_server",
    label=[Label(text="Server")],
    url=OPC_URL,
    data_channels=channels,
)

client = OpcUaServer(
    uuid=SERVER_UUID,
    name="example_client",
    label=[Label(text="Client")],
    url=OPC_URL,
    data_channels=channels,
    storage_locations=[Database(name="archive", label=[Label(text="Archive")])],
    auto_archive=True,
)

print(f"Server: {server.name}, {len(channels)} channels")
print(f"Client archive: {type(client.archive_database).__name__}")

received = []


async def generate_value(
    params: OpcUaServer.GetChannelValueCallbackParams,
):
    """Generate simulated sensor values.

    Can return:
    - A raw scalar (float) for simple channels
    - A Characteristic instance for typed channels (auto-converted
      to raw value for OPC UA, stored with unit context in archive)
    """
    if "temperature" in params.channel.name:
        # Return a Characteristic instance - the server extracts .value
        # for OPC UA, and _handle_data_change stores it with unit context
        return Temperature(
            value=random.uniform(20.0, 25.0),
            unit=TemperatureUnit.Celsius,
        )
    return random.uniform(1000.0, 1020.0)


async def on_data_change(params):
    received.append(params)


async def stop_after(seconds):
    await asyncio.sleep(seconds)
    await client.stop()
    await server.stop()


async def main():
    print(f"\nRunning for 5 seconds on {OPC_URL}...")

    await asyncio.gather(
        server.run_as_server(
            OpcUaServer.RunAsServerParams(
                get_channel_value_callback=generate_value,
            )
        ),
        client.run_as_client(
            OpcUaServer.RunAsClientParams(
                channel_datachange_notification_callback=on_data_change,
                auto_archive=True,
            )
        ),
        stop_after(5),
    )

    print(f"Received {len(received)} notifications")

    # -- 3. Read back using load_channel_data --
    print("\n--- Load temperature (auto-typed from characteristic) ---")
    results = await client.load_channel_data(
        client.LoadChannelDataParams(channel="temperature", limit=3)
    )
    for t in results[:3]:
        print(f"  {type(t).__name__}: {t.value} {t.unit}")

    print("\n--- Load pressure (raw, no characteristic) ---")
    results = await client.load_channel_data(
        client.LoadChannelDataParams(channel="pressure", limit=3)
    )
    for d in results[:3]:
        print(f"  {d}")

    # -- 4. Store a typed value manually --
    print("\n--- Store + load typed Temperature ---")
    await client.store_channel_data(
        client.StoreChannelDataParams(
            channel="temperature",
            value=Temperature(value=295.65, unit=TemperatureUnit.kelvin),
        )
    )
    results = await client.load_channel_data(
        client.LoadChannelDataParams(
            channel="temperature",
            target_schema=Temperature,
            limit=1,
        )
    )
    if results:
        print(f"  {results[-1].value} {results[-1].unit}")

    # Cleanup
    db_path = getattr(client.archive_database, "db_path", None)
    if db_path and Path(db_path).exists():
        os.unlink(db_path)
        print(f"\nCleaned up {db_path}")


if __name__ == "__main__":
    asyncio.run(main())
