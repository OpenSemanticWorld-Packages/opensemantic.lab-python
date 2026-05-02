"""Example: OPC UA Server with data archiving to local SQLite or remote PostgREST.

Demonstrates the full workflow:
1. Init an OSW client (optional, for loading entities from wiki)
2. Declare an OpcUaServer entity with storage_locations
3. Auto-init a DB controller from storage_locations
4. Run a test OPC UA server + client with auto-archiving
5. Read back archived data (raw and typed)

Uses v1 models throughout.
"""

import asyncio
import logging
import os
import random
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from opensemantic import compute_scoped_uuid
from opensemantic.base._controller_mixin import DataToolMixin, TSDCMixin
from opensemantic.base.v1 import (
    Database,
    DatabaseServer,
    LocalTimeSeriesDatabaseController,
)
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


# ---------------------------------------------------------------------------
# 1. (Optional) Load from an OO-LD backend like OpenSemanticLab (OSL)
# ---------------------------------------------------------------------------
# Option A: Load from a local oold file store
#
# from oold.backend.document_store import SimpleDictDocumentStore
# from oold.backend.interface import (
#     SetResolverParam, SetBackendParam, set_resolver, set_backend,
# )
# store = SimpleDictDocumentStore(file_path="./server_store.json")
# set_resolver(SetResolverParam(iri="Item", resolver=store))
# set_backend(SetBackendParam(iri="Item", backend=store))
# loaded = OpcUaServer["Item:OSW<your-server-osw-id>"]
#
# Option B: Load from an OSL instance via osw-python
#
# from osw.express import OswExpress
# osw = OswExpress(
#     domain="wiki-dev.open-semantic-lab.org",
#     cred_filepath="accounts.pwd.yaml",
# )
# loaded = OpcUaServer["Item:OSW<your-server-osw-id>"]


# ---------------------------------------------------------------------------
# 2. Declare an OpcUaServer entity locally
# ---------------------------------------------------------------------------
SERVER_UUID = uuid5(NAMESPACE_URL, "Example OPC UA Server")
OPC_URL = "opc.tcp://localhost:48500"

# Define the database where data will be archived
archive_db_entity = Database(
    name="opc_archive",
    label=[Label(text="OPC UA Archive Database")],
    server=DatabaseServer(
        name="local_db_server",
        label=[Label(text="Local DB Server")],
        url="http://localhost:3000",
        domain="localhost",
        network_port=[3000],
    ),
)

# Define data channels
# Channel UUIDs must be deterministic. Use compute_scoped_uuid(parent_uuid, node_id)
# to scope them to the server, avoiding collisions if the same node_id exists on
# multiple servers. osw_id is auto-prefixed: OSW<server>#OSW<channel> by DataToolMixin.
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
        # Characteristic IRI - defines the semantic type of data values
        characteristic=Temperature.get_cls_iri(),
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

# Declare the OpcUaServer entity with a storage location
server_entity = OpcUaServer(
    uuid=SERVER_UUID,
    name="example_opcua_server",
    label=[Label(text="Example OPC UA Server")],
    data_channels=channels,
    storage_locations=[archive_db_entity],
)

print("Declared OpcUaServer entity:")
print(f"  UUID: {server_entity.uuid}")
print(f"  Channels: {len(server_entity.data_channels)}")
print(f"  Storage locations: {len(server_entity.storage_locations)}")
print(f"  First storage: {server_entity.storage_locations[0].name}")


# ---------------------------------------------------------------------------
# 3. Auto-init DB controller from storage_locations
# ---------------------------------------------------------------------------
# When auto_archive=True and no explicit archive_database is set,
# DataToolMixin auto-creates a LocalTimeSeriesDatabaseController
# from the first storage_location.

DB_PATH = Path(__file__).parent / "archive_data.sqlite"

# The test client will auto-init archive_database from storage_locations
test_server = OpcUaServer(
    uuid=SERVER_UUID,
    name="example_opcua_server",
    label=[Label(text="Example OPC UA Server")],
    url=OPC_URL,
    data_channels=channels,
)

# Explicit archive_database to control db_path
archive_controller = LocalTimeSeriesDatabaseController(
    name=archive_db_entity.name,
    label=archive_db_entity.label,
    db_path=DB_PATH,
)

test_client = OpcUaServer(
    uuid=SERVER_UUID,
    name="example_opcua_client",
    label=[Label(text="Example OPC UA Client")],
    url=OPC_URL,
    data_channels=channels,
    archive_database=archive_controller,
    auto_archive=True,
)

print(f"\nArchive DB: {archive_controller.db_path}")

received_count = 0


async def generate_value(params):
    """Callback for the test server to generate random values."""
    if "temperature" in params.channel.name:
        return random.uniform(20.0, 25.0)
    elif "pressure" in params.channel.name:
        return random.uniform(1000.0, 1020.0)
    return 0.0


async def on_data_change(params):
    """Callback when the client receives new data."""
    global received_count
    received_count += 1


async def timeout():
    """Stop after 5 seconds."""
    await asyncio.sleep(5)
    await test_client.stop()
    await test_server.stop()


async def main():
    print("\nStarting OPC UA server + client with auto-archiving...")
    print(f"  Server URL: {OPC_URL}")
    print(f"  Archive DB: {DB_PATH}")

    await asyncio.gather(
        test_server.run_as_server(
            OpcUaServer.RunAsServerParams(
                get_channel_value_callback=generate_value,
            )
        ),
        test_client.run_as_client(
            OpcUaServer.RunAsClientParams(
                channel_datachange_notification_callback=on_data_change,
                auto_archive=True,
            )
        ),
        timeout(),
    )

    print(f"\nReceived {received_count} data change notifications")

    # -------------------------------------------------------------------
    # 4. Read back archived data (raw)
    # -------------------------------------------------------------------
    print("\n--- Raw read ---")

    tool_osw_id = test_client.get_osw_id()
    rows = await archive_controller.read_tool_channel_raw(
        TSDCMixin.ReadToolChannelRawParams(
            tool_osw_id=tool_osw_id,
            limit=5,
        )
    )
    print(f"Found {len(rows)} raw rows (showing up to 5):")
    for row in rows[:5]:
        print(f"  ts={row['ts']}  ch={row['ch'][:20]}...  data={row['data']}")

    # -------------------------------------------------------------------
    # 5. Typed write + read (Temperature channel)
    # -------------------------------------------------------------------
    print("\n--- Typed write + read ---")

    import datetime

    now = datetime.datetime.now(datetime.timezone.utc)
    temp_channel = test_client.data_channels[0]

    # Write typed data (compact - defaults stripped)
    await test_client.store_typed_data(
        DataToolMixin.StoreTypedDataParams(
            tool_osw_id=tool_osw_id,
            rows=[
                DataToolMixin.TypedDataRow(
                    ts=now,
                    channel=temp_channel,
                    value=Temperature(value=295.65, unit=TemperatureUnit.kelvin),
                ),
            ],
        )
    )
    print("Wrote typed Temperature(295.65 K) - compact (defaults stripped)")

    # Read back typed
    results = await test_client.read_typed_data(
        DataToolMixin.ReadTypedDataParams(
            tool_osw_id=tool_osw_id,
            channel=temp_channel,
            target_schema=Temperature,
            limit=1,
        )
    )
    if results:
        t = results[-1]
        print(f"Read back: Temperature(value={t.value}, unit={t.unit})")

    # Cleanup
    if DB_PATH.exists():
        os.unlink(DB_PATH)
        print(f"\nCleaned up {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
