"""Example: OPC UA Server with data archiving to local SQLite or remote PostgREST.

Demonstrates the full workflow:
1. Init an OSW client (optional, for loading entities from wiki)
2. Declare an OpcUaServer entity with storage_locations
3. Init a TimeSeriesDatabaseController from the storage_location
4. Run a test OPC UA server + client with auto-archiving
5. Read back archived data

Uses v1 models throughout.
"""

import asyncio
import logging
import os
import random
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from opensemantic import compute_scoped_uuid
from opensemantic.base.v1 import (
    Database,
    DatabaseServer,
    LocalTimeSeriesDatabaseController,
    TimeSeriesDatabaseController,
)
from opensemantic.characteristics.quantitative.v1 import Time, TimeUnit
from opensemantic.core.v1 import Label
from opensemantic.lab.v1 import (
    OpcUaClientMode,
    OpcUaDataChannel,
    OPCUADataType,
    OpcUaServer,
)

logging.basicConfig(level=logging.WARNING)

# ---------------------------------------------------------------------------
# 1. (Optional) Init OSW client to load entities from wiki
# ---------------------------------------------------------------------------
# Uncomment to load an OpcUaServer entity from the wiki instead of declaring
# it locally. Requires accounts.pwd.yaml with valid credentials.
#
# from osw.express import OswExpress
#
# osw = OswExpress(
#     domain="wiki-dev.open-semantic-lab.org",
#     cred_filepath=os.path.join(os.path.dirname(__file__), "accounts.pwd.yaml"),
# )
# osw.install_dependencies({
#     "OpcUaServer": "Category:OSW89fda9fed80b41b1ad4c0c011e645600",
# })
#
# # Load an existing OpcUaServer entity by its page title
# import osw.model.entity as model
# page = osw.site.get_page("Item:OSW<your-server-uuid>")
# server_data = page.get_slot_content("jsondata")
# server_entity = model.OpcUaServer(**server_data)


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
# 3. Init a DB controller from the storage_location
# ---------------------------------------------------------------------------
# The storage_locations field contains Database entities.
# We use the first one to construct a controller.

db_entity = server_entity.storage_locations[0]
DB_PATH = Path(__file__).parent / "archive_data.sqlite"

# Variant A: Local SQLite
archive_controller = LocalTimeSeriesDatabaseController(
    # Inherit identity from the Database entity
    name=db_entity.name,
    label=db_entity.label,
    # Controller-specific: local file path
    db_path=DB_PATH,
)

print("\nInitialized LocalTimeSeriesDatabaseController:")
print(f"  Name: {archive_controller.name}")
print(f"  DB path: {archive_controller.db_path}")
print(f"  Is Database: {isinstance(archive_controller, Database)}")

# Variant B: Remote PostgREST (setup only, requires postgrest package + running server)
try:
    from oold.backend.auth import set_credential
    from postgrest import AsyncPostgrestClient
    from pydantic import SecretStr

    from opensemantic.base.v1 import PostgrestTimeSeriesDatabaseController

    # Derive connection info from the Database entity's server
    server_info = db_entity.server
    pgrst_url = (
        server_info.url or f"http://{server_info.domain}:{server_info.network_port[0]}"
    )

    # Register credentials for this endpoint
    set_credential(pgrst_url, token=SecretStr("your-postgrest-jwt-token"))

    # Create PostgREST client
    pgrst_client = AsyncPostgrestClient(
        base_url=pgrst_url,
        schema="api",
        headers={"Authorization": "Bearer your-postgrest-jwt-token"},
        timeout=5.0,
    )

    remote_controller = PostgrestTimeSeriesDatabaseController(
        name=db_entity.name,
        label=db_entity.label,
    )
    remote_controller.set_client(pgrst_client)

    print("\nInitialized PostgrestTimeSeriesDatabaseController:")
    print(f"  Name: {remote_controller.name}")
    print(f"  PostgREST URL: {pgrst_url}")
    print("  (Not connecting - demo only)")

except ImportError:
    print("\nPostgREST variant: skipped (postgrest package not installed)")


# ---------------------------------------------------------------------------
# 4. Run OPC UA server + client with auto-archiving (using local SQLite)
# ---------------------------------------------------------------------------

# Create server and client controller instances
# (separate from the entity - these add the OPC UA connection management)
test_server = OpcUaServer(
    uuid=SERVER_UUID,
    name="example_opcua_server",
    label=[Label(text="Example OPC UA Server")],
    url=OPC_URL,
    data_channels=channels,
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

    # ---------------------------------------------------------------------------
    # 5. Read back archived data
    # ---------------------------------------------------------------------------
    print("\nReading archived data...")

    tool_osw_id = test_client.get_osw_id()
    rows = await archive_controller.read_tool_channel_raw(
        TimeSeriesDatabaseController.ReadToolChannelRawParams(
            tool_osw_id=tool_osw_id,
            limit=10,
        )
    )

    print(f"Found {len(rows)} archived rows (showing up to 10):")
    for row in rows[:10]:
        print(f"  ts={row['ts']}  ch={row['ch'][:20]}...  data={row['data']}")

    # Cleanup
    if DB_PATH.exists():
        os.unlink(DB_PATH)
        print(f"\nCleaned up {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
