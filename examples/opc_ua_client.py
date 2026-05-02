"""Minimal OPC UA client example.

Loads an OpcUaServer entity from the oold file-backed store (written by
opc_ua_server.py), connects as a client, collects data for a few
seconds, then reads back and prints archived data (raw and typed).

Usage:
    1. In one terminal: python opc_ua_server.py
    2. In another terminal: python opc_ua_client.py
"""

import asyncio
import logging
import sys
from uuid import NAMESPACE_URL, uuid5

from oold.backend.document_store import SimpleDictDocumentStore
from oold.backend.interface import (
    SetBackendParam,
    SetResolverParam,
    set_backend,
    set_resolver,
)

from opensemantic.base._controller_mixin import DataToolMixin, TSDCMixin
from opensemantic.characteristics.quantitative.v1 import Temperature
from opensemantic.lab.v1 import OpcUaServer

logging.basicConfig(level=logging.WARNING)

STORE_FILE = "./server_store.json"
# The OPC UA endpoint URL is a runtime config, not part of the data model.
# Both server and client must know it.
OPC_URL = "opc.tcp://localhost:48410"

# -- Set up the oold file-backed store --
store = SimpleDictDocumentStore(file_path=STORE_FILE)
set_resolver(SetResolverParam(iri="Item", resolver=store))
set_backend(SetBackendParam(iri="Item", backend=store))

# -- Load the server entity using OpcUaServer[IRI] --
# The entity provides data_channels, storage_locations, etc.
# storage_locations IRIs are auto-resolved via the registered backend.
# TODO: url should be part of the wiki model so it survives serialization.
# For now, it must be set as runtime config.
SERVER_UUID = uuid5(NAMESPACE_URL, "minimal-example-server")
server_iri = f"Item:OSW{str(SERVER_UUID).replace('-', '')}"

try:
    loaded = OpcUaServer[server_iri]
except (ValueError, KeyError):
    print(f"Server entity not found: {server_iri}")
    print("Run opc_ua_server.py first.")
    sys.exit(1)

# Create client controller: entity data from store + runtime config
# auto_archive=True + storage_locations -> auto-inits archive_database
client = OpcUaServer(
    uuid=loaded.uuid,
    name=loaded.name,
    label=loaded.label,
    url=OPC_URL,
    data_channels=loaded.data_channels,
    storage_locations=loaded.storage_locations,
    auto_archive=True,
)

print(f"Loaded server from store: {server_iri}")
print(f"  Name: {client.name}")
print(f"  URL: {client.url}")
print(f"  Channels: {len(client.data_channels or [])}")
print(f"  Archive DB: {type(client.archive_database).__name__}")

received = []


async def on_data_change(params):
    received.append(params)
    print(f"  {params.channel.name}: {params.value} " f"at {params.timestamp}")


async def run():
    print("\nConnecting to OPC UA server for 5 seconds...")

    async def stop_after():
        await asyncio.sleep(5)
        await client.stop()

    await asyncio.gather(
        client.run_as_client(
            OpcUaServer.RunAsClientParams(
                channel_datachange_notification_callback=on_data_change,
                auto_archive=True,
            )
        ),
        stop_after(),
    )

    print(f"\nReceived {len(received)} data changes")

    if client.archive_database is not None:
        # -- Read back raw archived data --
        print("\n--- Raw archived data (last 5) ---")
        tool_osw_id = client.get_osw_id()
        rows = await client.archive_database.read_tool_channel_raw(
            TSDCMixin.ReadToolChannelRawParams(
                tool_osw_id=tool_osw_id,
                limit=5,
            )
        )
        for row in rows:
            print(f"  ts={row['ts']}  data={row['data']}")

        # -- Read back typed (Temperature) --
        print("\n--- Typed archived data (last 5) ---")
        temp_channel = client.data_channels[0]
        typed_results = await client.read_typed_data(
            DataToolMixin.ReadTypedDataParams(
                tool_osw_id=tool_osw_id,
                channel=temp_channel,
                target_schema=Temperature,
                limit=5,
            )
        )
        for t in typed_results:
            print(f"  Temperature: {t.value:.2f} {t.unit.name}")
    else:
        print("\nNo archive database available (storage_locations empty?)")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nClient stopped.")
