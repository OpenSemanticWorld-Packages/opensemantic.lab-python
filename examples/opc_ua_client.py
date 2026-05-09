"""Minimal OPC UA client example.

Loads the OpcUaServer entity from the oold store (written by
opc_ua_server.py), connects, collects data, reads back archived values.

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

from opensemantic.lab.v1 import OpcUaServer

logging.basicConfig(level=logging.WARNING)

STORE_FILE = "./server_store.json"
OPC_URL = "opc.tcp://localhost:48410"

# -- Load entity from store --
store = SimpleDictDocumentStore(file_path=STORE_FILE)
set_resolver(SetResolverParam(iri="Item", resolver=store))
set_backend(SetBackendParam(iri="Item", backend=store))

SERVER_UUID = uuid5(NAMESPACE_URL, "minimal-example-server")
server_iri = f"Item:OSW{str(SERVER_UUID).replace('-', '')}"

try:
    # OpcUaServer[iri] returns a controller instance directly
    # (auto-resolved from _controller_types registry)
    client = OpcUaServer[server_iri]
except (ValueError, KeyError):
    print(f"Not found: {server_iri}. Run opc_ua_server.py first.")
    sys.exit(1)

# Set runtime config (not part of the data model yet)
client.url = OPC_URL
client.auto_archive = True

print(f"Loaded: {client.name}")
print(f"  Channels: {[ch.name for ch in client.data_channels or []]}")
print(f"  Archive: {type(client.archive_database).__name__}")

received = []


async def on_data_change(params):
    received.append(params)
    print(f"  {params.channel.name}: {params.value}")


async def run():
    print("\nConnecting for 5 seconds...")

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

    print(f"\n{len(received)} data changes received")

    # -- Read archived data --
    print("\n--- Archived (auto-typed from characteristic) ---")
    results = await client.load_channel_data(
        client.LoadChannelDataParams(channel="temperature", limit=5)
    )
    for t in results:
        print(f"  {type(t).__name__}: {t.value:.2f}")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nStopped.")
