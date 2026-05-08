"""Minimal OPC UA server example.

Defines an OpcUaServer, stores the instance to an oold file store,
and runs the server. The client (opc_ua_client.py) loads the same
instance and connects.

Usage:
    python opc_ua_server.py
"""

import asyncio
import logging
import random
from uuid import NAMESPACE_URL, uuid5

from oold.backend.document_store import SimpleDictDocumentStore
from oold.backend.interface import (
    SetBackendParam,
    SetResolverParam,
    StoreParam,
    set_backend,
    set_resolver,
)

from opensemantic import compute_scoped_uuid
from opensemantic.base.v1 import Database
from opensemantic.characteristics.quantitative.v1 import (
    Temperature,
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

SERVER_UUID = uuid5(NAMESPACE_URL, "minimal-example-server")
OPC_URL = "opc.tcp://localhost:48410"
STORE_FILE = "./server_store.json"

# -- Set up oold file store --
store = SimpleDictDocumentStore(file_path=STORE_FILE)
set_resolver(SetResolverParam(iri="Item", resolver=store))
set_backend(SetBackendParam(iri="Item", backend=store))

# -- Define and store the server entity --
server = OpcUaServer(
    uuid=SERVER_UUID,
    name="minimal_example_server",
    label=[Label(text="Minimal Example Server")],
    url=OPC_URL,
    data_channels=[
        OpcUaDataChannel(
            uuid=compute_scoped_uuid(SERVER_UUID, "ns=2;s=Temperature"),
            node_id="ns=2;s=Temperature",
            name="temperature",
            label=[Label(text="Temperature Sensor")],
            opcua_data_type=OPCUADataType.Float,
            client_mode=OpcUaClientMode.Subscription,
            sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
            refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
            characteristic=Temperature.get_cls_iri(),
        ),
    ],
    storage_locations=[Database(name="example_archive", label=[Label(text="Archive")])],
    auto_archive=True,
)

store.store(StoreParam(nodes={server.get_iri(): server}))
print(f"Stored: {server.get_iri()} -> {STORE_FILE}")
print(f"  URL: {OPC_URL}")
print(f"  Channels: {[ch.name for ch in server.data_channels]}")


async def generate_value(params):
    return random.uniform(18.0, 28.0)


async def run():
    print("\nStarting server (Ctrl+C to stop)...")
    await server.run_as_server(
        OpcUaServer.RunAsServerParams(
            get_channel_value_callback=generate_value,
        )
    )


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nStopped.")
