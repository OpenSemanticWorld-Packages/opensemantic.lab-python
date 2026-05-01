import asyncio
from uuid import NAMESPACE_URL, uuid5

import pytest

from opensemantic import compute_scoped_uuid
from opensemantic.characteristics.quantitative import Time, TimeUnit
from opensemantic.core._model import Label
from opensemantic.lab import OpcUaClientMode, OpcUaDataChannel
from opensemantic.lab.v1 import OpcUaDataChannel as OpcUaDataChannel_v1

try:
    from opensemantic.lab.v1 import OpcUaServer as OpcUaServer_v1

    _HAS_ASYNCUA_V1 = True
except ImportError:
    _HAS_ASYNCUA_V1 = False
from opensemantic.lab._controller_logic import (
    calculate_queue_size,
    get_interval_ms,
    get_opcua_initial_value,
)

# Deterministic UUIDs for tests
_TEST_SERVER_UUID = uuid5(NAMESPACE_URL, "test-server")
_TEST_SUB_SERVER_UUID = uuid5(NAMESPACE_URL, "test-sub-server")
_TEST_INT_SERVER_UUID = uuid5(NAMESPACE_URL, "test-int-server")


# -- Controller logic tests --


def test_compute_scoped_uuid():
    parent = uuid5(NAMESPACE_URL, "server-a")
    child_a = compute_scoped_uuid(parent, "ns=2;s=Temperature")
    child_b = compute_scoped_uuid(parent, "ns=2;s=Pressure")
    assert child_a != child_b
    # Same input always gives same result
    assert compute_scoped_uuid(parent, "ns=2;s=Temperature") == child_a
    # Different parent gives different result for same node_id
    parent2 = uuid5(NAMESPACE_URL, "server-b")
    assert compute_scoped_uuid(parent2, "ns=2;s=Temperature") != child_a


def test_get_interval_ms_milliseconds():
    t = Time(value=1000, unit=TimeUnit.milli_second)
    assert get_interval_ms(t) == pytest.approx(1000.0)


def test_get_interval_ms_seconds():
    t = Time(value=1.5, unit=TimeUnit.second)
    assert get_interval_ms(t) == pytest.approx(1500.0)


def test_get_interval_ms_minutes():
    t = Time(value=2, unit=TimeUnit.minute)
    assert get_interval_ms(t) == pytest.approx(120000.0)


def test_get_interval_ms_none():
    assert get_interval_ms(None) is None


def test_get_opcua_initial_value():
    assert get_opcua_initial_value("Float") == 0.0
    assert get_opcua_initial_value("String") == ""
    assert get_opcua_initial_value("Int32") == 0
    assert get_opcua_initial_value("Boolean") is False


def test_calculate_queue_size():
    assert calculate_queue_size(1000, 100) == 10
    assert calculate_queue_size(1000, 0) == 101
    assert calculate_queue_size(1000, None) == 0


# -- OpcUaDataChannel tests --


def test_channel_requires_uuid():
    with pytest.raises(ValueError, match="uuid is required"):
        OpcUaDataChannel(
            node_id="ns=2;s=Temperature",
            name="Temperature",
        )


def test_channel_with_explicit_uuid():
    uid = str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Temperature"))
    ch = OpcUaDataChannel(
        uuid=uid,
        node_id="ns=2;s=Temperature",
        name="Temperature",
    )
    assert ch.uuid == str(uid)


def test_channel_get_osw_id():
    uid = str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Pressure"))
    ch = OpcUaDataChannel(
        uuid=uid,
        node_id="ns=2;s=Pressure",
        name="Pressure",
    )
    osw_id = ch.get_osw_id()
    assert osw_id.startswith("OSW")
    assert "-" not in osw_id


def test_channel_str():
    uid = str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Test"))
    ch = OpcUaDataChannel(
        uuid=uid,
        node_id="ns=2;s=Test",
        name="TestChannel",
        opcua_data_type="Float",
    )
    assert "TestChannel" in str(ch)


def test_channel_subchannels():
    sub = OpcUaDataChannel(
        uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Sub")),
        node_id="ns=2;s=Sub",
        name="SubChannel",
    )
    main = OpcUaDataChannel(
        uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Main")),
        node_id="ns=2;s=Main",
        name="MainChannel",
        subchannels=[sub],
    )
    assert len(main.subchannels) == 1
    assert main.subchannels[0].name == "SubChannel"


# -- Subobject ID tests --


def test_channel_osw_id_prefixed_by_server():
    """Channel osw_id gets server prefix when assigned to a server."""
    try:
        from opensemantic.lab import OpcUaServer
    except ImportError:
        pytest.skip("asyncua not installed")

    ch_uuid = str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Temperature"))
    ch = OpcUaDataChannel(
        uuid=ch_uuid,
        node_id="ns=2;s=Temperature",
        name="Temperature",
        opcua_data_type="Float",
    )
    server = OpcUaServer(
        uuid=_TEST_SERVER_UUID,
        name="TestServer",
        label=[Label(text="Test")],
        url="opc.tcp://localhost:48400",
        data_channels=[ch],
    )
    server_osw = server.get_osw_id()
    ch_base = f"OSW{str(ch_uuid).replace('-', '')}"
    assert server.data_channels[0].osw_id == f"{server_osw}#{ch_base}"


# -- OpcUaServer tests (requires asyncua) --

try:
    from opensemantic.lab import OpcUaServer

    _HAS_ASYNCUA = True
except ImportError:
    _HAS_ASYNCUA = False


@pytest.fixture
def server_with_channels():
    channels = [
        OpcUaDataChannel(
            uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Temp")),
            node_id="ns=2;s=Temp",
            name="Temperature",
            opcua_data_type="Float",
            refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
            sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
            client_mode=OpcUaClientMode.Subscription,
        ),
        OpcUaDataChannel(
            uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Press")),
            node_id="ns=2;s=Press",
            name="Pressure",
            opcua_data_type="Float",
            refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
            sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
            client_mode=OpcUaClientMode.Subscription,
        ),
    ]
    sub_channels = [
        OpcUaDataChannel(
            uuid=str(compute_scoped_uuid(_TEST_SUB_SERVER_UUID, "ns=2;s=SubTemp")),
            node_id="ns=2;s=SubTemp",
            name="SubTemperature",
            opcua_data_type="Float",
        ),
    ]
    sub = OpcUaServer(
        uuid=_TEST_SUB_SERVER_UUID,
        name="SubDevice",
        label=[Label(text="Sub", lang="en")],
        url="opc.tcp://localhost:48401",
        data_channels=sub_channels,
    )
    server = OpcUaServer(
        uuid=_TEST_SERVER_UUID,
        name="MainDevice",
        label=[Label(text="Main", lang="en")],
        url="opc.tcp://localhost:48401",
        data_channels=channels,
        subdevices=[sub],
    )
    return server


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_get_all_channels(server_with_channels):
    all_ch = server_with_channels.get_all_channels()
    assert len(all_ch) == 3


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_get_subdevices(server_with_channels):
    subs = server_with_channels.get_subdevices()
    assert len(subs) == 1
    assert subs[0].name == "SubDevice"


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_get_channel_owner(server_with_channels):
    all_ch = server_with_channels.get_all_channels()
    temp_ch = [c for c in all_ch if c.name == "Temperature"][0]
    sub_ch = [c for c in all_ch if c.name == "SubTemperature"][0]

    assert server_with_channels.get_channel_owner(temp_ch).name == "MainDevice"
    assert server_with_channels.get_channel_owner(sub_ch).name == "SubDevice"


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_get_channel_owner_not_found(server_with_channels):
    unknown = OpcUaDataChannel(
        uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=Unknown")),
        node_id="ns=2;s=Unknown",
        name="Unknown",
    )
    with pytest.raises(ValueError, match="not found"):
        server_with_channels.get_channel_owner(unknown)


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_get_osw_id(server_with_channels):
    osw_id = server_with_channels.get_osw_id()
    assert osw_id.startswith("OSW")
    assert "-" not in osw_id


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_get_iri(server_with_channels):
    iri = server_with_channels.get_iri()
    assert iri.startswith("Item:OSW")


@pytest.mark.skipif(not _HAS_ASYNCUA, reason="asyncua not installed")
def test_server_client_integration(server_with_channels):
    """Full integration: start server, connect client, exchange data."""
    channels = [
        OpcUaDataChannel(
            uuid=str(compute_scoped_uuid(_TEST_INT_SERVER_UUID, "ns=2;s=IntTest.Temp")),
            node_id="ns=2;s=IntTest.Temp",
            name="Temperature",
            opcua_data_type="Float",
            refresh_interval=Time(value=500, unit=TimeUnit.milli_second),
            sampling_interval=Time(value=100, unit=TimeUnit.milli_second),
            client_mode=OpcUaClientMode.Subscription,
        ),
    ]

    server = OpcUaServer(
        uuid=_TEST_INT_SERVER_UUID,
        name="IntServer",
        label=[Label(text="Server", lang="en")],
        url="opc.tcp://localhost:48402",
        data_channels=channels,
    )
    client = OpcUaServer(
        uuid=_TEST_INT_SERVER_UUID,
        name="IntClient",
        label=[Label(text="Client", lang="en")],
        url="opc.tcp://localhost:48402",
        data_channels=channels,
    )

    received = []

    async def get_value(params):
        import random

        return random.uniform(20.0, 25.0)

    async def on_data_change(params):
        received.append((params.channel.name, params.value))

    async def timeout():
        await asyncio.sleep(3)
        await client.stop()
        await server.stop()

    async def run():
        await asyncio.gather(
            server.run_as_server(
                OpcUaServer.RunAsServerParams(get_channel_value_callback=get_value)
            ),
            client.run_as_client(
                OpcUaServer.RunAsClientParams(
                    channel_datachange_notification_callback=on_data_change,
                )
            ),
            timeout(),
        )

    asyncio.run(run())
    assert len(received) > 0, "No data changes received"


# -- v1 OpcUaDataChannel tests --


def test_v1_channel_subclasses_v1_model():
    from opensemantic.lab.v1._model import OpcUaDataChannel as OpcUaDataChannel_v1_model

    assert issubclass(OpcUaDataChannel_v1, OpcUaDataChannel_v1_model)


def test_v1_channel_not_subclass_of_v2_model():
    from opensemantic.lab._model import OpcUaDataChannel as OpcUaDataChannel_v2_model

    assert not issubclass(OpcUaDataChannel_v1, OpcUaDataChannel_v2_model)


def test_v2_channel_subclasses_v2_model():
    from opensemantic.lab._model import OpcUaDataChannel as OpcUaDataChannel_v2_model

    assert issubclass(OpcUaDataChannel, OpcUaDataChannel_v2_model)


def test_v2_channel_not_subclass_of_v1_model():
    from opensemantic.lab.v1._model import OpcUaDataChannel as OpcUaDataChannel_v1_model

    assert not issubclass(OpcUaDataChannel, OpcUaDataChannel_v1_model)


def test_v1_channel_requires_uuid():
    with pytest.raises(ValueError, match="uuid is required"):
        OpcUaDataChannel_v1(
            node_id="ns=2;s=V1Temp",
            name="V1Temperature",
        )


def test_v1_channel_with_explicit_uuid():
    uid = str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=V1Temp"))
    ch = OpcUaDataChannel_v1(
        uuid=uid,
        node_id="ns=2;s=V1Temp",
        name="V1Temperature",
    )
    assert ch.uuid == str(uid)


def test_v1_channel_get_osw_id():
    uid = str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=V1Press"))
    ch = OpcUaDataChannel_v1(
        uuid=uid,
        node_id="ns=2;s=V1Press",
        name="V1Pressure",
    )
    osw_id = ch.get_osw_id()
    assert osw_id.startswith("OSW")
    assert "-" not in osw_id


def test_v1_channel_subchannels():
    sub = OpcUaDataChannel_v1(
        uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=V1Sub")),
        node_id="ns=2;s=V1Sub",
        name="V1SubChannel",
    )
    main = OpcUaDataChannel_v1(
        uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=V1Main")),
        node_id="ns=2;s=V1Main",
        name="V1MainChannel",
        subchannels=[sub],
    )
    assert len(main.subchannels) == 1
    assert main.subchannels[0].name == "V1SubChannel"


# -- v1 OpcUaServer tests --


@pytest.mark.skipif(not _HAS_ASYNCUA_V1, reason="asyncua not installed")
def test_v1_server_subclasses_v1_model():
    from opensemantic.lab.v1._model import OpcUaServer as OpcUaServer_v1_model

    assert issubclass(OpcUaServer_v1, OpcUaServer_v1_model)


@pytest.mark.skipif(not _HAS_ASYNCUA_V1, reason="asyncua not installed")
def test_v1_server_not_subclass_of_v2_model():
    from opensemantic.lab._model import OpcUaServer as OpcUaServer_v2_model

    assert not issubclass(OpcUaServer_v1, OpcUaServer_v2_model)


@pytest.mark.skipif(not _HAS_ASYNCUA_V1, reason="asyncua not installed")
def test_v2_server_subclasses_v2_model():
    from opensemantic.lab._model import OpcUaServer as OpcUaServer_v2_model

    assert issubclass(OpcUaServer, OpcUaServer_v2_model)


@pytest.mark.skipif(not _HAS_ASYNCUA_V1, reason="asyncua not installed")
def test_v2_server_not_subclass_of_v1_model():
    from opensemantic.lab.v1._model import OpcUaServer as OpcUaServer_v1_model

    assert not issubclass(OpcUaServer, OpcUaServer_v1_model)


@pytest.mark.skipif(not _HAS_ASYNCUA_V1, reason="asyncua not installed")
def test_v1_server_get_all_channels():
    from opensemantic.core.v1._model import Label as Label_v1

    channels = [
        OpcUaDataChannel_v1(
            uuid=str(compute_scoped_uuid(_TEST_SERVER_UUID, "ns=2;s=V1Temp")),
            node_id="ns=2;s=V1Temp",
            name="V1Temperature",
            opcua_data_type="Float",
        ),
    ]
    server = OpcUaServer_v1(
        uuid=_TEST_SERVER_UUID,
        name="V1Server",
        label=[Label_v1(text="V1 Server", lang="en")],
        url="opc.tcp://localhost:48403",
        data_channels=channels,
    )
    assert len(server.get_all_channels()) == 1
    assert server.get_all_channels()[0].name == "V1Temperature"


@pytest.mark.skipif(not _HAS_ASYNCUA_V1, reason="asyncua not installed")
def test_v1_server_has_async_methods():
    assert hasattr(OpcUaServer_v1, "run_as_client")
    assert hasattr(OpcUaServer_v1, "run_as_server")
    assert hasattr(OpcUaServer_v1, "read_channel")
    assert hasattr(OpcUaServer_v1, "write_channel")
    assert hasattr(OpcUaServer_v1, "stop")
