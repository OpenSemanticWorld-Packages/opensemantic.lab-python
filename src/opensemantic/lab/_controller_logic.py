"""Pure helper functions for OPC UA controller logic.

Shared by v1 and v2 controller implementations. No Pydantic imports.
"""

import datetime
from typing import Any, Dict, Optional


def get_interval_ms(interval) -> Optional[float]:
    """Convert a Time QuantityValue to milliseconds.

    Args:
        interval: A Time instance (QuantityValue subclass) or None.

    Returns:
        The interval in milliseconds, or None if interval is None.
    """
    if interval is None:
        return None
    return interval.to_pint().to("millisecond").magnitude


def get_opcua_initial_value(variant_type_name: str) -> Any:
    """Get the initial value for a given OPC UA VariantType name.

    Args:
        variant_type_name: Name of the ua.VariantType enum member.

    Returns:
        An appropriate default value for the given type.
    """
    if variant_type_name == "String":
        return ""
    elif variant_type_name in (
        "Int16",
        "Int32",
        "Int64",
        "UInt16",
        "UInt32",
        "UInt64",
    ):
        return 0
    elif variant_type_name == "Boolean":
        return False
    elif variant_type_name == "DateTime":
        return datetime.datetime.now(datetime.timezone.utc)
    return 0.0


def group_channels_by_interval(
    channels: list,
    mode_filter: Optional[str] = None,
) -> Dict[float, list]:
    """Group channels by their refresh interval in milliseconds.

    Args:
        channels: List of OpcUaDataChannel instances.
        mode_filter: If set, only include channels with this client_mode value.
            Use "Subscription" or "Read"/"RegisteredRead".

    Returns:
        Dict mapping interval_ms to list of channels.
    """
    grouped: Dict[float, list] = {}
    for channel in channels:
        if mode_filter == "Subscription":
            if not hasattr(channel, "client_mode") or channel.client_mode is None:
                continue
            if channel.client_mode.value != "Subscription":
                continue
        elif mode_filter == "Read":
            if not hasattr(channel, "client_mode") or channel.client_mode is None:
                continue
            if channel.client_mode.value not in ("Read", "RegisteredRead"):
                continue

        interval_ms = get_interval_ms(channel.refresh_interval)
        if interval_ms is None:
            if mode_filter == "Read":
                continue
            interval_ms = 0.0

        if interval_ms not in grouped:
            grouped[interval_ms] = []
        grouped[interval_ms].append(channel)
    return grouped


def calculate_queue_size(
    refresh_interval_ms: float,
    sampling_interval_ms: Optional[float],
) -> int:
    """Calculate subscription queue size from refresh and sampling intervals.

    Args:
        refresh_interval_ms: Refresh interval in milliseconds.
        sampling_interval_ms: Sampling interval in milliseconds.

    Returns:
        Queue size for the subscription.
    """
    if sampling_interval_ms is not None and sampling_interval_ms > 0:
        if refresh_interval_ms > 0:
            return int(refresh_interval_ms / sampling_interval_ms)
    if sampling_interval_ms == 0:
        # assume fastest possible sampling interval as 10ms
        return int(refresh_interval_ms / 10) + 1
    return 0
