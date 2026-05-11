"""Live OPC UA DataTool View - extends archive view with realtime plot.

Adds a live tab that subscribes to selected channels via OPC UA and
plots incoming data in realtime with a rolling time window.

Usage:
    from opensemantic.lab.ui import LiveDataToolView

    view = LiveDataToolView(controllers=[opcua_ctrl])
    view.servable()
"""

import asyncio
import datetime as dt
import logging
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import panel as pn
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, Range1d
from bokeh.plotting import figure as bk_figure

from opensemantic.base.ui._channel_utils import (
    _t,
    get_display_label,
    get_unit_enum,
    group_channels_by_characteristic,
    resolve_value_type,
)
from opensemantic.base.ui._config import LiveDashboardConfig
from opensemantic.base.ui._datatool_dashboard import DataToolView

_logger = logging.getLogger(__name__)


def get_unit_enum_for_uuid(ch_uuid, selected):
    """Find the UnitEnum for a channel by UUID from the selected list."""
    for ctrl, ch in selected:
        if ch.uuid == ch_uuid:
            return get_unit_enum(ch)
    return None


class LiveDataToolView(DataToolView):
    """DataTool view with live OPC UA subscription support.

    Extends DataToolView with a second tab showing realtime data
    from OPC UA subscriptions with a configurable rolling time window.

    Parameters
    ----------
    controllers
        List of DataToolController or OpcUaServer instances.
    config
        LiveDashboardConfig with live subscription settings.
    title
        View title.
    """

    def __init__(
        self,
        controllers: Optional[List[Any]] = None,
        config: Optional[LiveDashboardConfig] = None,
        title: str = "DataTool Dashboard",
    ):
        self._live_config = config or LiveDashboardConfig()
        # Live state
        self._buffer_size = self._live_config.live.buffer_size
        self._live_buffers: Dict[str, Deque[Tuple[dt.datetime, Any]]] = defaultdict(
            lambda: deque(maxlen=self._buffer_size)
        )
        self._live_active = False
        self._periodic_callback = None
        self._original_callbacks: Dict[str, Any] = {}

        super().__init__(
            controllers=controllers,
            config=self._live_config,
            title=title,
        )

    @staticmethod
    def _is_opcua_controller(ctrl: Any) -> bool:
        """Check if a controller supports OPC UA live subscriptions."""
        try:
            from opensemantic.lab._controller_mixin import OpcUaServerMixin

            return isinstance(ctrl, OpcUaServerMixin)
        except ImportError:
            return False

    # -- Override plot building to add tabs --

    def _build_plot(self):
        """Create archive and live plot panes in a Tabs layout."""
        super()._build_plot()

        # Live plot uses Bokeh for efficient streaming via ColumnDataSource
        self._live_sources: Dict[str, ColumnDataSource] = {}
        self._live_figures: List[Any] = []
        self._live_bokeh_col = pn.Column(sizing_mode="stretch_both")

        self._live_toggle = pn.widgets.Toggle(
            name=_t("live", self.lang),
            button_type="primary",
        )
        self._live_toggle.param.watch(self._on_live_toggle, ["value"])

        self._live_panel = pn.Column(
            pn.Row(self._live_toggle),
            self._live_bokeh_col,
        )

        self._tabs = pn.Tabs(
            (_t("archive", self.lang), self._plot_col),
            (_t("live", self.lang), self._live_panel),
        )
        self._plot_card.objects = [self._tabs]

    def _build_main_area(self):
        """Override to use tabbed plot card."""
        self._app.main_set([self._plot_card, self._log_card])

    def _on_source_change(self, *args):
        """Override: also rebuild live figures when selection changes."""
        super()._on_source_change(*args)
        if self._live_active and self._live_fig_built:
            self._install_live_callbacks()
            self._rebuild_live_figures()

    def _on_unit_change(self, group_key, event):
        """Override: also rebuild live figures when unit changes."""
        super()._on_unit_change(group_key, event)
        if self._live_active and self._live_fig_built:
            self._rebuild_live_figures()

    def _rebuild_live_figures(self):
        """Rebuild live Bokeh figures for current selection."""
        self._live_fig_built = False
        self._build_live_figures()

    # -- Live subscription --

    def _on_live_toggle(self, event):
        if event.new:
            self._live_toggle.button_type = "success"
            self._start_live()
        else:
            self._live_toggle.button_type = "primary"
            self._stop_live()

    def _install_live_callbacks(self):
        """Install data change callbacks on all selected OPC UA controllers."""
        for ctrl, ch in self._selected:
            if not self._is_opcua_controller(ctrl):
                continue
            ctrl_key = str(id(ctrl))
            if ctrl_key not in self._original_callbacks:
                old_cb = getattr(
                    ctrl, "_channel_datachange_notification_callback", None
                )
                self._original_callbacks[ctrl_key] = old_cb

                async def on_data(params, _ctrl=ctrl, _old=old_cb):
                    self._on_live_data(params)
                    if _old is not None:
                        try:
                            await _old(params)
                        except Exception as e:
                            _logger.error("Error in original callback: %s", e)

                ctrl._channel_datachange_notification_callback = on_data

    def _start_live(self):
        """Begin live data collection from OPC UA controllers."""
        self._live_active = True
        self._live_buffers.clear()
        self._live_trace_map = {}
        self._live_fig_built = False

        self._install_live_callbacks()

        self._periodic_callback = pn.state.add_periodic_callback(
            self._update_live_plot,
            period=self._live_config.live.update_interval_ms,
        )

    def _stop_live(self):
        """Stop live data collection and restore original callbacks."""
        self._live_active = False
        if self._periodic_callback is not None:
            self._periodic_callback.stop()
            self._periodic_callback = None

        # Restore original callbacks
        for ctrl, ch in self._selected:
            ctrl_key = str(id(ctrl))
            if ctrl_key in self._original_callbacks:
                ctrl._channel_datachange_notification_callback = (
                    self._original_callbacks.pop(ctrl_key)
                )

    def _on_live_data(self, params: Any):
        """Handle incoming OPC UA data change notification.

        Appends (timestamp, value) to the channel's live buffer.
        """
        if not self._live_active:
            return
        ch = params.channel
        ch_uuid = ch.uuid
        value = params.value
        ts = params.timestamp or dt.datetime.now(dt.timezone.utc)

        # Value is now typed by the controller's _wrap_raw_value
        self._live_buffers[ch_uuid].append((ts, value))

    def _build_live_figures(self):
        """Build Bokeh figures with ColumnDataSources for each group."""
        from opensemantic.base.ui._datatool_dashboard import COLORS

        groups = group_channels_by_characteristic(
            self._selected, self._live_config.plot.grouping
        )

        self._live_sources.clear()
        self._live_figures.clear()
        self._live_trace_map = {}
        self._live_bokeh_col.clear()

        color_idx = 0
        for group_key, channels in groups.items():
            if not channels:
                continue
            sample_ch = channels[0][1]
            vtype = resolve_value_type(sample_ch)
            if vtype == "text":
                continue

            axis_label = self._get_axis_label(group_key)
            now = dt.datetime.now(dt.timezone.utc)
            history_s = self._live_config.live.history_seconds
            x_range = Range1d(
                start=now - dt.timedelta(seconds=history_s),
                end=now,
            )
            fig = bk_figure(
                height=250,
                sizing_mode="stretch_width",
                x_axis_type="datetime",
                y_axis_label=axis_label,
                x_range=x_range,
            )
            fig.xaxis.formatter = DatetimeTickFormatter(
                seconds="%H:%M:%S",
                minutes="%H:%M",
                hours="%H:%M",
            )

            for ctrl, ch in channels:
                src = ColumnDataSource(data={"x": [], "y": []})
                self._live_sources[ch.uuid] = src
                self._live_trace_map[ch.uuid] = (group_key, vtype)
                trace_name = (
                    f"{get_display_label(ctrl, self.lang)}/"
                    f"{get_display_label(ch, self.lang)}"
                )
                fig.line(
                    "x",
                    "y",
                    source=src,
                    legend_label=trace_name,
                    color=COLORS[color_idx % len(COLORS)],
                    line_width=2,
                )
                color_idx += 1

            fig.legend.click_policy = "hide"
            self._live_figures.append(fig)
            self._live_bokeh_col.append(pn.pane.Bokeh(fig, sizing_mode="stretch_width"))

        self._live_fig_built = True

    def _update_live_plot(self):
        """Periodic callback: stream data into Bokeh ColumnDataSources."""
        if not self._live_active or self._live_config is None:
            return

        if not self._live_fig_built:
            self._build_live_figures()
            return

        now = dt.datetime.now(dt.timezone.utc)
        history = getattr(
            getattr(self._live_config, "live", None), "history_seconds", 30
        )
        window_start = now - dt.timedelta(seconds=history)

        for ch_uuid, src in self._live_sources.items():
            buf = self._live_buffers.get(ch_uuid)
            if not buf:
                continue

            group_key, vtype = self._live_trace_map.get(ch_uuid, (None, None))
            sorted_buf = sorted(buf, key=lambda x: x[0])

            # Convert timestamps to local time
            timestamps = [
                (
                    ts.astimezone(tz=None).replace(tzinfo=None)
                    if ts.tzinfo is not None
                    else ts
                )
                for ts, v in sorted_buf
            ]

            # Extract numeric values with optional unit conversion
            target_unit_name = (
                self._unit_selections.get(group_key) if group_key else None
            )
            values = []
            for ts, v in sorted_buf:
                # Convert to target unit if value is typed
                if target_unit_name and hasattr(v, "to_unit"):
                    unit_enum = get_unit_enum_for_uuid(ch_uuid, self._selected)
                    if unit_enum and target_unit_name in unit_enum.__members__:
                        try:
                            v = v.to_unit(unit_enum[target_unit_name])
                        except Exception:
                            pass
                # Extract numeric value
                if hasattr(v, "value"):
                    values.append(v.value)
                elif isinstance(v, dict):
                    values.append(v.get("value", 0))
                else:
                    values.append(v)

            src.data = {"x": timestamps, "y": values}

        # Update x-range on all figures (local time)
        now = dt.datetime.now()
        history = getattr(
            getattr(self._live_config, "live", None), "history_seconds", 30
        )
        window_start = now - dt.timedelta(seconds=history)
        for fig in self._live_figures:
            fig.x_range.start = window_start
            fig.x_range.end = now

    # -- Config change handling --

    def _on_config_editor_change(self, event):
        if not event.new or not isinstance(event.new, dict):
            return
        try:
            new_config = LiveDashboardConfig(**event.new)
        except Exception as e:
            _logger.debug("Incomplete live config, skipping: %s", e)
            return
        if new_config is None:
            return

        old_live = self._live_config.live
        self._live_config = new_config
        self._config = new_config

        # Handle live config changes
        if old_live.buffer_size != new_config.live.buffer_size:
            # Resize buffers
            self._buffer_size = new_config.live.buffer_size
            new_buffers: Dict[str, Deque] = defaultdict(
                lambda: deque(maxlen=self._buffer_size)
            )
            for k, buf in self._live_buffers.items():
                new_buffers[k].extend(buf)
            self._live_buffers = new_buffers

        if (
            old_live.update_interval_ms != new_config.live.update_interval_ms
            and self._periodic_callback is not None
        ):
            self._periodic_callback.stop()
            self._periodic_callback = pn.state.add_periodic_callback(
                self._update_live_plot,
                period=new_config.live.update_interval_ms,
            )

        # Delegate base config changes
        super()._on_config_editor_change(event)

    def _on_controllers_changed(self):
        """Handle controllers change - also manages OPC UA lifecycle."""
        was_live = self._live_active
        if was_live:
            self._stop_live()

        super()._on_controllers_changed()

        # Auto-connect new OPC UA controllers
        for ctrl in self._controllers:
            if self._is_opcua_controller(ctrl):
                state = getattr(ctrl, "_state", None)
                if state is None or str(state) == "ControllerState.idle":
                    try:
                        asyncio.ensure_future(ctrl.run_as_client())
                    except Exception as e:
                        _logger.error(
                            "Failed to start OPC UA client for %s: %s",
                            getattr(ctrl, "name", "unknown"),
                            e,
                        )

        if was_live:
            self._start_live()

    def _rebuild_ui_labels(self):
        """Rebuild all UI labels including live-specific ones."""
        super()._rebuild_ui_labels()
        if hasattr(self, "_tabs"):
            self._tabs[0] = (_t("archive", self.lang), self._tabs[0][1])
            self._tabs[1] = (_t("live", self.lang), self._tabs[1][1])
