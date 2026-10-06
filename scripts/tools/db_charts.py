#!/usr/bin/env python3
"""
tools/db_charts.py

Chart rendering utilities for the SentimentAPI database viewer.

Provides two functions:
    ascii_line_chart  — inline terminal chart via plotext
    export_chart_png  — file export via matplotlib

Both share a consistent colour map for sentiment layers.
"""
from __future__ import annotations

from datetime import datetime

# ---------------------------------------------------------------------------
# Colour maps (shared between ASCII and PNG renderers)
# ---------------------------------------------------------------------------

# plotext colour names
_PLOTEXT_COLOURS: dict[str, str] = {
    "composite":  "cyan",
    "market":     "green",
    "narrative":  "blue",
    "influencer": "magenta",
    "macro":      "yellow",
    "confidence": "white",
}

# matplotlib hex colours (matching the terminal palette intent)
_MPL_COLOURS: dict[str, str] = {
    "composite":  "#00bcd4",
    "market":     "#4caf50",
    "narrative":  "#2196f3",
    "influencer": "#9c27b0",
    "macro":      "#ffc107",
    "confidence": "#9e9e9e",
}


def _pick_plotext_colour(label: str) -> str:
    """Return a plotext colour for the given series label."""
    key = label.lower().split("_")[0].split(" ")[0]
    return _PLOTEXT_COLOURS.get(key, "white")


def _pick_mpl_colour(label: str) -> str:
    """Return a matplotlib hex colour for the given series label."""
    key = label.lower().split("_")[0].split(" ")[0]
    return _MPL_COLOURS.get(key, "#9e9e9e")


# ---------------------------------------------------------------------------
# ASCII chart (plotext)
# ---------------------------------------------------------------------------

def ascii_line_chart(
    title: str,
    series: dict[str, list[tuple[datetime, float]]],
    *,
    width: int = 90,
    height: int = 18,
    y_range: tuple[float, float] | None = None,
) -> None:
    """
    Render a multi-series line chart directly to stdout using plotext.

    Parameters
    ----------
    title   : chart title displayed above the plot
    series  : mapping of label → list of (timestamp, value) pairs
    width   : terminal columns for the plot
    height  : terminal rows for the plot
    y_range : optional (min, max) for the y-axis

    Prints the chart directly via plt.show(). Returns None.
    """
    import plotext as plt

    # Filter out empty / all-None series
    valid_series: dict[str, list[tuple[datetime, float]]] = {}
    for label, points in series.items():
        cleaned = [(ts, v) for ts, v in points if v is not None]
        if cleaned:
            valid_series[label] = cleaned

    if not valid_series:
        print(f"  No data for chart: {title}")
        return

    plt.clear_figure()
    plt.plotsize(width, height)
    plt.title(title)
    plt.date_form("Y-m-d H:M")

    for label, points in valid_series.items():
        timestamps = [ts for ts, _ in points]
        values = [v for _, v in points]
        colour = _pick_plotext_colour(label)
        x_dates = plt.datetimes_to_string(timestamps)
        plt.plot(
            x_dates,
            values,
            label=label,
            color=colour,
        )

    if y_range is not None:
        plt.ylim(y_range[0], y_range[1])

    plt.theme("dark")
    plt.xlabel("Time")

    plt.show()


def export_chart_png(
    title: str,
    series: dict[str, list[tuple[datetime, float]]],
    output_path: str,
    *,
    y_range: tuple[float, float] | None = None,
) -> str:
    """
    Render the same data to a PNG file via matplotlib.

    Parameters
    ----------
    title       : chart title
    series      : mapping of label → list of (timestamp, value) pairs
    output_path : file path for the PNG output
    y_range     : optional (min, max) for the y-axis

    Returns the absolute path to the written PNG, or an empty string if
    no data was available to plot.
    """
    import os

    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    # Filter out empty / all-None series
    valid_series: dict[str, list[tuple[datetime, float]]] = {}
    for label, points in series.items():
        cleaned = [(ts, v) for ts, v in points if v is not None]
        if cleaned:
            valid_series[label] = cleaned

    if not valid_series:
        return ""

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor("#1e1e1e")
    ax.set_facecolor("#2d2d2d")

    for label, points in valid_series.items():
        timestamps = [ts for ts, _ in points]
        values = [v for _, v in points]
        colour = _pick_mpl_colour(label)
        ax.plot(timestamps, values, label=label, color=colour, linewidth=1.5)

    if y_range is not None:
        ax.set_ylim(y_range[0], y_range[1])

    ax.set_title(title, color="white", fontsize=13, pad=10)
    ax.set_xlabel("Time", color="white")
    ax.tick_params(colors="white")
    ax.spines["bottom"].set_color("#555")
    ax.spines["left"].set_color("#555")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.2)

    # Date formatting on x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    fig.autofmt_xdate(rotation=30)

    legend = ax.legend(loc="upper left", framealpha=0.7)
    for text in legend.get_texts():
        text.set_color("white")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    return os.path.abspath(output_path)
