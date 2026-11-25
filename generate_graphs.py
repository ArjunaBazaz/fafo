#!/usr/bin/env python3
"""Plot four series with per-point ±1 standard deviation error bars.

CSV header (nine columns):
contention,FAFO,FAFO stdev,OCC,OCC stdev,Block-STM,Block-STM stdev,OptME,OptME stdev

Usage:
    python3 generate_graphs.py results.csv --output graph.png

Dependency: matplotlib (python3 -m pip install matplotlib).
"""
import argparse
import csv
import math
from pathlib import Path


SERIES = ("FAFO", "OCC", "Block-STM", "OptME")


def read_results(csv_file):
    """Return sorted contention values and each series' means/deviations."""
    expected = {"contention"}
    expected.update(SERIES)
    expected.update(f"{name} stdev" for name in SERIES)
    rows = []
    with Path(csv_file).open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        headers = [name.strip() for name in (reader.fieldnames or [])]
        if len(headers) != len(expected) or set(headers) != expected:
            raise ValueError(
                "CSV must contain nine columns: contention, FAFO, FAFO stdev, "
                "OCC, OCC stdev, Block-STM, Block-STM stdev, OptME, OptME stdev"
            )
        reader.fieldnames = headers
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"CSV line {reader.line_num}: expected nine values")
            try:
                values = {key: float(value) for key, value in row.items()}
            except ValueError as error:
                raise ValueError(f"CSV line {reader.line_num}: all values must be numeric") from error
            if not all(math.isfinite(value) for value in values.values()):
                raise ValueError(f"CSV line {reader.line_num}: values must be finite")
            if any(values[f"{name} stdev"] < 0 for name in SERIES):
                raise ValueError(f"CSV line {reader.line_num}: standard deviations cannot be negative")
            rows.append(values)
    rows.sort(key=lambda row: row["contention"])
    if len(rows) != 10 or any(
        not math.isclose(row["contention"], index / 10, rel_tol=0, abs_tol=1e-9)
        for index, row in enumerate(rows)
    ):
        raise ValueError("CSV must have exactly one row for each contention from 0.0 to 0.9")
    contentions = [row["contention"] for row in rows]
    data = {
        name: ([row[name] for row in rows], [row[f"{name} stdev"] for row in rows])
        for name in SERIES
    }
    return contentions, data


def generate_graph(csv_file, output_file=None):
    """Save a line graph with symmetric ±1 SD error bars; return its path.

    The title, subtitle, x-axis label and y-axis label are placeholders. Each
    series' stdev column supplies a separate error magnitude for every point.
    Defaults to a PNG beside the input CSV; PNG, PDF and SVG outputs are supported.
    """
    contentions, data = read_results(csv_file)
    output = Path(output_file) if output_file else Path(csv_file).with_suffix(".png")
    if not output.suffix:
        output = output.with_suffix(".png")
    if output.resolve() == Path(csv_file).resolve():
        raise ValueError("Output path must differ from the input CSV")
    try:
        import matplotlib
        matplotlib.use("Agg")  # Supports Linux SSH sessions without a display.
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise RuntimeError("Install matplotlib: python3 -m pip install matplotlib") from error

    fig, ax = plt.subplots(figsize=(10, 6))
    try:
        fig.suptitle("ADD LABEL HERE", fontsize=16)
        ax.set_title("ADD LABEL HERE", fontsize=11, pad=12)
        ax.set_xlabel("ADD LABEL HERE", fontsize=12)
        ax.set_ylabel("ADD LABEL HERE", fontsize=12)
        colors = ("#0072B2", "#D55E00", "#009E73", "#CC79A7")
        markers = ("o", "s", "^", "D")
        for name, color, marker in zip(SERIES, colors, markers):
            means, deviations = data[name]
            ax.errorbar(
                contentions, means, yerr=deviations, label=name,
                color=color, marker=marker, linestyle="-", linewidth=1.8,
                markersize=5, capsize=4, elinewidth=1.2,
            )
        ax.set_xticks([index / 10 for index in range(10)])
        ax.set_xticklabels([f"{index / 10:.1f}" for index in range(10)])
        ax.set_axisbelow(True)
        ax.grid(True, alpha=0.25)
        ax.legend()
        fig.tight_layout(rect=(0, 0, 1, 0.95))
        output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output, dpi=300, bbox_inches="tight")
    finally:
        plt.close(fig)
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv_file", type=Path, help="CSV with contention, four means and four stdev columns")
    parser.add_argument("--output", "-o", type=Path, help="image path; default: input CSV name with .png extension")
    args = parser.parse_args(argv)
    try:
        output = generate_graph(args.csv_file, args.output)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"Error: {error}\n")
    print(f"Saved graph: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
